#!/usr/bin/env python3
"""
Processo (nó) do anel lógico: exclusão mútua distribuída com Token Ring.

Cada nó tem um número (--id, de 0 a N-1) e conhece o endereço de todos os nós
do anel (--ring). O sucessor do nó i é o nó (i+1) mod N. Só existe um token:
quem o possui pode entrar na seção crítica (usar o recurso compartilhado, que
fica no servidor); quem não precisa dele repassa ao sucessor.

Uso:
    python node.py --id 0 --ring 127.0.0.1:6000,127.0.0.1:6001,127.0.0.1:6002 \
                   --server 127.0.0.1:5000
"""
import argparse
import socketserver
import threading
import time

from protocol import call, parse_addr, send

DEFAULT_CFG = {"cs_time": 1.0, "pass_delay": 0.8}


class Node:
    def __init__(self, node_id, ring, server_addr):
        self.id = node_id
        self.ring = ring                    # lista de (host, porta), índice = número do processo
        self.n = len(ring)
        self.server = server_addr           # servidor do recurso compartilhado / monitor
        self.lock = threading.Lock()
        self.pending = 0                    # pedidos de seção crítica ainda não atendidos
        self.epoch = 0                      # maior época de token já vista (descarta tokens antigos)
        self.cfg = dict(DEFAULT_CFG)
        self.state = "idle"                 # idle | waiting | cs (mostrado na interface)

    # ------------------------------------------------------------ comunicação com o servidor
    def report(self, msg, want_reply=False):
        msg = dict(msg, node=self.id)
        if want_reply:
            return call(self.server, msg)
        send(self.server, msg)
        return None

    def log(self, text):
        self.report({"type": "LOG", "msg": text})

    def set_state(self, state):
        self.state = state
        self.report({"type": "STATE", "state": state, "pending": self.pending})

    # ------------------------------------------------------------ recepção de mensagens
    def handle(self, msg):
        """Despacha uma mensagem recebida na porta deste nó."""
        kind = msg.get("type")
        if kind == "TOKEN":
            self.on_token(msg)
            return None
        if kind == "REQUEST":               # o usuário (interface) quer usar o recurso
            with self.lock:
                self.pending += 1
            if self.state == "idle":        # se está na seção crítica, o estado mostrado continua "cs"
                self.set_state("waiting")
            else:
                self.report({"type": "STATE", "state": self.state, "pending": self.pending})
            return {"ok": True, "pending": self.pending}
        if kind == "GENERATE":              # o monitor pede um novo token (token perdido)
            threading.Thread(target=self.on_token,
                             args=({"type": "TOKEN", "epoch": msg["epoch"], "from": self.id},),
                             daemon=True).start()
            return {"ok": True}
        if kind == "PING":
            return {"type": "PONG", "node": self.id}
        return None

    def on_token(self, msg):
        """Recebeu o token: usa a seção crítica se precisar e repassa ao sucessor."""
        with self.lock:
            if msg.get("epoch", 0) < self.epoch:
                stale = True
            else:
                stale = False
                self.epoch = msg.get("epoch", 0)
        if stale:                           # token de uma época antiga (já foi regenerado): descarta
            self.report({"type": "TOKEN_DROPPED", "epoch": msg.get("epoch", 0)})
            return

        reply = self.report({"type": "TOKEN_AT", "epoch": self.epoch,
                             "from": msg.get("from")}, want_reply=True)
        if reply and "cfg" in reply:
            self.cfg = reply["cfg"]

        with self.lock:
            wants = self.pending > 0
            if wants:
                self.pending -= 1
        if wants:
            self.critical_section()

        time.sleep(self.cfg["pass_delay"])  # só para a animação da interface ficar legível
        self.forward_token()

    # ------------------------------------------------------------ seção crítica
    def critical_section(self):
        """Usa o recurso compartilhado (contador no servidor) com exclusão mútua."""
        self.set_state("cs")
        reply = self.report({"type": "ENTER"}, want_reply=True)
        if reply and "cfg" in reply:
            self.cfg = reply["cfg"]
        value = (self.report({"type": "READ"}, want_reply=True) or {}).get("value", 0)
        time.sleep(self.cfg["cs_time"])     # "trabalho" feito com o recurso (janela para condição de corrida)
        self.report({"type": "WRITE", "value": value + 1}, want_reply=True)
        self.report({"type": "EXIT"}, want_reply=True)
        self.set_state("waiting" if self.pending > 0 else "idle")

    # ------------------------------------------------------------ envio do token
    def forward_token(self):
        """Envia o token ao sucessor; se ele estiver fora do ar, tenta o seguinte do anel."""
        msg = {"type": "TOKEN", "epoch": self.epoch, "from": self.id}
        for k in range(1, self.n):
            target = (self.id + k) % self.n
            if send(self.ring[target], msg):
                if k > 1:
                    self.log(f"sucessor inalcançável; token enviado ao nó {target}")
                self.report({"type": "TOKEN_SENT", "to": target})
                return
        # nenhum outro nó está vivo: o token volta para mim mesmo
        self.log("sou o único nó vivo; mantendo o token")
        threading.Thread(target=self.on_token, args=(msg,), daemon=True).start()


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        import json
        line = self.rfile.readline()
        if not line:
            return
        try:
            msg = json.loads(line)
        except ValueError:
            return
        reply = self.server.node.handle(msg)
        if reply is not None:
            self.wfile.write((json.dumps(reply) + "\n").encode())


class NodeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    ap = argparse.ArgumentParser(description="Nó do anel Token Ring")
    ap.add_argument("--id", type=int, required=True, help="número do processo (0..N-1)")
    ap.add_argument("--ring", required=True, help="lista host:porta de todos os nós, em ordem")
    ap.add_argument("--server", default="127.0.0.1:5000", help="servidor do recurso (host:porta)")
    args = ap.parse_args()

    ring = [parse_addr(a) for a in args.ring.split(",")]
    node = Node(args.id, ring, parse_addr(args.server))
    host, port = ring[args.id]
    srv = NodeServer(("0.0.0.0", port), Handler)
    srv.node = node
    print(f"[nó {node.id}] escutando em {host}:{port}; sucessor = nó {(node.id + 1) % node.n}", flush=True)
    node.report({"type": "HELLO"})
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
