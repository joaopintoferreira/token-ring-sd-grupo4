#!/usr/bin/env python3
"""
Servidor do recurso compartilhado + monitor do anel + interface web.

Papéis deste processo:
  1. Servidor do recurso compartilhado (cliente-servidor): guarda um contador que os nós
     leem e escrevem dentro da seção crítica (ENTER / READ / WRITE / EXIT). Ele também
     detecta violações da exclusão mútua (dois nós dentro da seção crítica ao mesmo tempo).
  2. Monitor do anel: acompanha onde está o token, descobre nós que caíram (PING) e,
     se o token se perder, gera um novo com uma época maior (os tokens antigos são descartados).
  3. Interface web (http://localhost:8000): mostra o anel, o token, os contadores e o log, e
     permite pedir a seção crítica em um nó, falhar/recuperar nós e ajustar os tempos.
  4. Supervisor (opcional): inicia os N processos dos nós como subprocessos (--spawn).

Uso rápido:   python server.py --nodes 5
"""
import argparse
import json
import os
import socketserver
import subprocess
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from protocol import call, parse_addr, send

HERE = os.path.dirname(os.path.abspath(__file__))


class Monitor:
    def __init__(self, args):
        self.lock = threading.RLock()
        self.n = args.nodes
        self.args = args
        if args.node_addrs:
            self.addrs = [parse_addr(a) for a in args.node_addrs.split(",")]
            self.n = len(self.addrs)
        else:
            self.addrs = [("127.0.0.1", args.node_port + i) for i in range(self.n)]
        self.spawn = not args.no_spawn
        self.procs = {}
        self.cfg = {"cs_time": args.cs_time, "pass_delay": args.pass_delay}
        self.auto_regen = True
        self.token_timeout_override = args.token_timeout

        # recurso compartilhado
        self.counter = 0
        self.entries = 0
        self.violations = 0
        self.in_cs = set()

        # estado do anel
        self.nodes = {i: {"alive": False, "state": "idle", "pending": 0, "entries": 0}
                      for i in range(self.n)}
        self.epoch = 0
        self.token_started = False
        self.token_holder = None
        self.token_to = None
        self.last_token_time = time.time()
        self.regenerations = 0
        self.token_passes = 0
        self.log_lines = deque(maxlen=300)
        self.seq = 0

    # -------------------------------------------------------------- utilidades
    def log(self, text, node=None, kind="info"):
        with self.lock:
            self.seq += 1
            self.log_lines.append({"n": self.seq, "t": time.strftime("%H:%M:%S"),
                                   "node": node, "msg": text, "kind": kind})

    def ring_string(self):
        return ",".join(f"{h}:{p}" for h, p in self.addrs)

    def token_timeout(self):
        if self.token_timeout_override:
            return self.token_timeout_override
        alive = max(1, sum(1 for v in self.nodes.values() if v["alive"]))
        cycle = alive * (self.cfg["pass_delay"] + self.cfg["cs_time"] + 0.2)
        return max(6.0, 2.5 * cycle + 2.0)

    # -------------------------------------------------------------- mensagens dos nós (TCP)
    def on_message(self, msg):
        kind = msg.get("type")
        node = msg.get("node")
        with self.lock:
            if kind == "HELLO":
                self.nodes[node]["alive"] = True
                self.log(f"nó {node} entrou no anel", node, "ok")
            elif kind == "LOG":
                self.log(msg["msg"], node)
            elif kind == "STATE":
                self.nodes[node]["state"] = msg["state"]
                self.nodes[node]["pending"] = msg.get("pending", 0)
            elif kind == "TOKEN_AT":
                self.token_holder = node
                self.token_to = None
                self.token_passes += 1
                self.last_token_time = time.time()
                self.epoch = max(self.epoch, msg.get("epoch", 0))
                return {"ok": True, "cfg": self.cfg}
            elif kind == "TOKEN_SENT":
                self.token_to = msg.get("to")
                self.last_token_time = time.time()
            elif kind == "TOKEN_DROPPED":
                self.log(f"token da época {msg.get('epoch')} descartado no nó {node} (época antiga)", node, "warn")
            elif kind == "ENTER":
                if self.in_cs:
                    self.violations += 1
                    self.log(f"VIOLAÇÃO: nó {node} entrou enquanto {sorted(self.in_cs)} já estava na seção crítica",
                             node, "err")
                self.in_cs.add(node)
                self.entries += 1
                self.nodes[node]["entries"] += 1
                self.log(f"nó {node} ENTROU na seção crítica", node, "cs")
                return {"ok": True, "cfg": self.cfg}
            elif kind == "READ":
                return {"ok": True, "value": self.counter}
            elif kind == "WRITE":
                self.counter = msg["value"]
                return {"ok": True}
            elif kind == "EXIT":
                self.in_cs.discard(node)
                self.log(f"nó {node} SAIU da seção crítica (contador = {self.counter})", node, "cs")
                return {"ok": True}
        return None

    # -------------------------------------------------------------- ações da interface
    def request_cs(self, node):
        if not self.nodes.get(node, {}).get("alive"):
            return {"ok": False, "error": "nó fora do anel"}
        ok = send(self.addrs[node], {"type": "REQUEST"})
        if ok:
            self.log(f"usuário pediu a seção crítica no nó {node}", node, "req")
        return {"ok": ok}

    def spawn_node(self, i):
        cmd = [sys.executable, os.path.join(HERE, "node.py"), "--id", str(i),
               "--ring", self.ring_string(), "--server", f"127.0.0.1:{self.args.port}"]
        self.procs[i] = subprocess.Popen(cmd, stdout=subprocess.DEVNULL if self.args.quiet else None)

    def fail_node(self, i):
        p = self.procs.get(i)
        if not p or p.poll() is not None:
            return {"ok": False, "error": "nó não é gerenciado por este servidor ou já está parado"}
        p.terminate()
        p.wait(timeout=3)
        self.log(f"nó {i} FALHOU (processo encerrado)", i, "err")
        return {"ok": True}

    def recover_node(self, i):
        p = self.procs.get(i)
        if p and p.poll() is None:
            return {"ok": False, "error": "nó já está em execução"}
        self.spawn_node(i)
        self.log(f"nó {i} reiniciado", i, "ok")
        return {"ok": True}

    def regenerate(self, reason="pedido do usuário", initial=False):
        with self.lock:
            alive = [i for i, v in self.nodes.items() if v["alive"]]
            if not alive:
                return {"ok": False, "error": "nenhum nó vivo"}
            self.epoch += 1
            if not initial:
                self.regenerations += 1
            target = alive[0]
            self.last_token_time = time.time()
            self.token_holder, self.token_to = None, None
            what = "token inicial gerado" if initial else "novo token gerado"
            self.log(f"{what} (época {self.epoch}) no nó {target}: {reason}", target, "ok" if initial else "warn")
            epoch = self.epoch
        send(self.addrs[target], {"type": "GENERATE", "epoch": epoch})
        return {"ok": True, "epoch": epoch}

    def reset_counters(self):
        with self.lock:
            self.counter = self.entries = self.violations = 0
            for v in self.nodes.values():
                v["entries"] = 0
            self.log("contadores zerados", None, "info")

    # -------------------------------------------------------------- thread do monitor
    def monitor_loop(self):
        while True:
            time.sleep(0.4)
            # 1) quem está vivo?
            for i, addr in enumerate(self.addrs):
                alive = call(addr, {"type": "PING"}, timeout=0.5) is not None
                with self.lock:
                    v = self.nodes[i]
                    if v["alive"] and not alive:
                        self.log(f"nó {i} não responde: removido do anel", i, "err")
                        v["state"], v["pending"] = "falho", 0
                        self.in_cs.discard(i)
                    if not v["alive"] and alive:
                        v["state"] = "idle"
                    v["alive"] = alive
            with self.lock:
                alive_n = sum(1 for v in self.nodes.values() if v["alive"])
                # 2) primeiro token: quando todos os nós esperados estiverem no ar
                if not self.token_started and alive_n == self.n:
                    self.token_started = True
                    start = True
                else:
                    start = False
                # 3) token perdido?
                lost = (self.token_started and alive_n > 0 and self.auto_regen
                        and time.time() - self.last_token_time > self.token_timeout())
            if start:
                self.regenerate("início do anel", initial=True)
            elif lost:
                self.regenerate("token perdido (timeout)")

    # -------------------------------------------------------------- estado para a interface
    def snapshot(self):
        with self.lock:
            return {
                "n": self.n, "nodes": self.nodes, "epoch": self.epoch,
                "token_holder": self.token_holder, "token_to": self.token_to,
                "token_age": round(time.time() - self.last_token_time, 1),
                "token_timeout": round(self.token_timeout(), 1),
                "counter": self.counter, "entries": self.entries, "violations": self.violations,
                "in_cs": sorted(self.in_cs), "regenerations": self.regenerations,
                "token_passes": self.token_passes, "token_started": self.token_started, "cfg": self.cfg, "auto_regen": self.auto_regen,
                "can_spawn": self.spawn, "log": list(self.log_lines)[-120:],
            }

    def stop(self):
        for p in self.procs.values():
            if p.poll() is None:
                p.terminate()


# ------------------------------------------------------------------ TCP (nós -> servidor)
class TcpHandler(socketserver.StreamRequestHandler):
    def handle(self):
        line = self.rfile.readline()
        if not line:
            return
        try:
            msg = json.loads(line)
        except ValueError:
            return
        reply = self.server.monitor.on_message(msg)
        if reply is not None:
            self.wfile.write((json.dumps(reply) + "\n").encode())


class TcpServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


# ------------------------------------------------------------------ HTTP (interface)
class HttpHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, code, body, ctype="application/json"):
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        m = self.server.monitor
        if self.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "ui", "index.html"), "rb") as f:
                self._send(200, f.read(), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            self._send(200, m.snapshot())
        else:
            self._send(404, {"error": "não encontrado"})

    def do_POST(self):
        m = self.server.monitor
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            body = {}
        p = self.path
        if p == "/api/request":
            res = m.request_cs(int(body["node"]))
        elif p == "/api/fail":
            res = m.fail_node(int(body["node"]))
        elif p == "/api/recover":
            res = m.recover_node(int(body["node"]))
        elif p == "/api/regenerate":
            res = m.regenerate("pedido do usuário")
        elif p == "/api/reset":
            m.reset_counters()
            res = {"ok": True}
        elif p == "/api/config":
            with m.lock:
                for k in ("cs_time", "pass_delay"):
                    if k in body:
                        m.cfg[k] = max(0.0, min(5.0, float(body[k])))
                if "auto_regen" in body:
                    m.auto_regen = bool(body["auto_regen"])
            res = {"ok": True}
        else:
            return self._send(404, {"error": "não encontrado"})
        self._send(200, res)


def main():
    ap = argparse.ArgumentParser(description="Servidor do recurso + monitor + interface web")
    ap.add_argument("--nodes", type=int, default=5, help="número de nós do anel (padrão 5)")
    ap.add_argument("--port", type=int, default=5000, help="porta TCP do servidor do recurso")
    ap.add_argument("--http-port", type=int, default=8000, help="porta da interface web")
    ap.add_argument("--node-port", type=int, default=6000, help="porta do nó 0; o nó i usa node-port+i")
    ap.add_argument("--node-addrs", help="host:porta dos nós (para rodar em várias máquinas)")
    ap.add_argument("--no-spawn", action="store_true",
                    help="não iniciar os nós automaticamente (inicie node.py à mão)")
    ap.add_argument("--cs-time", type=float, default=1.0, help="tempo usando o recurso (s)")
    ap.add_argument("--pass-delay", type=float, default=0.8, help="pausa antes de repassar o token (s)")
    ap.add_argument("--token-timeout", type=float, default=0, help="força o tempo (s) para considerar o token perdido")
    ap.add_argument("--quiet", action="store_true", help="esconde a saída dos nós")
    args = ap.parse_args()

    mon = Monitor(args)
    tcp = TcpServer(("0.0.0.0", args.port), TcpHandler)
    tcp.monitor = mon
    threading.Thread(target=tcp.serve_forever, daemon=True).start()
    http = ThreadingHTTPServer(("0.0.0.0", args.http_port), HttpHandler)
    http.monitor = mon
    threading.Thread(target=http.serve_forever, daemon=True).start()
    print(f"Servidor do recurso em TCP :{args.port} | interface em http://localhost:{args.http_port}", flush=True)

    if mon.spawn:
        for i in range(mon.n):
            mon.spawn_node(i)
    else:
        print("Aguardando os nós (inicie com node.py)...", flush=True)
    threading.Thread(target=mon.monitor_loop, daemon=True).start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        mon.stop()


if __name__ == "__main__":
    main()
