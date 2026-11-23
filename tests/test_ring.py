#!/usr/bin/env python3
"""
Testes automáticos do anel (usa só a biblioteca padrão).

  1. Exclusão mútua: pedidos concorrentes em todos os nós -> contador == entradas e 0 violações.
  2. Falha de nó: um nó cai; os outros continuam usando o recurso e o token pula o nó caído.
  3. Token perdido: o nó que tem o token cai; o monitor gera um novo token (época maior).
  4. Recuperação: o nó volta ao anel e volta a receber o token.

Execução (a partir da pasta do projeto):   python tests/test_ring.py
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HTTP, TCP, NODE0 = 8100, 5100, 6100
N = 4


def api(path, body=None):
    url = f"http://127.0.0.1:{HTTP}{path}"
    if body is None:
        return json.load(urllib.request.urlopen(url, timeout=3))
    req = urllib.request.Request(url, json.dumps(body).encode(), {"Content-Type": "application/json"})
    return json.load(urllib.request.urlopen(req, timeout=3))


def wait_for(cond, timeout, what):
    t0 = time.time()
    while time.time() - t0 < timeout:
        s = api("/api/state")
        if cond(s):
            return s
        time.sleep(0.2)
    raise AssertionError(f"tempo esgotado esperando: {what}\nestado: {json.dumps(api('/api/state'))[:600]}")


def check(ok, msg):
    print(("  OK  " if ok else " FALHOU ") + msg)
    if not ok:
        raise AssertionError(msg)


def main():
    srv = subprocess.Popen([sys.executable, os.path.join(ROOT, "server.py"), "--nodes", str(N),
                            "--port", str(TCP), "--http-port", str(HTTP), "--node-port", str(NODE0),
                            "--cs-time", "0.15", "--pass-delay", "0.1", "--token-timeout", "3", "--quiet"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                api("/api/state"); break
            except Exception:
                time.sleep(0.2)
        wait_for(lambda s: all(v["alive"] for v in s["nodes"].values()) and s["token_holder"] is not None,
                 15, "todos os nós vivos e token em circulação")

        print("Teste 1: exclusão mútua com pedidos concorrentes")
        for i in range(N):
            for _ in range(5):
                api("/api/request", {"node": i})
        s = wait_for(lambda s: s["entries"] >= 5 * N and not s["in_cs"], 40, "20 entradas")
        check(s["violations"] == 0, f"nenhuma violação (violações = {s['violations']})")
        check(s["counter"] == s["entries"] == 5 * N, f"contador ({s['counter']}) == entradas ({s['entries']}) == 20")
        check(all(v["entries"] == 5 for v in s["nodes"].values()), "cada nó entrou exatamente 5 vezes (justiça)")

        print("Teste 2: falha de um nó (o token pula o nó caído)")
        api("/api/fail", {"node": 2})
        wait_for(lambda s: not s["nodes"]["2"]["alive"], 5, "nó 2 marcado como caído")
        base = api("/api/state")["entries"]
        for i in (0, 1, 3):
            for _ in range(3):
                api("/api/request", {"node": i})
        s = wait_for(lambda s: s["entries"] >= base + 9 and not s["in_cs"], 40, "9 novas entradas sem o nó 2")
        check(s["violations"] == 0 and s["counter"] == s["entries"], "exclusão mútua mantida sem o nó 2")

        print("Teste 3: token perdido (cai o nó que está com o token)")
        epoch0, regen0 = s["epoch"], s["regenerations"]
        for _ in range(100):
            holder = api("/api/state")["token_holder"]
            if holder is not None and holder != 2:
                break
            time.sleep(0.05)
        api("/api/fail", {"node": holder})
        s = wait_for(lambda s: s["regenerations"] > regen0, 20, "monitor regenerar o token")
        check(s["epoch"] > epoch0, f"época do token aumentou ({epoch0} -> {s['epoch']})")
        base = s["entries"]
        alive = [int(i) for i, v in s["nodes"].items() if v["alive"]]
        for i in alive:
            api("/api/request", {"node": i})
        s = wait_for(lambda s: s["entries"] >= base + len(alive) and not s["in_cs"], 30, "uso após regenerar")
        check(s["violations"] == 0 and s["counter"] == s["entries"], "exclusão mútua mantida após regenerar o token")

        print("Teste 4: recuperação dos nós")
        for i in (2, holder):
            api("/api/recover", {"node": i})
        wait_for(lambda s: all(v["alive"] for v in s["nodes"].values()), 10, "todos os nós vivos de novo")
        base = api("/api/state")["entries"]
        for i in range(N):
            api("/api/request", {"node": i})
        s = wait_for(lambda s: s["entries"] >= base + N and not s["in_cs"], 30, "todos usando de novo")
        check(s["violations"] == 0 and s["counter"] == s["entries"], "exclusão mútua mantida com o anel completo")
        print("\nTodos os testes passaram.")
    finally:
        srv.terminate()
        try:
            srv.wait(timeout=5)
        except Exception:
            srv.kill()


if __name__ == "__main__":
    main()
