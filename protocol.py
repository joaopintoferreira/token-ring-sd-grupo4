"""
Protocolo de mensagens do trabalho prático (Token Ring, Grupo 4).

Todas as mensagens são objetos JSON, um por linha, enviados sobre TCP.
Cada mensagem tem o campo "type". Uma conexão TCP carrega uma mensagem e,
opcionalmente, uma resposta (também uma linha JSON).

Tipos de mensagem (resumo, detalhes no README):
  Entre nós do anel      : TOKEN
  Servidor -> nó         : REQUEST, GENERATE, PING
  Nó -> servidor         : HELLO, STATE, TOKEN_AT, TOKEN_SENT, TOKEN_DROPPED, LOG
  Nó -> recurso (servidor): ENTER, READ, WRITE, EXIT
"""
import json
import socket


def encode(msg: dict) -> bytes:
    """Serializa uma mensagem como uma linha JSON terminada em \\n."""
    return (json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8")


def send(addr, msg, timeout=1.5) -> bool:
    """Envia uma mensagem sem esperar resposta. Retorna False se não conseguiu entregar."""
    try:
        with socket.create_connection(addr, timeout=timeout) as s:
            s.sendall(encode(msg))
        return True
    except OSError:
        return False


def call(addr, msg, timeout=1.5):
    """Envia uma mensagem e espera uma resposta (uma linha JSON). Retorna None em caso de erro."""
    try:
        with socket.create_connection(addr, timeout=timeout) as s:
            s.sendall(encode(msg))
            f = s.makefile("rb")
            line = f.readline()
            return json.loads(line) if line else None
    except (OSError, ValueError):
        return None


def parse_addr(text: str):
    """'host:porta' -> (host, porta)."""
    host, port = text.rsplit(":", 1)
    return host, int(port)
