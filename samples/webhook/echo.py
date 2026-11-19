"""Print one TCP line for the webhook sample."""

from __future__ import annotations

import socket

import os

HOST = "127.0.0.1"
PORT = int(os.environ.get("SWITCHBOARD_ECHO_PORT", "19011"))


def main() -> None:
    server = socket.create_server((HOST, PORT))
    print("listening", flush=True)
    try:
        while True:
            connection, _address = server.accept()
            with connection:
                payload = _read_line(connection)
                if payload:
                    print(payload.decode(), end="", flush=True)
                    connection.sendall(payload)
    finally:
        server.close()


def _read_line(connection: socket.socket) -> bytes:
    chunks = bytearray()
    while b"\n" not in chunks:
        piece = connection.recv(1024)
        if not piece:
            break
        chunks.extend(piece)
    return bytes(chunks)


if __name__ == "__main__":
    main()
