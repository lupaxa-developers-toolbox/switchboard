"""Send one line through the line sample gateway."""

from __future__ import annotations

import socket

import os

HOST = "127.0.0.1"
PORT = int(os.environ.get("SWITCHBOARD_SEND_PORT", "19000"))


def main() -> None:
    with socket.create_connection((HOST, PORT), timeout=5) as connection:
        connection.sendall(b"hello\n")
        payload = _read_line(connection)
    print(payload.decode().rstrip("\n"))


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
