"""WebSocket, HTTP, TLS, and outbound reconnect behaviour."""

from __future__ import annotations

import asyncio
import logging
import shutil
import ssl
import subprocess
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web

from lupaxa.switchboard.config import load_config
from lupaxa.switchboard.gateway import Gateway

SECRET = "SECRET-PAYLOAD-VALUE"


async def test_websocket_to_tcp_round_trip(tmp_path: Path) -> None:
    server, upstream = await _tcp_echo()
    gateway = await _start(tmp_path, _ws_tcp(upstream))
    session = aiohttp.ClientSession()
    try:
        socket = await session.ws_connect(_ws_url(gateway))
        await socket.send_bytes(b"ping")
        reply = await asyncio.wait_for(socket.receive(), 2)
        assert reply.data == b"ping"
        await socket.close()
        await _until(lambda: gateway.runtime("line").active == 0)
    finally:
        await session.close()
        await gateway.stop()
        server.close()
        await server.wait_closed()


async def test_two_websocket_clients_stay_isolated(tmp_path: Path) -> None:
    server, upstream = await _tcp_echo()
    gateway = await _start(tmp_path, _ws_tcp(upstream))
    session = aiohttp.ClientSession()

    async def once(payload: bytes) -> bytes:
        socket = await session.ws_connect(_ws_url(gateway))
        await socket.send_bytes(payload)
        reply = await asyncio.wait_for(socket.receive(), 2)
        await socket.close()
        data = reply.data
        assert isinstance(data, bytes)
        return data

    try:
        first, second = await asyncio.gather(once(b"alpha"), once(b"beta"))
    finally:
        await session.close()
        await gateway.stop()
        server.close()
        await server.wait_closed()
    assert {first, second} == {b"alpha", b"beta"}


async def test_tcp_to_websocket_echo(tmp_path: Path) -> None:
    site, port = await _ws_echo()
    text = f"""
version: 1
bridges:
  - name: line
    mode: gateway
    input:
      protocol: tcp
      host: 127.0.0.1
      port: 0
      framing:
        type: newline
    output:
      protocol: websocket
      url: ws://127.0.0.1:{port}/session
"""
    gateway = await _start(tmp_path, text)
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    writer.write(b"ping\n")
    await writer.drain()
    assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"ping\n"
    writer.close()
    await gateway.stop()
    await site.cleanup()


async def test_http_upstream_hides_sensitive_headers(tmp_path: Path) -> None:
    seen: dict[str, str] = {}

    async def handler(request: web.Request) -> web.Response:
        seen["authorization"] = request.headers.get("Authorization", "")
        seen["content-type"] = request.headers.get("Content-Type", "")
        return web.Response(status=201, text="created")

    site, port = await _http_site(handler)
    text = f"""
version: 1
bridges:
  - name: hook
    mode: gateway
    http:
      response: upstream
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    output:
      protocol: http
      url: http://127.0.0.1:{port}/events
"""
    gateway = await _start(tmp_path, text)
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"
    try:
        response = await session.post(
            url,
            data=b"hello",
            headers={"Authorization": "Bearer secret-token", "Content-Type": "text/plain"},
        )
        body = await response.read()
        assert response.status == 201
        assert body == b"created"
        assert seen["authorization"] == ""
        assert seen["content-type"] == "text/plain"
        assert "secret-token" not in repr(seen)
    finally:
        await session.close()
        await gateway.stop()
        await site.cleanup()


async def test_http_forwarded_to_tcp_answers_after_the_write(tmp_path: Path) -> None:
    received: asyncio.Queue[bytes] = asyncio.Queue()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.read(64)
        await received.put(data)
        writer.close()

    server, upstream = await _tcp_server(serve)
    text = f"""
version: 1
bridges:
  - name: hook
    mode: gateway
    http:
      response: forwarded
      response_status: 202
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    output:
      protocol: tcp
      host: 127.0.0.1
      port: {upstream}
      framing:
        type: newline
"""
    gateway = await _start(tmp_path, text)
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"
    try:
        response = await session.post(url, data=b"hello")
        assert response.status == 202
        assert await asyncio.wait_for(received.get(), 2) == b"hello\n"
    finally:
        await session.close()
        await gateway.stop()
        server.close()
        await server.wait_closed()


async def test_accepted_responds_before_the_output_finishes(tmp_path: Path) -> None:
    started = asyncio.Event()
    release = asyncio.Event()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        started.set()
        await release.wait()
        await reader.read(64)
        writer.close()

    server, upstream = await _tcp_server(serve)
    text = f"""
version: 1
bridges:
  - name: hook
    mode: gateway
    http:
      response: accepted
      response_status: 202
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    output:
      protocol: tcp
      host: 127.0.0.1
      port: {upstream}
      framing:
        type: newline
"""
    gateway = await _start(tmp_path, text)
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"
    try:
        response = await asyncio.wait_for(session.post(url, data=b"hello"), 1)
        assert response.status == 202
        await asyncio.wait_for(started.wait(), 1)
    finally:
        release.set()
        await session.close()
        await gateway.stop()
        server.close()
        await server.wait_closed()


async def test_http_limits_reject_the_method_and_a_large_body(tmp_path: Path) -> None:
    async def handler(_request: web.Request) -> web.Response:
        return web.Response(text="ok")

    site, port = await _http_site(handler)
    text = f"""
version: 1
bridges:
  - name: hook
    mode: gateway
    http:
      max_body_bytes: 8
      allowed_methods: [POST]
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    output:
      protocol: http
      url: http://127.0.0.1:{port}/events
"""
    gateway = await _start(tmp_path, text)
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"
    try:
        denied = await session.get(url)
        assert denied.status == 405
        oversized = await session.post(url, data=b"123456789")
        assert oversized.status == 413
    finally:
        await session.close()
        await gateway.stop()
        await site.cleanup()


async def test_reconnect_does_not_replay_a_delivered_line(tmp_path: Path) -> None:
    seen: list[bytes] = []

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        data = await reader.readuntil(b"\n")
        seen.append(data)
        writer.write(data)
        await writer.drain()
        writer.close()

    port = _free_port()
    text = f"""
version: 1
bridges:
  - name: line
    mode: gateway
    reconnect:
      enabled: true
      policy: fixed
      max_attempts: 8
      initial_delay_seconds: 0.05
      max_delay_seconds: 0.05
    input:
      protocol: tcp
      host: 127.0.0.1
      port: 0
      framing:
        type: newline
    output:
      protocol: tcp
      host: 127.0.0.1
      port: {port}
      framing:
        type: newline
"""
    gateway = await _start(tmp_path, text)
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    await asyncio.sleep(0.12)
    server = await asyncio.start_server(serve, "127.0.0.1", port)
    writer.write(b"once\n")
    await writer.drain()
    assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"once\n"
    assert seen == [b"once\n"]
    writer.close()
    await gateway.stop()
    server.close()
    await server.wait_closed()


async def test_tls_tcp_round_trip_and_unverified_warning(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("openssl is not available")
    cert, key = _certificate(tmp_path, openssl)
    server, upstream = await _tcp_echo(ssl_context=_server_ssl(cert, key))
    text = f"""
version: 1
bridges:
  - name: line
    mode: gateway
    input:
      protocol: tcp
      host: 127.0.0.1
      port: 0
      framing:
        type: newline
    output:
      protocol: tcp
      host: 127.0.0.1
      port: {upstream}
      framing:
        type: newline
      tls:
        enabled: true
        verify: false
"""
    caplog.set_level(logging.WARNING, logger="lupaxa.switchboard")
    gateway = await _start(tmp_path, text)
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    writer.write(SECRET.encode() + b"\n")
    await writer.drain()
    assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == SECRET.encode() + b"\n"
    assert "disables TLS certificate verification" in caplog.text
    assert "BEGIN PRIVATE KEY" not in caplog.text
    assert SECRET not in caplog.text
    writer.close()
    await gateway.stop()
    server.close()
    await server.wait_closed()


def _ws_tcp(upstream: int) -> str:
    return f"""
version: 1
bridges:
  - name: line
    mode: gateway
    input:
      protocol: websocket
      host: 127.0.0.1
      port: 0
      path: /session
    output:
      protocol: tcp
      host: 127.0.0.1
      port: {upstream}
      framing:
        type: newline
"""


def _ws_url(gateway: Gateway) -> str:
    runtime = gateway.runtime("line")
    return f"ws://127.0.0.1:{runtime.input_port}/session"


async def _start(tmp_path: Path, text: str) -> Gateway:
    path = tmp_path / "switchboard.yml"
    path.write_text(text)
    gateway = Gateway(load_config(path, user_path=None))
    await gateway.start()
    return gateway


async def _tcp_echo(ssl_context: ssl.SSLContext | None = None) -> tuple[asyncio.Server, int]:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await reader.read(1024)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        finally:
            writer.close()

    return await _tcp_server(echo, ssl_context=ssl_context)


async def _tcp_server(
    handler,
    ssl_context: ssl.SSLContext | None = None,
) -> tuple[asyncio.Server, int]:
    server = await asyncio.start_server(handler, "127.0.0.1", 0, ssl=ssl_context)
    bound = server.sockets
    if not bound:
        raise RuntimeError("listener did not bind")
    return server, int(bound[0].getsockname()[1])


async def _ws_echo() -> tuple[web.AppRunner, int]:
    async def handler(request: web.Request) -> web.WebSocketResponse:
        socket = web.WebSocketResponse()
        await socket.prepare(request)
        async for message in socket:
            if message.type == aiohttp.WSMsgType.BINARY and isinstance(message.data, bytes):
                await socket.send_bytes(message.data)
            elif message.type == aiohttp.WSMsgType.TEXT and isinstance(message.data, str):
                await socket.send_bytes(message.data.encode("utf-8"))
        return socket

    return await _site(handler, "/session")


async def _http_site(handler) -> tuple[web.AppRunner, int]:
    return await _site(handler, "/events")


async def _site(handler, path: str) -> tuple[web.AppRunner, int]:
    app = web.Application()
    app.router.add_route("*", path, handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, reuse_address=False)
    await site.start()
    names = runner.addresses
    if not names:
        raise RuntimeError("listener did not bind")
    return runner, int(names[0][1])


def _free_port() -> int:
    import socket

    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    port = int(held.getsockname()[1])
    held.close()
    return port


def _certificate(tmp_path: Path, openssl: str) -> tuple[Path, Path]:
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return cert, key


def _server_ssl(cert: Path, key: Path) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, key)
    return context


async def _until(predicate) -> None:
    for _ in range(50):
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition was not met")
