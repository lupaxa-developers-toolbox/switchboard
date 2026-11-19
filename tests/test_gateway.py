"""TCP and UDP forwarding for one-to-one bridges."""

from __future__ import annotations

import asyncio
import logging
import socket

import pytest

from lupaxa.switchboard.config import load_config
from lupaxa.switchboard.exceptions import SwitchboardError
from lupaxa.switchboard.gateway import Gateway

SECRET = b"SECRET-PAYLOAD-VALUE"


async def test_direction_pipelines_keep_newline_framing(tmp_path) -> None:
    server, upstream = await _tcp_server(_echo)
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
    forward:
      transforms:
        - type: prefix
          value: "F:"
    reverse:
      transforms:
        - type: prefix
          value: "R:"
"""
    gateway = Gateway(load_config(_write(tmp_path, text), user_path=None))
    await gateway.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    writer.write(b"hello\n")
    await writer.drain()
    assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"R:F:hello\n"
    writer.close()
    await _stop(gateway, server, writer)


async def test_trace_preview_redacts_a_configured_field(tmp_path) -> None:
    server, upstream = await _tcp_server(_echo)
    text = f"""
version: 1
diagnostics:
  trace_payloads: true
  trace_bytes: 64
  secret_fields: [password]
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
"""
    gateway = Gateway(load_config(_write(tmp_path, text), user_path=None))
    await gateway.start()
    port = gateway.runtime("line").input_port
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b'{"password":"SECRET"}\n')
    await writer.drain()
    assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b'{"password":"SECRET"}\n'
    recorded = str(list(gateway._events))
    writer.close()
    await _stop(gateway, server, writer)
    assert "SECRET" not in recorded
    assert "534543524554" not in recorded


async def test_tcp_newline_round_trip_survives_a_split_read(tmp_path) -> None:
    server, upstream = await _tcp_server(_echo)
    gateway = await _gateway(tmp_path, "tcp", upstream)
    port = gateway.runtime("line").input_port
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"hel")
    await writer.drain()
    await asyncio.sleep(0.05)
    writer.write(b"lo\n")
    await writer.drain()
    assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"hello\n"
    writer.close()
    await _stop(gateway, server, writer)


async def test_two_tcp_clients_do_not_share_an_upstream(tmp_path) -> None:
    server, upstream = await _tcp_server(_echo)
    gateway = await _gateway(tmp_path, "tcp", upstream)

    async def once(payload: bytes) -> bytes:
        port = gateway.runtime("line").input_port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(payload)
        await writer.drain()
        got = await asyncio.wait_for(reader.readuntil(b"\n"), 2)
        writer.close()
        await writer.wait_closed()
        return got

    first, second = await asyncio.gather(once(b"alpha\n"), once(b"beta\n"))
    assert {first, second} == {b"alpha\n", b"beta\n"}
    await _stop(gateway, server)


async def test_udp_datagrams_keep_their_boundaries(tmp_path) -> None:
    echo, upstream = await _udp_echo()
    gateway = await _gateway(tmp_path, "udp", upstream, framing=False)
    received: asyncio.Queue[bytes] = asyncio.Queue()

    class _Client(asyncio.DatagramProtocol):
        def datagram_received(self, data: bytes, addr: tuple[str | int, ...] | None) -> None:
            received.put_nowait(data)

    _transport, _protocol = await asyncio.get_running_loop().create_datagram_endpoint(
        _Client,
        local_addr=("127.0.0.1", 0),
    )
    for payload in (b"one", b"two"):
        _transport.sendto(payload, ("127.0.0.1", gateway.runtime("line").input_port))
    found = {
        await asyncio.wait_for(received.get(), 2),
        await asyncio.wait_for(received.get(), 2),
    }
    assert found == {b"one", b"two"}
    _transport.close()
    echo.close()
    await gateway.stop()


async def test_tcp_frames_become_udp_datagrams(tmp_path) -> None:
    echo, upstream = await _udp_echo()
    gateway = await _gateway(tmp_path, "udp", upstream, input_protocol="tcp")
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    writer.write(b"ping\n")
    await writer.drain()
    assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"ping\n"
    writer.close()
    echo.close()
    await _stop(gateway, None, writer)


async def test_one_bind_failure_does_not_stop_the_other_bridge(tmp_path) -> None:
    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    held.listen(1)
    blocked = held.getsockname()[1]
    text = _document(protocol="tcp", output=9, port=blocked, name="blocked")
    text += _bridge_only(protocol="tcp", output=9, port=0, name="open")
    gateway = Gateway(load_config(_write(tmp_path, text), user_path=None))
    await gateway.start()
    assert gateway.runtime("blocked").listening is False
    assert gateway.runtime("open").listening is True
    assert gateway.runtime("open").input_port != 0
    await gateway.stop()
    held.close()


async def test_no_listening_bridge_is_an_error(tmp_path) -> None:
    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    held.listen(1)
    text = _document(protocol="tcp", output=9, port=held.getsockname()[1])
    gateway = Gateway(load_config(_write(tmp_path, text), user_path=None))
    with pytest.raises(SwitchboardError, match="No bridges started"):
        await gateway.start()
    await gateway.stop()
    held.close()


async def test_connection_limit_refuses_the_next_client(tmp_path) -> None:
    server, upstream = await _tcp_server(_hold)
    gateway = await _gateway(tmp_path, "tcp", upstream, extra="      max_connections: 1\n")
    port = gateway.runtime("line").input_port
    first_reader, first_writer = await asyncio.open_connection("127.0.0.1", port)
    await _until(lambda: gateway.runtime("line").active == 1)
    _second_reader, second_writer = await asyncio.open_connection("127.0.0.1", port)
    assert await asyncio.wait_for(_second_reader.read(8), 1) == b""
    assert gateway.runtime("line").refused >= 1
    first_writer.close()
    second_writer.close()
    await _stop(gateway, server, first_writer, second_writer)
    first_reader.feed_eof()


async def test_stop_releases_the_listener(tmp_path) -> None:
    gateway = await _gateway(tmp_path, "tcp", 9)
    port = gateway.runtime("line").input_port
    await gateway.stop()
    sock = socket.socket()
    sock.bind(("127.0.0.1", port))
    sock.close()


async def test_idle_timeout_closes_a_quiet_connection(tmp_path) -> None:
    server, upstream = await _tcp_server(_hold)
    gateway = await _gateway(
        tmp_path,
        "tcp",
        upstream,
        extra="      idle_timeout_seconds: 0.2\n",
    )
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    assert await asyncio.wait_for(reader.read(8), 1) == b""
    writer.close()
    await _stop(gateway, server, writer)


async def test_frame_timeout_closes_a_partial_line(tmp_path) -> None:
    server, upstream = await _tcp_server(_echo)
    gateway = await _gateway(
        tmp_path,
        "tcp",
        upstream,
        extra="      idle_timeout_seconds: 5\n      frame_timeout_seconds: 0.2\n",
    )
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    writer.write(b"partial")
    await writer.drain()
    assert await asyncio.wait_for(reader.read(8), 1) == b""
    await _until(lambda: gateway.runtime("line").failures >= 1)
    writer.close()
    await _stop(gateway, server, writer)


async def test_half_close_lets_the_upstream_reply(tmp_path) -> None:
    server, upstream = await _tcp_server(_reply_after_eof)
    gateway = await _gateway(tmp_path, "tcp", upstream, disconnect="half_close")
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    writer.write(b"ping\n")
    await writer.drain()
    writer.write_eof()
    assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"ok\n"
    writer.close()
    await _stop(gateway, server, writer)


async def test_logs_do_not_contain_the_payload(tmp_path, caplog: pytest.LogCaptureFixture) -> None:
    server, upstream = await _tcp_server(_echo)
    text = _document(protocol="tcp", output=upstream, max_frame=8)
    gateway = Gateway(load_config(_write(tmp_path, text), user_path=None))
    caplog.set_level(logging.DEBUG, logger="lupaxa.switchboard")
    await gateway.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    writer.write(SECRET + b"\n")
    await writer.drain()
    assert await asyncio.wait_for(reader.read(8), 1) == b""
    assert SECRET.decode() not in caplog.text
    writer.close()
    await _stop(gateway, server, writer)


async def _echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    finally:
        writer.close()


async def _hold(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        await reader.read()
    finally:
        writer.close()


async def _reply_after_eof(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    while True:
        data = await reader.read(1024)
        if not data:
            break
    writer.write(b"ok\n")
    await writer.drain()
    writer.close()


async def _tcp_server(handler) -> tuple[asyncio.Server, int]:
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    bound = server.sockets
    if not bound:
        raise RuntimeError("listener did not bind")
    return server, int(bound[0].getsockname()[1])


async def _udp_echo() -> tuple[asyncio.DatagramTransport, int]:
    class _Echo(asyncio.DatagramProtocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def datagram_received(self, data: bytes, addr: tuple[str | int, ...] | None) -> None:
            if isinstance(self.transport, asyncio.DatagramTransport) and addr is not None:
                self.transport.sendto(data, addr)

    transport, _protocol = await asyncio.get_running_loop().create_datagram_endpoint(
        _Echo,
        local_addr=("127.0.0.1", 0),
    )
    if not isinstance(transport, asyncio.DatagramTransport):
        raise RuntimeError("expected a datagram transport")
    return transport, int(transport.get_extra_info("sockname")[1])


async def _gateway(
    tmp_path,
    protocol: str,
    output: int,
    *,
    framing: bool = True,
    input_protocol: str | None = None,
    extra: str = "",
    disconnect: str = "close_both",
) -> Gateway:
    text = _document(
        protocol=protocol,
        output=output,
        framing=framing,
        input_protocol=input_protocol,
        extra=extra,
        disconnect=disconnect,
    )
    gateway = Gateway(load_config(_write(tmp_path, text), user_path=None))
    await gateway.start()
    return gateway


def _document(
    *,
    protocol: str,
    output: int,
    port: int = 0,
    name: str = "line",
    framing: bool = True,
    input_protocol: str | None = None,
    extra: str = "",
    disconnect: str = "close_both",
    max_frame: int = 65536,
) -> str:
    return "version: 1\nbridges:\n" + _bridge_only(
        protocol=protocol,
        output=output,
        port=port,
        name=name,
        framing=framing,
        input_protocol=input_protocol,
        extra=extra,
        disconnect=disconnect,
        max_frame=max_frame,
    )


def _bridge_only(
    *,
    protocol: str,
    output: int,
    port: int,
    name: str,
    framing: bool = True,
    input_protocol: str | None = None,
    extra: str = "",
    disconnect: str = "close_both",
    max_frame: int = 65536,
) -> str:
    incoming = input_protocol or protocol
    frame = f"        type: newline\n        max_frame_bytes: {max_frame}\n" if framing else ""
    frame_block = f"      framing:\n{frame}" if framing else ""
    limits = f"    limits:\n{extra}" if extra else ""
    return f"""  - name: {name}
    mode: gateway
    disconnect:
      on_peer_close: {disconnect}
{limits}    input:
      protocol: {incoming}
      host: 127.0.0.1
      port: {port}
{frame_block}    output:
      protocol: {protocol}
      host: 127.0.0.1
      port: {output}
{frame_block}"""


def _write(tmp_path, text: str):
    path = tmp_path / "switchboard.yml"
    path.write_text(text)
    return path


async def _until(predicate) -> None:
    for _ in range(50):
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition was not met")


async def _stop(
    gateway: Gateway,
    server: asyncio.Server | None = None,
    *writers: asyncio.StreamWriter,
) -> None:
    for writer in writers:
        writer.close()
    await gateway.stop()
    if server is not None:
        server.close()
        await server.wait_closed()
