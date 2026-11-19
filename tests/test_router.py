"""Router mode: rules, transforms, fan-out, buffers, and retries."""

from __future__ import annotations

import asyncio
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web

from lupaxa.switchboard.config import TransformSpec, load_config
from lupaxa.switchboard.exceptions import ConfigError
from lupaxa.switchboard.gateway import Gateway
from lupaxa.switchboard.transform import Document, run_pipeline


def test_template_does_not_execute_code() -> None:
    step = TransformSpec(type="template", template="{{level}} {{level.__class__}}")
    document = Document(data=b"{}", fields={"level": "high"})
    result = run_pipeline(document, [step], on_error="fail")
    assert result is not None
    assert result.data == b"high {{level.__class__}}"


def test_a_routing_cycle_is_rejected(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: one
    mode: router
    input:
      protocol: http
      host: 127.0.0.1
      port: 18080
      path: /a
    outputs:
      - name: next
        protocol: http
        url: http://127.0.0.1:18081/b
  - name: two
    mode: router
    input:
      protocol: http
      host: 127.0.0.1
      port: 18081
      path: /b
    outputs:
      - name: back
        protocol: http
        url: http://127.0.0.1:18080/a
"""
    with pytest.raises(ConfigError, match="routing cycle"):
        load_config(_write(tmp_path, text), user_path=None)


def test_a_complex_pattern_is_rejected(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: hook
    mode: router
    input:
      protocol: http
      port: 8080
      path: /events
    transforms:
      - type: regex
        pattern: "(.+)+"
    outputs:
      - name: audit
        protocol: http
        url: http://127.0.0.1:9/audit
"""
    with pytest.raises(ConfigError, match="pattern"):
        load_config(_write(tmp_path, text), user_path=None)


async def test_rules_fan_out_and_keep_unmapped_fields(tmp_path: Path) -> None:
    seen: dict[str, list[bytes]] = {"urgent": [], "audit": []}

    async def urgent(request: web.Request) -> web.Response:
        seen["urgent"].append(await request.read())
        return web.Response(status=204)

    async def audit(request: web.Request) -> web.Response:
        seen["audit"].append(await request.read())
        return web.Response(status=500)

    runner, port = await _app({"/urgent": urgent, "/audit": audit})
    text = f"""
version: 1
bridges:
  - name: hook
    mode: router
    http:
      response: forwarded
      response_status: 202
    delivery:
      success: best_effort
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    transforms:
      - type: json-parse
      - type: field-map
        mappings:
          priority: level
    rules:
      - when:
          field: level
          equals: high
        send_to: [urgent, audit]
    on_unmatched: drop
    outputs:
      - name: urgent
        protocol: http
        url: http://127.0.0.1:{port}/urgent
      - name: audit
        protocol: http
        url: http://127.0.0.1:{port}/audit
"""
    gateway = await _start(tmp_path, text)
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"
    try:
        response = await session.post(url, data=b'{"priority":"high","extra":1}')
        assert response.status == 202
        assert await response.read() == b""
        low = await session.post(url, data=b'{"priority":"low"}')
        assert low.status == 204
        assert gateway.runtime("hook").unmatched == 1
    finally:
        await session.close()
        await gateway.stop()
        await runner.cleanup()
    assert seen["urgent"]
    import json

    assert json.loads(seen["urgent"][0]) == {"level": "high", "extra": 1}
    assert seen["audit"]
    assert seen["urgent"][0] == seen["audit"][0]


async def test_stop_on_match_skips_later_rules(tmp_path: Path) -> None:
    seen: list[str] = []

    async def make(name: str):
        async def handler(request: web.Request) -> web.Response:
            seen.append(name)
            await request.read()
            return web.Response(status=204)

        return handler

    runner, port = await _app({"/urgent": await make("urgent"), "/audit": await make("audit")})
    text = f"""
version: 1
bridges:
  - name: hook
    mode: router
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    transforms:
      - type: json-parse
    stop_on_match: true
    rules:
      - when:
          field: level
          exists: true
        send_to: [urgent]
      - when:
          field: level
          equals: high
        send_to: [audit]
    outputs:
      - name: urgent
        protocol: http
        url: http://127.0.0.1:{port}/urgent
      - name: audit
        protocol: http
        url: http://127.0.0.1:{port}/audit
"""
    gateway = await _start(tmp_path, text)
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"
    try:
        response = await session.post(url, data=b'{"level":"high"}')
        assert response.status == 202
    finally:
        await session.close()
        await gateway.stop()
        await runner.cleanup()
    assert seen == ["urgent"]


async def test_buffer_reject_does_not_deliver(tmp_path: Path) -> None:
    called = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal called
        called += 1
        await request.read()
        return web.Response(status=204)

    runner, port = await _app({"/audit": handler})
    text = f"""
version: 1
bridges:
  - name: hook
    mode: router
    delivery:
      buffer:
        max_bytes: 4
        overflow: reject
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    outputs:
      - name: audit
        protocol: http
        url: http://127.0.0.1:{port}/audit
"""
    gateway = await _start(tmp_path, text)
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"
    try:
        response = await session.post(url, data=b"hello")
        assert response.status == 503
    finally:
        await session.close()
        await gateway.stop()
        await runner.cleanup()
    assert called == 0
    assert gateway.runtime("hook").dropped >= 1


async def test_hop_limit_drops_the_message(tmp_path: Path) -> None:
    called = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal called
        called += 1
        await request.read()
        return web.Response(status=204)

    runner, port = await _app({"/audit": handler})
    text = f"""
version: 1
bridges:
  - name: hook
    mode: router
    max_hops: 2
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    outputs:
      - name: audit
        protocol: http
        url: http://127.0.0.1:{port}/audit
"""
    gateway = await _start(tmp_path, text)
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"
    try:
        response = await session.post(url, data=b"ping", headers={"X-Switchboard-Hops": "2"})
        assert response.status == 508
    finally:
        await session.close()
        await gateway.stop()
        await runner.cleanup()
    assert called == 0


async def test_connect_failure_is_retried_without_a_second_delivery(tmp_path: Path) -> None:
    hits: list[bytes] = []

    async def handler(request: web.Request) -> web.Response:
        hits.append(await request.read())
        return web.Response(status=204)

    text = """
version: 1
bridges:
  - name: hook
    mode: router
    http:
      response: forwarded
    delivery:
      retry:
        max_attempts: 6
        initial_delay_ms: 40
        max_delay_ms: 40
        backoff: fixed
        replay: false
    input:
      protocol: http
      host: 127.0.0.1
      port: 0
      path: /events
    outputs:
      - name: audit
        protocol: http
        url: http://127.0.0.1:9/audit
"""
    path = _write(tmp_path, text)
    # The port is reserved after the gateway starts so the first attempts fail.
    gateway = Gateway(load_config(path, user_path=None))
    # Rewrite is easier with a known free port before start.
    await gateway.stop()
    port = _free_port()
    path.write_text(text.replace("127.0.0.1:9", f"127.0.0.1:{port}"))
    gateway = Gateway(load_config(path, user_path=None))
    await gateway.start()
    session = aiohttp.ClientSession()
    url = f"http://127.0.0.1:{gateway.runtime('hook').input_port}/events"

    async def post() -> int:
        response = await session.post(url, data=b"once")
        code = response.status
        await response.read()
        return code

    try:
        pending = asyncio.create_task(post())
        await asyncio.sleep(0.12)
        runner, _bound = await _app({"/audit": handler}, port=port)
        assert await asyncio.wait_for(pending, 2) == 202
    finally:
        await session.close()
        await gateway.stop()
        await runner.cleanup()
    assert hits == [b"once"]


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "switchboard.yml"
    path.write_text(text)
    return path


async def _start(tmp_path: Path, text: str) -> Gateway:
    gateway = Gateway(load_config(_write(tmp_path, text), user_path=None))
    await gateway.start()
    return gateway


async def _app(routes: dict, port: int = 0) -> tuple[web.AppRunner, int]:
    app = web.Application()
    for path, handler in routes.items():
        app.router.add_route("*", path, handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port, reuse_address=False)
    await site.start()
    names = runner.addresses
    if not names:
        raise RuntimeError("listener did not bind")
    return runner, int(names[0][1])


def _free_port() -> int:
    import socket

    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    number = int(held.getsockname()[1])
    held.close()
    return number
