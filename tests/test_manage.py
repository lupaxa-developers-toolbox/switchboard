"""Management socket, reload, traces, and the optional text interface."""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

import pytest

from lupaxa.switchboard.cli import main
from lupaxa.switchboard.config import load_config
from lupaxa.switchboard.exceptions import SwitchboardError
from lupaxa.switchboard.gateway import Gateway
from lupaxa.switchboard.manage import Endpoint, request


def test_tcp_management_requires_the_token(tmp_path: Path) -> None:
    text = _pair(tmp_path, "tcp")
    asyncio.run(_token_round_trip(tmp_path, text))


def test_trace_hides_the_payload_and_reload_keeps_the_other_bridge(tmp_path: Path) -> None:
    asyncio.run(_reload_and_trace(tmp_path))


def test_commands_do_not_treat_configuration_as_delivery(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "switchboard.yml", _pair(tmp_path, "local"))
    assert main(["--config", str(path), "test", "keep", "--json"]) in {0, 1}
    checks = json.loads(capsys.readouterr().out)["checks"]
    assert checks[-1]["name"] == "end_to_end"
    assert checks[-1]["status"] == "not_run"
    assert main(["--config", str(path), "doctor"]) == 0
    assert "configuration" in capsys.readouterr().out


async def test_the_text_interface_drops_rows_when_management_closes() -> None:
    pytest.importorskip("textual")
    from textual.widgets import DataTable

    from lupaxa.switchboard.tui import Dashboard

    class _Client:
        def __init__(self) -> None:
            self.down = False

        async def call(self, method: str, params: dict[str, object] | None = None) -> object:
            if self.down:
                raise SwitchboardError("not connected")
            if method == "status":
                row = {"name": "line", "input": "tcp://127.0.0.1:9", "listening": True}
                return {"bridges": [row]}
            return {method: []}

    client = _Client()
    app = Dashboard(client)
    async with app.run_test() as pilot:
        await pilot.pause()
        table = app.query_one(DataTable)
        assert table.row_count == 1
        client.down = True
        await app._refresh()
        assert app.connected is False
        assert table.row_count == 0


async def _token_round_trip(tmp_path: Path, text: str) -> None:
    path = _write(tmp_path / "switchboard.yml", text)
    gateway = Gateway(load_config(path, user_path=None), source_path=path)
    await gateway.start()
    assert gateway.management is not None
    try:
        bad = Endpoint(
            transport="tcp",
            host="127.0.0.1",
            port=gateway.management.port,
            token="not-the-real-token",
        )
        with pytest.raises(SwitchboardError):
            await request(bad, "health")
        health = await request(gateway.management, "health")
        assert isinstance(health, dict)
        assert health["status"] == "ok"
    finally:
        await gateway.stop()


async def _reload_and_trace(tmp_path: Path) -> None:
    directory = Path(tempfile.mkdtemp(prefix="sb-", dir="/tmp"))
    socket_path = directory / "m.sock"
    path = _write(tmp_path / "switchboard.yml", _pair(tmp_path, "local", socket_path))
    gateway = Gateway(load_config(path, user_path=None), source_path=path)
    await gateway.start()
    kept = gateway.bridges["keep"].server
    assert gateway.management is not None
    try:
        port = gateway.bridges["keep"].input_port
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"SECRET\n")
        await writer.drain()
        await asyncio.sleep(0.05)
        events = await request(gateway.management, "events", {"bridge": "keep"})
        assert "SECRET" not in json.dumps(events)
        path.write_text(_pair(tmp_path, "local", socket_path, drop=False))
        report = await gateway.reload()
        assert gateway.bridges["keep"].server is kept
        assert "drop" not in gateway.bridges
        assert isinstance(report, dict)
        changes = report["changes"]
        kept_change = isinstance(changes, list) and any(
            isinstance(item, dict) and item.get("action") == "keep" for item in changes
        )
        assert kept_change
        writer.close()
        await writer.wait_closed()
    finally:
        await gateway.stop()


def _pair(
    tmp_path: Path,
    transport: str,
    socket_path: Path | None = None,
    *,
    drop: bool = True,
) -> str:
    token = "0123456789abcdef"
    if transport == "tcp":
        management = f"  enabled: true\n  transport: tcp\n  token: {token}\n"
    else:
        management = f"  enabled: true\n  path: {socket_path}\n"
    bridges = _bridge("keep") + (_bridge("drop") if drop else "")
    return f"version: 1\nmanagement:\n{management}bridges:\n{bridges}"


def _bridge(name: str) -> str:
    return f"""  - name: {name}
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
      port: 9
      framing:
        type: newline
"""


def _write(path: Path, text: str) -> Path:
    path.write_text(text)
    return path
