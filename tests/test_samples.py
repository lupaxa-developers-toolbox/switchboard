"""The documented samples load, and each script does what the guide says."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

from lupaxa.switchboard.config import load_config
from lupaxa.switchboard.gateway import Gateway

_ROOT = Path(__file__).resolve().parents[1]
_SAMPLES = _ROOT / "samples"


def test_sample_configs_load() -> None:
    line = load_config(_SAMPLES / "line" / "switchboard.yml", user_path=None)
    webhook = load_config(_SAMPLES / "webhook" / "switchboard.yml", user_path=None)
    profile = load_config(_SAMPLES / "profile" / "switchboard.yml", user_path=None)
    assert line.bridges[0].name == "line"
    assert webhook.bridges[0].http.response == "forwarded"
    assert "device" in profile.profiles


async def test_line_sample_echoes_one_line() -> None:
    heard = await _run_gateway("line", "send.py", "echo.py")
    assert heard["client"] == "hello"
    assert heard["echo"] == "hello\n"


async def test_webhook_sample_writes_the_post_body() -> None:
    heard = await _run_gateway("webhook", "post.py", "echo.py")
    assert heard["client"] == "202"
    assert heard["echo"] == '{"event":"ping"}\n'


async def test_profile_sample_round_trips() -> None:
    payload = json.loads(await _python("profile", "roundtrip.py"))
    assert payload["decoded"] == {"kind": 1, "note": "ping", "rest": ""}
    assert payload["bytes"] > 0


async def _run_gateway(name: str, script: str, helper: str) -> dict[str, str]:
    root = _SAMPLES / name
    config = load_config(root / "switchboard.yml", user_path=None)
    bridge = config.bridges[0]
    echo_port = _free_port()
    listen_port = 0 if bridge.input.protocol == "http" else _free_port()
    updated = config.model_copy(
        update={
            "bridges": [
                bridge.model_copy(
                    update={
                        "input": bridge.input.model_copy(update={"port": listen_port}),
                        "output": bridge.output.model_copy(update={"port": echo_port}),
                    }
                )
            ]
        }
    )
    echo_env = os.environ.copy()
    echo_env["SWITCHBOARD_ECHO_PORT"] = str(echo_port)
    echo = subprocess.Popen(
        [sys.executable, str(root / helper)],
        stdout=subprocess.PIPE,
        text=True,
        env=echo_env,
    )
    assert echo.stdout is not None
    try:
        assert echo.stdout.readline().strip() == "listening"
        gateway = Gateway(updated)
        await gateway.start()
        runtime = gateway.bridges[bridge.name]
        client_env = _client_env(name, runtime)
        try:
            client = await _python(name, script, client_env)
        finally:
            await gateway.stop()
        echoed = echo.stdout.readline()
    finally:
        echo.terminate()
        echo.wait(timeout=5)
        if echo.poll() is None:
            echo.kill()
            echo.wait(timeout=5)
    return {"client": client, "echo": echoed}


def _client_env(name: str, runtime: object) -> dict[str, str]:
    env: dict[str, str] = {}
    if name == "line":
        env["SWITCHBOARD_SEND_PORT"] = str(runtime.input_port)
    if name == "webhook":
        env["SWITCHBOARD_POST_URL"] = f"http://127.0.0.1:{runtime.input_port}/events"
    return env


def _free_port() -> int:
    import socket

    held = socket.socket()
    held.bind(("127.0.0.1", 0))
    number = int(held.getsockname()[1])
    held.close()
    return number


async def _python(name: str, script: str, extra_env: dict[str, str] | None = None) -> str:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(_ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    if extra_env:
        env.update(extra_env)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(_SAMPLES / name / script),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    output, error = await process.communicate()
    if process.returncode != 0:
        detail = error.decode() or output.decode()
        raise AssertionError(detail)
    return output.decode().strip()
