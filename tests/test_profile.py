"""Protocol profiles: validation, binary round-trips, and gateway use."""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import pytest

from lupaxa.switchboard.cli import main
from lupaxa.switchboard.config import load_config
from lupaxa.switchboard.exceptions import ConfigError, ProfileError
from lupaxa.switchboard.gateway import Gateway
from lupaxa.switchboard.profile import decode_profile, encode_profile


def test_a_structure_round_trips(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    message = {"kind": 1, "note": "ping", "rest": base64.b64encode(b"hi").decode()}
    raw = encode_profile(profile, json.dumps(message))
    assert json.loads(decode_profile(profile, raw)) == message
    assert encode_profile(profile, decode_profile(profile, raw)) == raw


def test_a_message_type_name_encodes_as_its_code(tmp_path: Path) -> None:
    profile = _profile(tmp_path, message_type=True)
    raw = encode_profile(profile, json.dumps({"kind": "status", "note": "ping", "rest": ""}))
    assert json.loads(decode_profile(profile, raw))["kind"] == 1


def test_unknown_fields_and_boundaries_are_rejected(tmp_path: Path) -> None:
    unknown = _document("        - name: extra\n          type: struct\n")
    with pytest.raises(ConfigError, match="unknown field type"):
        load_config(_write(tmp_path / "unknown.yml", unknown), user_path=None)
    trailing = _document(
        "        - name: rest\n          type: remaining_bytes\n"
        "        - name: kind\n          type: uint8\n"
    )
    with pytest.raises(ConfigError, match="ambiguous"):
        load_config(_write(tmp_path / "order.yml", trailing), user_path=None)
    conflict = _document("", framing="type: fixed_length\n      size: 3\n")
    with pytest.raises(ConfigError, match="conflicting"):
        load_config(_write(tmp_path / "fixed.yml", conflict), user_path=None)
    nested = _document("        - name: extra\n          type: device\n")
    with pytest.raises(ConfigError, match="unknown field type"):
        load_config(_write(tmp_path / "nested.yml", nested), user_path=None)


def test_missing_and_duplicate_profiles_are_rejected(tmp_path: Path) -> None:
    missing = """
version: 1
profiles: {}
bridges:
  - name: line
    mode: gateway
    input:
      profile: device
      port: 9000
    output:
      protocol: tcp
      port: 9001
      framing:
        type: newline
"""
    with pytest.raises(ConfigError, match="missing profile"):
        load_config(_write(tmp_path / "missing.yml", missing), user_path=None)
    duplicate = """
version: 1
profiles:
  device: {transport: tcp, framing: {type: newline}}
  device: {transport: tcp, framing: {type: newline}}
"""
    with pytest.raises(ConfigError, match="duplicate"):
        load_config(_write(tmp_path / "duplicate.yml", duplicate), user_path=None)


def test_profile_commands_list_and_validate(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "switchboard.yml", _document(""))
    assert main(["--config", str(path), "profile", "list"]) == 0
    assert "device" in capsys.readouterr().out
    bare = tmp_path / "profiles.yml"
    bare.write_text("device:\n  transport: tcp\n  framing:\n    type: newline\n")
    assert main(["profile", "validate", str(bare)]) == 0
    assert "valid" in capsys.readouterr().out
    sample = tmp_path / "sample.bin"
    message = tmp_path / "message.json"
    message.write_text('{"kind": 1, "note": "ping", "rest": ""}')
    assert main(["--config", str(path), "profile", "encode", "device", str(message)]) == 0
    sample.write_bytes(capsys.readouterr().out.encode("latin-1"))
    assert main(["--config", str(path), "profile", "decode", "device", str(sample)]) == 0
    assert json.loads(capsys.readouterr().out)["note"] == "ping"


def test_invalid_profile_json_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ProfileError):
        encode_profile(_profile(tmp_path), "{")


async def test_a_profile_decodes_once_on_the_way_through(tmp_path: Path) -> None:
    received: list[bytes] = []

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        line = await reader.readuntil(b"\n")
        received.append(line)
        writer.write(line)
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    names = server.sockets
    if not names:
        raise RuntimeError("listener did not bind")
    upstream = int(names[0].getsockname()[1])
    text = _document("", port=0).replace("port: 9001", f"port: {upstream}")
    gateway = Gateway(load_config(_write(tmp_path / "live.yml", text), user_path=None))
    await gateway.start()
    port = gateway.runtime("line").input_port
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    payload = encode_profile(
        _profile(tmp_path),
        json.dumps({"kind": 1, "note": "ping", "rest": ""}),
    )
    writer.write(len(payload).to_bytes(2, "big") + payload)
    await writer.drain()
    try:
        frame = await asyncio.wait_for(reader.readexactly(2 + len(payload)), 2)
    finally:
        writer.close()
        await gateway.stop()
        server.close()
        await server.wait_closed()
    assert json.loads(received[0]) == {"kind": 1, "note": "ping", "rest": ""}
    assert frame[2:] == payload


async def test_an_unstructured_profile_does_not_transcode(tmp_path: Path) -> None:
    received: list[bytes] = []

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        line = await reader.readuntil(b"\n")
        received.append(line)
        writer.write(line)
        await writer.drain()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    names = server.sockets
    if not names:
        raise RuntimeError("listener did not bind")
    upstream = int(names[0].getsockname()[1])
    text = f"""
version: 1
profiles:
  legacy-chat:
    transport: tcp
    encoding: latin-1
    framing:
      type: newline
bridges:
  - name: line
    mode: gateway
    input:
      profile: legacy-chat
      host: 127.0.0.1
      port: 0
    output:
      protocol: tcp
      host: 127.0.0.1
      port: {upstream}
      framing:
        type: newline
"""
    gateway = Gateway(load_config(_write(tmp_path / "legacy.yml", text), user_path=None))
    await gateway.start()
    reader, writer = await asyncio.open_connection("127.0.0.1", gateway.runtime("line").input_port)
    writer.write(b"caf\xe9\n")
    await writer.drain()
    try:
        assert await asyncio.wait_for(reader.readuntil(b"\n"), 2) == b"caf\xe9\n"
    finally:
        writer.close()
        await gateway.stop()
        server.close()
        await server.wait_closed()
    assert received == [b"caf\xe9\n"]


def _profile(tmp_path: Path, message_type: bool = False):
    extra = ""
    if message_type:
        extra = "      message_type:\n        field: kind\n        values:\n          1: status\n"
    text = _document("", extra=extra)
    return load_config(_write(tmp_path / "profile.yml", text), user_path=None).profiles["device"]


def _document(fields: str, *, framing: str = "", port: int = 9000, extra: str = "") -> str:
    frame = framing or "type: length_prefix\n      prefix_bytes: 2\n      endian: big\n"
    body = fields or (
        "        - name: kind\n          type: uint8\n"
        "        - name: note\n          type: string\n          length: 4\n"
        "        - name: rest\n          type: remaining_bytes\n"
    )
    return f"""
version: 1
profiles:
  device:
    transport: tcp
    framing:
      {frame}
    structure:
      endian: big
      fields:
{body}
{extra}bridges:
  - name: line
    mode: gateway
    input:
      profile: device
      host: 127.0.0.1
      port: {port}
    output:
      protocol: tcp
      host: 127.0.0.1
      port: 9001
      framing:
        type: newline
"""


def _write(path: Path, text: str) -> Path:
    path.write_text(text)
    return path
