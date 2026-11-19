"""Configuration parsing and validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from lupaxa.switchboard.config import load_config
from lupaxa.switchboard.exceptions import ConfigError


def test_defaults_fill_a_tcp_bridge(tmp_path: Path) -> None:
    path = _write(tmp_path, _bridge())
    config = load_config(path, user_path=None)
    bridge = config.bridges[0]
    assert config.version == 1
    assert bridge.input.host == "127.0.0.1"
    assert bridge.input.framing is not None
    assert bridge.input.framing.type == "newline"
    assert bridge.limits.idle_timeout_seconds == 300


def test_explicit_file_overrides_the_user_file(tmp_path: Path) -> None:
    user = _write(tmp_path / "user.yml", "version: 1\nserver:\n  shutdown_timeout_seconds: 9\n")
    explicit = _write(tmp_path / "explicit.yml", _bridge())
    config = load_config(explicit, user_path=user)
    assert config.server.shutdown_timeout_seconds == 9
    assert config.bridges[0].name == "line"


def test_log_level_override_replaces_the_file(tmp_path: Path) -> None:
    path = _write(tmp_path, _bridge() + "logging:\n  level: error\n")
    config = load_config(path, user_path=None, log_level="debug")
    assert config.logging.level == "debug"


def test_unknown_version_is_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "version: 2\n")
    with pytest.raises(ConfigError):
        load_config(path, user_path=None)


def test_duplicate_bridge_names_are_rejected(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: line
    mode: gateway
    input:
      protocol: tcp
      port: 9000
      framing:
        type: newline
    output:
      protocol: tcp
      port: 9001
      framing:
        type: newline
  - name: line
    mode: gateway
    input:
      protocol: tcp
      port: 9002
      framing:
        type: newline
    output:
      protocol: tcp
      port: 9003
      framing:
        type: newline
"""
    path = _write(tmp_path, text)
    with pytest.raises(ConfigError, match="duplicate bridge"):
        load_config(path, user_path=None)


def test_listener_conflicts_are_rejected(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: one
    mode: gateway
    input:
      protocol: tcp
      host: 127.0.0.1
      port: 9000
      framing:
        type: newline
    output:
      protocol: tcp
      host: 127.0.0.1
      port: 9001
      framing:
        type: newline
  - name: two
    mode: gateway
    input:
      protocol: tcp
      host: 127.0.0.1
      port: 9000
      framing:
        type: newline
    output:
      protocol: tcp
      host: 127.0.0.1
      port: 9002
      framing:
        type: newline
"""
    path = _write(tmp_path, text)
    with pytest.raises(ConfigError, match="already used"):
        load_config(path, user_path=None)


def test_tcp_without_framing_is_rejected(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: bare
    mode: gateway
    input:
      protocol: tcp
      port: 9000
    output:
      protocol: tcp
      port: 9001
      framing:
        type: newline
"""
    path = _write(tmp_path, text)
    with pytest.raises(ConfigError, match="framing"):
        load_config(path, user_path=None)


def test_router_mode_and_other_protocols_are_rejected(tmp_path: Path) -> None:
    router = _bridge().replace("mode: gateway", "mode: router")
    with pytest.raises(ConfigError, match="outputs"):
        load_config(_write(tmp_path / "router.yml", router), user_path=None)
    foreign = _bridge().replace("protocol: tcp", "protocol: websocket", 1)
    with pytest.raises(ConfigError, match="already have boundaries"):
        load_config(_write(tmp_path / "ws.yml", foreign), user_path=None)


def test_raw_stream_must_be_paired(tmp_path: Path) -> None:
    text = _bridge().replace("type: newline", "type: raw_stream", 1)
    with pytest.raises(ConfigError, match="raw_stream"):
        load_config(_write(tmp_path, text), user_path=None)


def test_invalid_framing_parameters_are_rejected(tmp_path: Path) -> None:
    text = _bridge().replace(
        "type: newline",
        "type: fixed_length\n        size: 128\n        max_frame_bytes: 16",
    )
    with pytest.raises(ConfigError, match="max_frame_bytes"):
        load_config(_write(tmp_path, text), user_path=None)


def test_timeouts_must_be_positive(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: line
    mode: gateway
    limits:
      idle_timeout_seconds: 0
    input:
      protocol: tcp
      port: 9000
      framing:
        type: newline
    output:
      protocol: tcp
      port: 9001
      framing:
        type: newline
"""
    with pytest.raises(ConfigError):
        load_config(_write(tmp_path, text), user_path=None)


def test_websocket_url_and_http_policy_are_validated(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: web
    mode: gateway
    input:
      protocol: websocket
      host: 127.0.0.1
      port: 0
      path: /session
    output:
      protocol: websocket
      url: ws://127.0.0.1:8080/session
"""
    config = load_config(_write(tmp_path / "ws.yml", text), user_path=None)
    output = config.bridges[0].output
    assert output is not None
    assert output.port == 8080
    assert output.path == "/session"
    upstream = """
version: 1
bridges:
  - name: hook
    mode: gateway
    http:
      response: upstream
    input:
      protocol: http
      port: 8080
      path: /events
    output:
      protocol: tcp
      port: 9000
      framing:
        type: newline
"""
    with pytest.raises(ConfigError, match="upstream responses"):
        load_config(_write(tmp_path / "up.yml", upstream), user_path=None)


def test_tls_listener_requires_a_certificate(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: secure
    mode: gateway
    input:
      protocol: tcp
      port: 9443
      framing:
        type: newline
      tls:
        enabled: true
    output:
      protocol: tcp
      port: 9000
      framing:
        type: newline
"""
    with pytest.raises(ConfigError, match="certfile"):
        load_config(_write(tmp_path, text), user_path=None)


def test_gateway_directions_load_and_field_steps_need_json(tmp_path: Path) -> None:
    text = """
version: 1
bridges:
  - name: line
    mode: gateway
    input:
      protocol: tcp
      port: 9000
      framing:
        type: newline
    output:
      protocol: websocket
      url: ws://127.0.0.1:8080/session
    forward:
      transforms:
        - type: decode
          encoding: utf-8
    reverse:
      transforms:
        - type: encode
          encoding: utf-8
"""
    loaded = load_config(_write(tmp_path / "ok.yml", text), user_path=None)
    assert loaded.bridges[0].forward is not None
    field_map = "type: field-map\n          mappings:\n            user: name"
    mapped = text.replace("type: decode", field_map)
    with pytest.raises(ConfigError, match="incompatible transformation"):
        load_config(_write(tmp_path / "fields.yml", mapped), user_path=None)
    router = """
version: 1
bridges:
  - name: hook
    mode: router
    input:
      protocol: http
      port: 8080
      path: /events
    forward:
      transforms:
        - type: decode
    outputs:
      - name: audit
        protocol: http
        url: http://127.0.0.1:9/audit
"""
    with pytest.raises(ConfigError, match="forward and reverse"):
        load_config(_write(tmp_path / "router.yml", router), user_path=None)


def test_management_tcp_requires_a_token(tmp_path: Path) -> None:
    text = "version: 1\nmanagement:\n  enabled: true\n  transport: tcp\n"
    with pytest.raises(ConfigError, match="token"):
        load_config(_write(tmp_path, text), user_path=None)


def _bridge(name: str = "line", port: int = 9000) -> str:
    other = port + 1
    return f"""
version: 1
bridges:
  - name: {name}
    mode: gateway
    input:
      protocol: tcp
      host: 127.0.0.1
      port: {port}
      framing:
        type: newline
    output:
      protocol: tcp
      host: 127.0.0.1
      port: {other}
      framing:
        type: newline
"""


def _write(path: Path, text: str) -> Path:
    target = path if path.suffix == ".yml" else path / "switchboard.yml"
    target.write_text(text)
    return target
