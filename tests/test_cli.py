"""Command-line behaviour for the TCP/UDP gateway."""

from __future__ import annotations

import json
from pathlib import Path

from lupaxa.switchboard.cli import main
from lupaxa.switchboard.version import __version__


def test_version_matches_the_package(capsys) -> None:
    assert main(["--version"]) == 0
    assert capsys.readouterr().out.strip() == __version__


def test_unknown_flag_is_usage(capsys) -> None:
    assert main(["--not-a-real-flag"]) == 2
    assert capsys.readouterr().err


def test_quiet_and_verbose_together_are_rejected(capsys) -> None:
    assert main(["--quiet", "--verbose", "version"]) == 2
    assert "cannot be used together" in capsys.readouterr().err


def test_validate_accepts_a_tcp_bridge(tmp_path: Path, capsys) -> None:
    path = _config(tmp_path)
    assert main(["config", "validate", str(path)]) == 0
    assert "valid" in capsys.readouterr().out


def test_serve_rejects_a_router_without_outputs(tmp_path: Path, capsys) -> None:
    path = _config(tmp_path)
    path.write_text(path.read_text().replace("mode: gateway", "mode: router"))
    assert main(["serve", "--config", str(path)]) == 2
    assert "outputs" in capsys.readouterr().err


def test_validate_rejects_a_missing_file(tmp_path: Path, capsys) -> None:
    missing = tmp_path / "missing.yml"
    assert main(["config", "validate", str(missing)]) == 2
    assert "not found" in capsys.readouterr().err


def test_bridge_list_json_describes_the_file(tmp_path: Path, capsys) -> None:
    path = _config(tmp_path)
    assert main(["--config", str(path), "bridge", "list", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source"] == "configuration"
    assert payload["bridges"][0]["name"] == "line"
    assert payload["bridges"][0]["input"].startswith("tcp://")


def test_status_does_not_claim_a_live_process(tmp_path: Path, capsys) -> None:
    path = _config(tmp_path)
    assert main(["--config", str(path), "status", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["source"] == "configuration"
    assert "pid" not in payload


def _config(tmp_path: Path) -> Path:
    path = tmp_path / "switchboard.yml"
    path.write_text(
        """
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
"""
    )
    return path
