"""Version shape only. Never a hard-coded release number."""

from __future__ import annotations

import re

import pytest

from lupaxa.switchboard import __version__, get_version
from lupaxa.switchboard.cli import main


def test_version_is_dotted_semver() -> None:
    assert get_version() == __version__
    assert re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.]+)?", __version__)


def test_cli_prints_the_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--version"]) == 0
    assert capsys.readouterr().out.strip() == __version__
