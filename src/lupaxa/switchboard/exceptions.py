"""Errors reported to the operator."""

from __future__ import annotations


class SwitchboardError(Exception):
    """A failure the command line can report without a traceback."""

    def __init__(self, message: str, *, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class ConfigError(SwitchboardError):
    """The configuration file is missing or invalid."""

    def __init__(self, message: str) -> None:
        super().__init__(message, exit_code=2)


class UsageError(SwitchboardError):
    """The command line was used incorrectly."""

    def __init__(self, message: str) -> None:
        super().__init__(message, exit_code=2)


class FrameError(SwitchboardError):
    """A frame could not be parsed or encoded safely."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message, exit_code=1)
        self.code = code


class ProfileError(FrameError):
    """A protocol profile could not encode or decode a message."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="PROFILE")
