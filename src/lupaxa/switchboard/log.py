"""Text and JSON logs. Payloads are not written here."""

from __future__ import annotations

import json
import logging


def configure_logging(level: str, log_format: str) -> None:
    """Attach one handler to the Switchboard logger."""
    logger = logging.getLogger("lupaxa.switchboard")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(level.upper())
    handler = logging.StreamHandler()
    if log_format == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(handler)


class _JsonFormatter(logging.Formatter):
    """One JSON object per record, without message payloads."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))
