"""Normalised messages. The envelope is never written to the wire."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass(frozen=True)
class Source:
    """The peer that produced a payload."""

    protocol: str
    address: str
    port: int


@dataclass(frozen=True)
class Message:
    """One framed payload plus internal bookkeeping."""

    id: str
    bridge: str
    timestamp: str
    source: Source
    content_type: str
    data: bytes
    metadata: dict[str, str] = field(default_factory=dict)
    trace_id: str | None = None

    @classmethod
    def create(
        cls,
        bridge: str,
        source: Source,
        data: bytes,
        *,
        session_id: str,
        trace_id: str | None = None,
    ) -> Message:
        """Build a message. Metadata stays off the wire."""
        return cls(
            id=str(uuid.uuid4()),
            bridge=bridge,
            timestamp=datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            source=source,
            content_type="application/octet-stream",
            data=data,
            metadata={"session_id": session_id},
            trace_id=trace_id,
        )

    def wire_payload(self) -> bytes:
        """Return the bytes an adapter may send. Metadata is not included."""
        return self.data
