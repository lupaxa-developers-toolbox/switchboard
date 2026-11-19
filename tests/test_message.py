"""The internal envelope stays off the wire."""

from __future__ import annotations

from lupaxa.switchboard.message import Message, Source


def test_wire_payload_is_only_the_bytes() -> None:
    message = Message.create(
        "line",
        Source(protocol="tcp", address="127.0.0.1", port=9),
        b"hello",
        session_id="session-1",
    )
    assert message.wire_payload() == b"hello"
    assert message.metadata["session_id"] == "session-1"
    assert b"session-1" not in message.wire_payload()
