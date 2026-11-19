"""Frame parsers for streams and datagrams.

The buffer never grows past the configured maximum. An oversized length
prefix is rejected before a body of that size is stored.
"""

from __future__ import annotations

import struct
from typing import Literal

from lupaxa.switchboard.config import FramingSpec
from lupaxa.switchboard.exceptions import FrameError

_NEED = object()
_DROP = object()


class FrameParser:
    """Extract payloads from a byte stream and encode them again."""

    def __init__(
        self,
        framing: FramingSpec,
        *,
        on_error: Literal["close", "drop"] = "close",
    ) -> None:
        self.framing = framing
        self.on_error = on_error
        self.dropped = 0
        self._buf = bytearray()
        self._tail = bytearray()
        self._skipping = False
        self._expected: int | None = None
        self._delimiter = b"\n" if framing.type == "newline" else framing.delimiter_bytes()

    @property
    def pending(self) -> bool:
        """True when a frame has started and is not complete."""
        return bool(self._buf) or self._skipping or bool(self._tail)

    @property
    def buffered(self) -> int:
        """Bytes held while waiting for a complete frame."""
        return len(self._buf) + len(self._tail)

    def feed(self, data: bytes) -> list[bytes]:
        """Accept the next read. Return every payload that is now complete."""
        if self.framing.type == "raw_stream":
            if not data:
                return []
            if len(data) > self.framing.max_frame_bytes:
                raise FrameError("The frame exceeds the size limit.", code="FRAME_TOO_LARGE")
            return [bytes(data)]
        frames: list[bytes] = []
        view = memoryview(data)
        index = 0
        while index < len(view) or self._has_frame():
            if self._skipping:
                index = self._skip(view, index)
                if self._skipping:
                    break
                continue
            if self.framing.type == "length_prefix":
                index = self._fill_length(view, index)
                if self._skipping:
                    continue
            else:
                index = self._fill_capped(view, index)
            progressed = False
            while True:
                item = self._next()
                if item is _NEED:
                    break
                progressed = True
                if not isinstance(item, bytes):
                    continue
                frames.append(item)
            if index >= len(view) and not progressed:
                break
            if index >= len(view) and not self._has_frame():
                break
        return frames

    def finish(self) -> list[bytes]:
        """End of the stream. A partial frame is an error."""
        if self.framing.type == "raw_stream" or not self.pending:
            return []
        raise FrameError("The stream ended inside a frame.", code="INCOMPLETE_FRAME")

    def encode(self, payload: bytes) -> bytes:
        """Turn a payload into the bytes the output adapter writes."""
        if len(payload) > self.framing.max_frame_bytes:
            raise FrameError("The frame exceeds the size limit.", code="FRAME_TOO_LARGE")
        kind = self.framing.type
        if kind == "raw_stream":
            return bytes(payload)
        if kind in {"newline", "delimiter"}:
            if self._delimiter in payload:
                raise FrameError(
                    "The payload contains the frame delimiter.",
                    code="MALFORMED_FRAME",
                )
            if self.framing.retain_delimiter:
                return bytes(payload)
            return bytes(payload) + self._delimiter
        if kind == "fixed_length":
            size = self.framing.size or 0
            if len(payload) != size:
                raise FrameError(
                    "The payload is not the configured fixed length.",
                    code="MALFORMED_FRAME",
                )
            return bytes(payload)
        return _pack_length(self.framing, len(payload)) + bytes(payload)

    def _has_frame(self) -> bool:
        if self.framing.type in {"newline", "delimiter"}:
            return self._delimiter in self._buf
        if self.framing.type == "fixed_length":
            return len(self._buf) >= (self.framing.size or 0)
        if self._expected is None:
            return False
        return len(self._buf) >= self.framing.prefix_bytes + self._expected

    def _fill_capped(self, view: memoryview, index: int) -> int:
        room = self._cap() - len(self._buf)
        if room <= 0 or index >= len(view):
            return index
        take = min(room, len(view) - index)
        self._buf.extend(view[index : index + take])
        return index + take

    def _fill_length(self, view: memoryview, index: int) -> int:
        prefix = self.framing.prefix_bytes
        if len(self._buf) < prefix and index < len(view):
            take = min(prefix - len(self._buf), len(view) - index)
            self._buf.extend(view[index : index + take])
            index += take
        if len(self._buf) < prefix:
            return index
        if self._expected is None:
            payload_len = _payload_length(self.framing, bytes(self._buf[:prefix]))
            if payload_len > self.framing.max_frame_bytes:
                self._fail("The length prefix exceeds the size limit.", "FRAME_TOO_LARGE")
                return index
            self._expected = payload_len
        need = prefix + self._expected - len(self._buf)
        if need > 0 and index < len(view):
            take = min(need, len(view) - index)
            self._buf.extend(view[index : index + take])
            index += take
        return index

    def _next(self) -> bytes | object:
        kind = self.framing.type
        if kind in {"newline", "delimiter"}:
            return self._next_delimited()
        if kind == "fixed_length":
            size = self.framing.size or 0
            if len(self._buf) < size:
                if len(self._buf) > self.framing.max_frame_bytes:
                    self._fail("The frame exceeds the size limit.", "FRAME_TOO_LARGE")
                return _NEED
            payload = bytes(self._buf[:size])
            del self._buf[:size]
            return payload
        if self._expected is None:
            return _NEED
        prefix = self.framing.prefix_bytes
        total = prefix + self._expected
        if len(self._buf) < total:
            return _NEED
        payload = bytes(self._buf[prefix:total])
        del self._buf[:total]
        self._expected = None
        return payload

    def _next_delimited(self) -> bytes | object:
        index = self._buf.find(self._delimiter)
        if index < 0:
            if len(self._buf) > self.framing.max_frame_bytes:
                self._fail("The frame exceeds the size limit.", "FRAME_TOO_LARGE")
                return _DROP
            return _NEED
        raw = bytes(self._buf[:index])
        del self._buf[: index + len(self._delimiter)]
        strip_cr = self.framing.type == "newline" and raw.endswith(b"\r")
        body = raw[:-1] if strip_cr else raw
        if len(body) > self.framing.max_frame_bytes:
            self.dropped += 1
            if self.on_error == "drop":
                return _DROP
            raise FrameError("The frame exceeds the size limit.", code="FRAME_TOO_LARGE")
        if self.framing.retain_delimiter:
            kept = body + (b"\r" if raw.endswith(b"\r") and self.framing.type == "newline" else b"")
            return kept + self._delimiter
        return body

    def _fail(self, message: str, code: str) -> None:
        delimited = self.framing.type in {"newline", "delimiter"}
        if self.on_error == "drop" and delimited:
            self.dropped += 1
            self._buf.clear()
            self._expected = None
            self._skipping = True
            return
        self._buf.clear()
        self._expected = None
        raise FrameError(message, code=code)

    def _skip(self, view: memoryview, index: int) -> int:
        self._tail.extend(view[index:])
        found = self._tail.find(self._delimiter)
        if found < 0:
            keep = max(0, len(self._delimiter) - 1)
            if len(self._tail) > keep:
                del self._tail[:-keep]
            return len(view)
        rest = bytes(self._tail[found + len(self._delimiter) :])
        self._tail.clear()
        self._skipping = False
        self._buf.extend(rest)
        return len(view)

    def _cap(self) -> int:
        if self.framing.type in {"newline", "delimiter"}:
            return self.framing.max_frame_bytes + max(1, len(self._delimiter))
        if self.framing.type == "fixed_length":
            return self.framing.size or self.framing.max_frame_bytes
        return self.framing.prefix_bytes + self.framing.max_frame_bytes


def frames_in_datagram(
    payload: bytes,
    framing: FramingSpec,
    *,
    on_error: Literal["close", "drop"] = "close",
) -> list[bytes]:
    """Split one UDP datagram. Bytes do not carry over to the next datagram."""
    parser = FrameParser(framing, on_error=on_error)
    found = parser.feed(payload)
    found.extend(parser.finish())
    return found


def _payload_length(framing: FramingSpec, prefix: bytes) -> int:
    endian = ">" if framing.endian == "big" else "<"
    fmt = {1: "B", 2: "H", 4: "I", 8: "Q"}[framing.prefix_bytes]
    declared = struct.unpack(endian + fmt, prefix)[0]
    payload_len = declared - framing.prefix_bytes if framing.includes_prefix else declared
    if payload_len < 0:
        raise FrameError("The length prefix is invalid.", code="INVALID_LENGTH")
    return int(payload_len)


def _pack_length(framing: FramingSpec, payload_len: int) -> bytes:
    declared = payload_len + framing.prefix_bytes if framing.includes_prefix else payload_len
    limit = 1 << (8 * framing.prefix_bytes)
    if declared >= limit:
        raise FrameError("The frame exceeds the length prefix.", code="FRAME_TOO_LARGE")
    endian = ">" if framing.endian == "big" else "<"
    fmt = {1: "B", 2: "H", 4: "I", 8: "Q"}[framing.prefix_bytes]
    return struct.pack(endian + fmt, declared)
