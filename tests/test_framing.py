"""Framing strategies, including partial reads and hostile lengths."""

from __future__ import annotations

import contextlib
import struct

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from lupaxa.switchboard.config import FramingSpec
from lupaxa.switchboard.exceptions import FrameError
from lupaxa.switchboard.framing import FrameParser

_PAYLOADS = st.binary(max_size=20).filter(
    lambda value: b"\n" not in value and not value.endswith(b"\r")
)


def test_newline_splits_a_combined_read_and_a_partial_read() -> None:
    parser = FrameParser(_newline())
    assert parser.feed(b"one\ntwo\n") == [b"one", b"two"]
    again = FrameParser(_newline())
    assert again.feed(b"hel") == []
    assert again.pending
    assert again.feed(b"lo\n") == [b"hello"]


def test_newline_removes_a_preceding_carriage_return() -> None:
    parser = FrameParser(_newline())
    assert parser.feed(b"hello\r\n") == [b"hello"]


def test_delimiter_round_trip() -> None:
    framing = FramingSpec(type="delimiter", delimiter="\r\n", max_frame_bytes=32)
    parser = FrameParser(framing)
    blob = parser.encode(b"ping") + parser.encode(b"pong")
    assert parser.feed(blob) == [b"ping", b"pong"]


def test_fixed_length_waits_for_a_full_frame() -> None:
    framing = FramingSpec(type="fixed_length", size=4, max_frame_bytes=4)
    parser = FrameParser(framing)
    assert parser.feed(b"ab") == []
    assert parser.feed(b"cd") == [b"abcd"]
    fresh = FrameParser(framing)
    assert fresh.feed(b"abcdefgh") == [b"abcd", b"efgh"]
    partial = FrameParser(framing)
    partial.feed(b"abc")
    with pytest.raises(FrameError):
        partial.finish()


def test_length_prefix_rejects_an_oversized_declaration_without_buffering_it() -> None:
    framing = FramingSpec(type="length_prefix", prefix_bytes=4, endian="big", max_frame_bytes=8)
    parser = FrameParser(framing)
    declared = struct.pack(">I", 2**31)
    with pytest.raises(FrameError, match="size limit"):
        parser.feed(declared + b"x" * 64)
    assert parser.buffered <= 8


def test_length_prefix_round_trip_includes_prefix_and_little_endian() -> None:
    framing = FramingSpec(
        type="length_prefix",
        prefix_bytes=2,
        endian="little",
        includes_prefix=True,
        max_frame_bytes=16,
    )
    parser = FrameParser(framing)
    blob = parser.encode(b"ab") + parser.encode(b"")
    assert parser.feed(blob) == [b"ab", b""]


def test_incomplete_frame_at_disconnect_is_an_error() -> None:
    parser = FrameParser(_newline())
    parser.feed(b"partial")
    with pytest.raises(FrameError, match="inside a frame"):
        parser.finish()


def test_drop_policy_skips_an_oversized_line_and_keeps_the_next() -> None:
    framing = FramingSpec(type="newline", max_frame_bytes=4)
    parser = FrameParser(framing, on_error="drop")
    assert parser.feed(b"12345\nOK\n") == [b"OK"]
    assert parser.dropped == 1


def test_raw_stream_forwards_each_read() -> None:
    framing = FramingSpec(type="raw_stream", max_frame_bytes=8)
    parser = FrameParser(framing)
    assert parser.feed(b"abc") == [b"abc"]
    assert parser.finish() == []


@given(st.lists(_PAYLOADS, max_size=8))
@settings(max_examples=40)
def test_newline_round_trip_one_byte_at_a_time(parts: list[bytes]) -> None:
    parser = FrameParser(_newline())
    blob = b"".join(parser.encode(part) for part in parts)
    found: list[bytes] = []
    reader = FrameParser(_newline())
    for index in range(len(blob)):
        found.extend(reader.feed(blob[index : index + 1]))
    assert found == parts


@given(st.binary(max_size=200))
@settings(max_examples=40)
def test_hostile_bytes_stay_within_the_cap(data: bytes) -> None:
    framing = FramingSpec(type="newline", max_frame_bytes=16)
    parser = FrameParser(framing)
    with contextlib.suppress(FrameError):
        parser.feed(data)
    assert parser.buffered <= framing.max_frame_bytes + 1


def _newline() -> FramingSpec:
    return FramingSpec(type="newline", max_frame_bytes=64)
