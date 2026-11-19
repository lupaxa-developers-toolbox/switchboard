"""Encode and decode protocol-profile structures.

Framing stays on the endpoint. A structure is turned into JSON once, on the
way in, and back into bytes once, on the way out. An endpoint without a
structure leaves the payload unchanged.
"""

from __future__ import annotations

import base64
import binascii
import json
import struct

from lupaxa.switchboard.config import (
    EndpointSpec,
    FieldSpec,
    ProfileSpec,
    StructureSpec,
    field_size,
)
from lupaxa.switchboard.exceptions import ProfileError

_SIGNED = {"int8", "int16", "int32", "int64"}
_FORMAT = {
    1: ("b", "B"),
    2: ("h", "H"),
    4: ("i", "I"),
    8: ("q", "Q"),
}


def view_payload(endpoint: EndpointSpec, payload: bytes) -> bytes:
    """Return the payload handed to the rest of the process."""
    if endpoint.structure is None:
        return payload
    limit = endpoint.framing.max_frame_bytes if endpoint.framing is not None else 16_777_216
    values = decode_structure(endpoint.structure, payload, max_bytes=limit)
    return json.dumps(values, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def wire_payload(endpoint: EndpointSpec, payload: bytes) -> bytes:
    """Return the bytes placed inside one outbound frame."""
    if endpoint.structure is None:
        return payload
    return encode_structure(endpoint.structure, _object(payload))


def decode_profile(profile: ProfileSpec, payload: bytes) -> str:
    """Decode one payload for the profile command."""
    if profile.structure is None:
        try:
            return payload.decode(profile.encoding)
        except UnicodeError as exc:
            raise ProfileError("payload does not match the profile encoding") from exc
    if profile.framing is not None:
        limit = profile.framing.max_frame_bytes
    else:
        limit = profile.limits.max_frame_bytes
    values = decode_structure(profile.structure, payload, max_bytes=limit)
    return json.dumps(values, ensure_ascii=False)


def encode_profile(profile: ProfileSpec, message: str) -> bytes:
    """Encode one JSON message for the profile command."""
    if profile.structure is None:
        text = _text(message)
        try:
            return text.encode(profile.encoding)
        except UnicodeError as exc:
            raise ProfileError("payload does not match the profile encoding") from exc
    return encode_structure(profile.structure, _object(message.encode("utf-8")))


def decode_structure(
    structure: StructureSpec,
    payload: bytes,
    *,
    max_bytes: int,
) -> dict[str, object]:
    """Read a flat structure. The result uses JSON-friendly values."""
    if len(payload) > max_bytes or len(payload) < structure.minimum_size:
        raise ProfileError("frame does not match the structure")
    values: dict[str, object] = {}
    offset = 0
    for field in structure.fields:
        value, offset = _read(structure, field, payload, offset)
        values[field.name] = value
    if offset != len(payload):
        raise ProfileError("frame has trailing bytes")
    _check_type(structure, values)
    _check_rules(structure, values)
    return values


def encode_structure(structure: StructureSpec, values: dict[str, object]) -> bytes:
    """Write a flat structure from a JSON object."""
    known = {field.name for field in structure.fields}
    if any(key not in known for key in values):
        raise ProfileError("unknown field")
    _check_type(structure, values)
    _check_rules(structure, values)
    parts = [_write(structure, field, values) for field in structure.fields]
    return b"".join(parts)


def _object(payload: bytes) -> dict[str, object]:
    try:
        loaded = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ProfileError("structured profile requires a JSON object") from exc
    if not isinstance(loaded, dict):
        raise ProfileError("structured profile requires a JSON object")
    return {str(key): value for key, value in loaded.items()}


def _text(message: str) -> str:
    try:
        loaded = json.loads(message)
    except json.JSONDecodeError as exc:
        raise ProfileError("message JSON is invalid") from exc
    if isinstance(loaded, str):
        return loaded
    payload = loaded.get("payload") if isinstance(loaded, dict) else None
    if isinstance(payload, str):
        return payload
    raise ProfileError("text profile requires a JSON string")


def _read(
    structure: StructureSpec,
    field: FieldSpec,
    payload: bytes,
    offset: int,
) -> tuple[object, int]:
    if field.type == "remaining_bytes":
        return _b64(payload[offset:]), len(payload)
    if field.type == "array":
        values: list[object] = []
        item = _item(field)
        count = field.count or 0
        for _ in range(count):
            value, offset = _read(structure, item, payload, offset)
            values.append(value)
        return values, offset
    width = field_size(field)
    if width is None or offset + width > len(payload):
        raise ProfileError("frame is shorter than the structure")
    chunk = payload[offset : offset + width]
    offset += width
    if field.type == "bytes":
        return _b64(chunk), offset
    if field.type == "string":
        return _text_value(structure, field, chunk), offset
    if field.type == "bool":
        number = _unpack(field.type, width, structure.endian, chunk)
        if number not in {0, 1}:
            raise ProfileError("field value does not fit")
        return bool(number), offset
    return _unpack(field.type, width, structure.endian, chunk), offset


def _write(structure: StructureSpec, field: FieldSpec, values: dict[str, object]) -> bytes:
    if field.name not in values:
        raise ProfileError("field is missing")
    value = values[field.name]
    if field.type == "remaining_bytes":
        return _bytes_value(value)
    if field.type == "array":
        if not isinstance(value, list) or field.count is None or len(value) != field.count:
            raise ProfileError("field has the wrong type")
        item = _item(field)
        return b"".join(_write(structure, item, {item.name: entry}) for entry in value)
    if field.type == "bytes":
        raw = _bytes_value(value)
        if field.length is None or len(raw) != field.length:
            raise ProfileError("field value does not fit")
        return raw
    if field.type == "string":
        return _pack_text(structure, field, value)
    if field.type == "bool":
        if value is True or value == 1:
            number = 1
        elif value is False or value == 0:
            number = 0
        else:
            raise ProfileError("field has the wrong type")
        width = field.length or 1
        return _pack("uint8" if width == 1 else f"uint{width * 8}", width, structure.endian, number)
    width = field_size(field) or 0
    if isinstance(value, str):
        value = _named_type(structure, field, value)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProfileError("field has the wrong type")
    return _pack(field.type, width, structure.endian, value)


def _item(field: FieldSpec) -> FieldSpec:
    length = field.length if field.item in {"bytes", "string"} else None
    return FieldSpec(
        name=field.name,
        type=field.item or "uint8",
        length=length,
        encoding=field.encoding,
    )


def _text_value(structure: StructureSpec, field: FieldSpec, chunk: bytes) -> str:
    encoding = field.encoding or structure.encoding
    try:
        return chunk.rstrip(b"\x00").decode(encoding)
    except UnicodeError as exc:
        raise ProfileError("payload does not match the profile encoding") from exc


def _pack_text(structure: StructureSpec, field: FieldSpec, value: object) -> bytes:
    if not isinstance(value, str) or field.length is None:
        raise ProfileError("field has the wrong type")
    encoding = field.encoding or structure.encoding
    try:
        raw = value.encode(encoding)
    except UnicodeError as exc:
        raise ProfileError("payload does not match the profile encoding") from exc
    if len(raw) > field.length:
        raise ProfileError("field value does not fit")
    return raw.ljust(field.length, b"\x00")


def _bytes_value(value: object) -> bytes:
    if not isinstance(value, str):
        raise ProfileError("field has the wrong type")
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ProfileError("field value does not fit") from exc


def _b64(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def _unpack(kind: str, width: int, endian: str, chunk: bytes) -> int:
    signed = kind in _SIGNED
    marker = _FORMAT[width][0 if signed else 1]
    prefix = ">" if endian == "big" else "<"
    try:
        value = struct.unpack(prefix + marker, chunk)[0]
    except struct.error as exc:
        raise ProfileError("field value does not fit") from exc
    if isinstance(value, int):
        return value
    raise ProfileError("field value does not fit")


def _pack(kind: str, width: int, endian: str, value: int) -> bytes:
    signed = kind in _SIGNED
    marker = _FORMAT[width][0 if signed else 1]
    prefix = ">" if endian == "big" else "<"
    try:
        return struct.pack(prefix + marker, value)
    except struct.error as exc:
        raise ProfileError("field value does not fit") from exc


def _named_type(structure: StructureSpec, field: FieldSpec, value: str) -> int:
    named = structure.message_type
    if named is None or named.field != field.name:
        raise ProfileError("field has the wrong type")
    for code, label in named.values.items():
        if label == value:
            return code
    raise ProfileError("unknown message type")


def _check_type(structure: StructureSpec, values: dict[str, object]) -> None:
    named = structure.message_type
    if named is None or not named.values:
        return
    value = values.get(named.field)
    if isinstance(value, str):
        if value not in named.values.values():
            raise ProfileError("unknown message type")
        return
    if isinstance(value, bool) or not isinstance(value, int) or value not in named.values:
        raise ProfileError("unknown message type")


def _check_rules(structure: StructureSpec, values: dict[str, object]) -> None:
    for rule in structure.rules:
        value = values.get(rule.field)
        if "equals" in rule.model_fields_set and value != rule.equals:
            raise ProfileError("field failed validation")
        if isinstance(value, bool) or not isinstance(value, int):
            if rule.minimum is not None or rule.maximum is not None:
                raise ProfileError("field failed validation")
            continue
        if rule.minimum is not None and value < rule.minimum:
            raise ProfileError("field failed validation")
        if rule.maximum is not None and value > rule.maximum:
            raise ProfileError("field failed validation")
