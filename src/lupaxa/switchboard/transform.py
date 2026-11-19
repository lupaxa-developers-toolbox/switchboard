"""Declarative message transforms. They do not execute Python expressions."""

from __future__ import annotations

import base64
import binascii
import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from lupaxa.switchboard.config import TransformSpec
from lupaxa.switchboard.route import matches, safe_pattern

_TEMPLATE = re.compile(r"\{\{\s*(metadata\.[A-Za-z_][A-Za-z0-9_]*|[A-Za-z_][A-Za-z0-9_]*)\s*\}\}")
_JSON_FIELD = re.compile(
    r'"([^"\\]+)"\s*:\s*'
    r'("(?:\\.|[^"\\])*"|-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?|true|false|null)'
)


class TransformError(Exception):
    """A transform could not be applied. The message does not include the payload."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


@dataclass
class Document:
    """The working copy of one routed message."""

    data: bytes
    text: str | None = None
    fields: dict[str, Any] | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    content_type: str = "application/octet-stream"
    dropped: bool = False


def redact_fields(payload: bytes, names: list[str]) -> bytes:
    """Replace configured JSON field values. The original values are not returned."""
    if not names:
        return payload
    try:
        text = payload.decode("utf-8")
    except UnicodeError:
        return payload
    hidden = set(names)

    def replace(match: re.Match[str]) -> str:
        if match.group(1) not in hidden:
            return match.group(0)
        return f'"{match.group(1)}":"*"'

    return _JSON_FIELD.sub(replace, text).encode("utf-8")


def run_pipeline(
    document: Document,
    steps: list[TransformSpec],
    *,
    on_error: str,
) -> Document | None:
    """Apply each step. drop and skip are handled here. Other policies propagate."""
    for step in steps:
        try:
            document = apply_step(step, document)
        except TransformError:
            if on_error == "skip":
                continue
            if on_error == "drop":
                return None
            raise
        if document.dropped:
            return None
    if document.fields is not None and document.text is None:
        document.text = json.dumps(document.fields, separators=(",", ":"))
        document.data = document.text.encode("utf-8")
        document.content_type = "application/json"
    return document


def apply_step(step: TransformSpec, document: Document) -> Document:
    """Apply one transform."""
    kind = step.type
    if kind == "decode":
        document.text = _decode(document.data, step.encoding)
        return document
    if kind == "encode":
        if document.text is None:
            raise TransformError("encode requires text")
        document.data = document.text.encode(step.encoding)
        return document
    if kind == "json-parse":
        raw = document.text if document.text is not None else _decode(document.data, step.encoding)
        parsed = _json_object(raw)
        document.fields = parsed
        document.text = None
        return document
    if kind == "json-serialize":
        if document.fields is None:
            raise TransformError("json-serialize requires fields")
        document.text = json.dumps(document.fields, separators=(",", ":"))
        document.data = document.text.encode("utf-8")
        document.content_type = "application/json"
        return document
    if kind == "prefix":
        return _set_text(document, step.value + _text(document))
    if kind == "suffix":
        return _set_text(document, _text(document) + step.value)
    if kind == "replace":
        return _set_text(document, _text(document).replace(step.find, step.replacement))
    if kind == "regex":
        try:
            pattern = safe_pattern(step.pattern)
        except ValueError as exc:
            raise TransformError("pattern is too complex") from exc
        subject = _text(document)
        if len(subject) > 8192:
            raise TransformError("regex input is too large")
        return _set_text(document, pattern.sub(step.replacement, subject, count=100))
    if kind == "field-map":
        document.fields = _map_fields(document, step)
        document.text = None
        return document
    if kind == "extract":
        return _extract(document, step)
    if kind == "rename":
        fields = _fields(document)
        if step.source not in fields:
            raise TransformError("field is missing")
        fields[step.target] = fields.pop(step.source)
        document.fields = fields
        document.text = None
        return document
    if kind == "remove":
        fields = _fields(document)
        fields.pop(step.field, None)
        document.fields = fields
        document.text = None
        return document
    if kind == "set":
        fields = _fields(document)
        fields[step.field] = step.value
        document.fields = fields
        document.text = None
        return document
    if kind == "timestamp":
        fields = _fields(document)
        name = step.field or "timestamp"
        fields[name] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        document.fields = fields
        document.text = None
        return document
    if kind == "require":
        fields = _fields(document)
        for name in step.fields:
            if name not in fields or fields[name] is None:
                raise TransformError("required field is missing")
        return document
    if kind == "permit":
        fields = _fields(document)
        value = fields.get(step.field)
        if str(value) not in step.values:
            raise TransformError("field value is not permitted")
        return document
    if kind == "filter":
        if step.when is not None and matches(step.when, document):
            if step.action == "drop":
                document.dropped = True
            return document
        if step.when is not None and step.action == "keep":
            document.dropped = True
        return document
    if kind == "hex-encode":
        return _set_text(document, document.data.hex())
    if kind == "hex-decode":
        try:
            document.data = bytes.fromhex(_text(document))
        except ValueError as exc:
            raise TransformError("hex payload is invalid") from exc
        document.text = None
        document.fields = None
        return document
    if kind == "base64-encode":
        return _set_text(document, base64.b64encode(document.data).decode("ascii"))
    if kind == "base64-decode":
        try:
            document.data = base64.b64decode(_text(document), validate=True)
        except (ValueError, binascii.Error) as exc:
            raise TransformError("base64 payload is invalid") from exc
        document.text = None
        document.fields = None
        return document
    if kind == "template":
        rendered = _TEMPLATE.sub(
            lambda match: _template_value(document, match.group(1)),
            step.template,
        )
        document.text = rendered
        document.data = document.text.encode("utf-8")
        document.fields = None
        document.content_type = "text/plain"
        return document
    raise TransformError("transform is not available")


def _set_text(document: Document, text: str) -> Document:
    document.text = text
    document.data = text.encode("utf-8")
    document.fields = None
    return document


def _decode(data: bytes, encoding: str) -> str:
    try:
        return data.decode(encoding)
    except (LookupError, UnicodeError) as exc:
        raise TransformError("payload could not be decoded") from exc


def _text(document: Document) -> str:
    if document.text is not None:
        return document.text
    try:
        return document.data.decode("utf-8")
    except UnicodeError as exc:
        raise TransformError("payload could not be decoded") from exc


def _json_object(raw: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TransformError("payload is not json") from exc
    if not isinstance(parsed, dict):
        raise TransformError("json payload must be an object")
    return {str(key): value for key, value in parsed.items()}


def _fields(document: Document) -> dict[str, Any]:
    if document.fields is None:
        raise TransformError("transform requires fields")
    return dict(document.fields)


def _map_fields(document: Document, step: TransformSpec) -> dict[str, Any]:
    source = _fields(document)
    mapped: dict[str, Any] = {}
    used: set[str] = set()
    for original, target in step.mappings.items():
        if original in source:
            mapped[target] = source[original]
            used.add(original)
    if step.keep:
        for key, value in source.items():
            if key not in used and key not in mapped:
                mapped[key] = value
    return mapped


def _extract(document: Document, step: TransformSpec) -> Document:
    parts = [part for part in step.path.split(".") if part and part != "payload"]
    current: object = document.fields if document.fields is not None else None
    found = True
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            found = False
            break
        current = current[part]
    if not found or not parts:
        if step.on_missing == "default":
            document.text = step.default or ""
            document.data = document.text.encode("utf-8")
            document.fields = None
            return document
        raise TransformError("field is missing")
    if isinstance(current, dict):
        document.fields = {str(key): value for key, value in current.items()}
        document.text = None
        return document
    document.fields = None
    document.text = str(current)
    document.data = document.text.encode("utf-8")
    return document


def _template_value(document: Document, name: str) -> str:
    if name.startswith("metadata."):
        return document.metadata.get(name.removeprefix("metadata."), "")
    if document.fields is None or name not in document.fields:
        return ""
    value = document.fields[name]
    if isinstance(value, str):
        return value
    if value is None:
        return ""
    return json.dumps(value, separators=(",", ":"))
