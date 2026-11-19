"""Conditional routing for independent messages."""

from __future__ import annotations

import re
from typing import Protocol

from lupaxa.switchboard.config import BridgeSpec, RouteSpec, WhenSpec


class _Fields(Protocol):
    fields: dict[str, object] | None
    text: str | None
    metadata: dict[str, str]


def select_outputs(
    bridge: BridgeSpec,
    routes: list[RouteSpec],
    document: _Fields,
) -> list[str] | None:
    """Return output names, or None when no rule matched.

    A router with no rules selects every output.
    """
    groups = _groups(bridge, routes)
    if not groups:
        return [item.name for item in bridge.outputs]
    chosen: list[str] = []
    matched = False
    for stop, rules in groups:
        for rule in rules:
            if not matches(rule.when, document):
                continue
            matched = True
            for name in rule.send_to:
                if name not in chosen:
                    chosen.append(name)
            if stop:
                return chosen
    if not matched:
        return None
    return chosen


def matches(spec: WhenSpec, document: _Fields) -> bool:
    """Evaluate one rule. A failed comparison does not match."""
    if spec.all is not None:
        return all(matches(item, document) for item in spec.all)
    if spec.any is not None:
        return any(matches(item, document) for item in spec.any)
    if spec.negated is not None:
        return not matches(spec.negated, document)
    value = _lookup(document, spec.field or "")
    present = _present(document, spec.field or "")
    if "exists" in spec.model_fields_set:
        return present is spec.exists
    if not present or spec.field is None:
        return False
    if "equals" in spec.model_fields_set:
        return value == spec.equals or str(value) == str(spec.equals)
    if "not_equals" in spec.model_fields_set:
        return value != spec.not_equals and str(value) != str(spec.not_equals)
    if "contains" in spec.model_fields_set:
        return _contains(value, spec.contains)
    if "starts_with" in spec.model_fields_set:
        prefix = spec.starts_with
        return isinstance(value, str) and isinstance(prefix, str) and value.startswith(prefix)
    if "ends_with" in spec.model_fields_set:
        suffix = spec.ends_with
        return isinstance(value, str) and isinstance(suffix, str) and value.endswith(suffix)
    if "greater_than" in spec.model_fields_set:
        number = _number(value)
        return number is not None and spec.greater_than is not None and number > spec.greater_than
    if "less_than" in spec.model_fields_set:
        number = _number(value)
        return number is not None and spec.less_than is not None and number < spec.less_than
    if "matches" in spec.model_fields_set:
        if not isinstance(value, str) or not isinstance(spec.matches, str):
            return False
        if len(value) > 8192:
            return False
        try:
            pattern = safe_pattern(spec.matches)
        except ValueError:
            return False
        return pattern.search(value) is not None
    return False


def safe_pattern(pattern: str) -> re.Pattern[str]:
    """Compile a bounded expression. Hostile patterns are rejected."""
    if len(pattern) > 128 or pattern.count("+") + pattern.count("*") > 6:
        raise ValueError("pattern is too complex")
    if re.search(r"\([^)]*[+*][^)]*\)[+*{]", pattern):
        raise ValueError("pattern is too complex")
    return re.compile(pattern)


def _groups(bridge: BridgeSpec, routes: list[RouteSpec]) -> list[tuple[bool, list]]:
    groups: list[tuple[bool, list]] = []
    if bridge.rules:
        groups.append((bridge.stop_on_match, list(bridge.rules)))
    for route in routes:
        if route.from_bridge == bridge.name and route.rules:
            groups.append((route.stop_on_match, list(route.rules)))
    return groups


def _present(document: _Fields, field: str) -> bool:
    if field == "":
        return False
    if field.startswith("metadata."):
        return field.removeprefix("metadata.") in document.metadata
    if document.fields is None:
        return False
    current: object = document.fields
    for part in _path(field):
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return True


def _lookup(document: _Fields, field: str) -> object:
    if field.startswith("metadata."):
        return document.metadata.get(field.removeprefix("metadata."))
    if document.fields is None:
        return None
    current: object = document.fields
    for part in _path(field):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def _path(field: str) -> list[str]:
    parts = [part for part in field.split(".") if part]
    if parts and parts[0] == "payload":
        return parts[1:]
    return parts


def _contains(value: object, expected: object) -> bool:
    if isinstance(value, str) and isinstance(expected, str):
        return expected in value
    if isinstance(value, list):
        return expected in value
    return False


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        return float(value)
    except ValueError:
        return None
