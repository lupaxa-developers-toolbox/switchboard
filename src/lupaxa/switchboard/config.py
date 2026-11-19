"""Versioned YAML configuration.

Built-in defaults are filled in by the models. A user file is merged under an
explicit file. Lists in the explicit file replace lists from the user file.
"""

from __future__ import annotations

import ipaddress
import re
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import platformdirs
import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from lupaxa.switchboard.exceptions import ConfigError

_MISSING = object()


class FramingSpec(BaseModel):
    """How a TCP stream, or one UDP datagram, is split into payloads."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["newline", "delimiter", "fixed_length", "length_prefix", "raw_stream"]
    max_frame_bytes: int = Field(default=65536, gt=0, le=16_777_216)
    retain_delimiter: bool = False
    delimiter: str = "\n"
    size: int | None = Field(default=None, gt=0)
    prefix_bytes: Literal[1, 2, 4, 8] = 4
    endian: Literal["big", "little"] = "big"
    includes_prefix: bool = False
    encoding: str = "utf-8"

    @model_validator(mode="after")
    def _check(self) -> FramingSpec:
        try:
            "\n".encode(self.encoding)
        except LookupError as exc:
            raise ValueError(f"unknown encoding {self.encoding}") from exc
        if self.type == "delimiter":
            if self.delimiter == "":
                raise ValueError("delimiter must not be empty")
            try:
                encoded = self.delimiter.encode(self.encoding)
            except UnicodeError as exc:
                raise ValueError("delimiter does not match the framing encoding") from exc
            if encoded == b"":
                raise ValueError("delimiter must not be empty")
        if self.type == "fixed_length":
            if self.size is None:
                raise ValueError("fixed_length requires size")
            if self.size > self.max_frame_bytes:
                raise ValueError("size exceeds max_frame_bytes")
        return self

    def delimiter_bytes(self) -> bytes:
        """The delimiter as it appears on the wire. Newline framing uses LF."""
        if self.type == "newline":
            return b"\n"
        if self.type != "delimiter":
            return b""
        return self.delimiter.encode(self.encoding)


class TlsSpec(BaseModel):
    """Certificate material for one endpoint. Verification is on unless disabled."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    certfile: str | None = None
    keyfile: str | None = None
    cafile: str | None = None
    verify: bool = True


class HttpSpec(BaseModel):
    """Request limits and how an HTTP input answers the caller."""

    model_config = ConfigDict(extra="forbid")

    response: Literal["accepted", "forwarded", "upstream"] = "forwarded"
    response_status: int = Field(default=202, ge=100, le=599)
    max_body_bytes: int = Field(default=1_048_576, gt=0, le=16_777_216)
    max_header_bytes: int = Field(default=8192, gt=0, le=65536)
    allowed_methods: list[str] = Field(default_factory=lambda: ["POST"])
    forward_headers: list[str] = Field(default_factory=list)
    timeout_seconds: float = Field(default=30, gt=0)

    @model_validator(mode="after")
    def _check(self) -> HttpSpec:
        methods = [method.upper() for method in self.allowed_methods]
        if not methods or any(not method.isalpha() for method in methods):
            raise ValueError("allowed_methods must name HTTP methods")
        self.allowed_methods = methods
        self.forward_headers = [name.lower() for name in self.forward_headers]
        return self


class WebSocketSpec(BaseModel):
    """WebSocket message limits and ping interval."""

    model_config = ConfigDict(extra="forbid")

    max_message_bytes: int = Field(default=1_048_576, gt=0, le=16_777_216)
    heartbeat_seconds: float = Field(default=20, gt=0)


class ReconnectSpec(BaseModel):
    """Outbound connect retries. Payloads that were already written are not replayed."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    policy: Literal["fixed", "exponential"] = "fixed"
    max_attempts: int = Field(default=3, gt=0, le=20)
    initial_delay_seconds: float = Field(default=0.2, gt=0)
    max_delay_seconds: float = Field(default=5, gt=0)

    @model_validator(mode="after")
    def _check(self) -> ReconnectSpec:
        if self.initial_delay_seconds > self.max_delay_seconds:
            raise ValueError("initial_delay_seconds exceeds max_delay_seconds")
        return self


_FIELD_TYPES = {
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "int8",
    "int16",
    "int32",
    "int64",
    "bytes",
    "string",
    "remaining_bytes",
    "bool",
    "array",
}
_INTEGER_WIDTH = {
    "uint8": 1,
    "int8": 1,
    "uint16": 2,
    "int16": 2,
    "uint32": 4,
    "int32": 4,
    "uint64": 8,
    "int64": 8,
}
_ITEM_TYPES = set(_INTEGER_WIDTH) | {"bytes", "string", "bool"}


class FieldSpec(BaseModel):
    """One fixed field in a binary structure."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    type: str
    length: int | None = Field(default=None, gt=0, le=16_777_216)
    count: int | None = Field(default=None, gt=0, le=4096)
    item: str | None = None
    encoding: str | None = None

    @model_validator(mode="after")
    def _check(self) -> FieldSpec:
        if self.type not in _FIELD_TYPES:
            raise ValueError("unknown field type")
        if self.encoding is not None:
            _known_encoding(self.encoding)
        if self.type in _INTEGER_WIDTH and (self.length or self.count or self.item):
            raise ValueError("invalid field length")
        bad_bool = self.length not in {None, 1, 2, 4, 8} or self.count or self.item
        if self.type == "bool" and bad_bool:
            raise ValueError("invalid field length")
        if self.type in {"bytes", "string"} and self.length is None:
            raise ValueError("invalid field length")
        if self.type == "remaining_bytes" and (self.length or self.count or self.item):
            raise ValueError("ambiguous field boundary")
        if self.type == "array":
            if self.count is None or self.item not in _ITEM_TYPES:
                raise ValueError("invalid field length")
            if self.item in {"bytes", "string"} and self.length is None:
                raise ValueError("invalid field length")
        elif self.item is not None or (self.count is not None and self.type != "array"):
            raise ValueError("invalid field length")
        return self


class MessageTypeSpec(BaseModel):
    """Names for the integer that identifies a message."""

    model_config = ConfigDict(extra="forbid")

    field: str
    values: dict[int, str] = Field(default_factory=dict)


class ProfileRule(BaseModel):
    """A bound checked after a structure is decoded."""

    model_config = ConfigDict(extra="forbid")

    field: str
    equals: int | str | bool | None = None
    minimum: int | None = None
    maximum: int | None = None

    @model_validator(mode="after")
    def _check(self) -> ProfileRule:
        if "equals" not in self.model_fields_set and self.minimum is None and self.maximum is None:
            raise ValueError("validation rule is empty")
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError("invalid field length")
        return self


class StructureSpec(BaseModel):
    """A flat binary layout. Variable-length records and nested structs are rejected."""

    model_config = ConfigDict(extra="forbid")

    endian: Literal["big", "little"] = "big"
    encoding: str = "utf-8"
    fields: list[FieldSpec] = Field(min_length=1)
    message_type: MessageTypeSpec | None = None
    rules: list[ProfileRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> StructureSpec:
        _known_encoding(self.encoding)
        names = [item.name for item in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("duplicate field name")
        remaining = [item for item in self.fields if item.type == "remaining_bytes"]
        if len(remaining) > 1 or (remaining and self.fields[-1].type != "remaining_bytes"):
            raise ValueError("ambiguous field boundary")
        if self.minimum_size > 16_777_216:
            raise ValueError("structure exceeds the maximum frame size")
        known = set(names)
        if self.message_type is not None:
            named = self.message_type.field
            field = next((item for item in self.fields if item.name == named), None)
            if field is None or field.type not in _INTEGER_WIDTH:
                raise ValueError("message type field is not an integer")
        for rule in self.rules:
            if rule.field not in known:
                raise ValueError("validation rule names an unknown field")
        return self

    @property
    def minimum_size(self) -> int:
        """Bytes required before a remaining-bytes field."""
        return sum(size for item in self.fields if (size := field_size(item)) is not None)

    @property
    def exact_size(self) -> int | None:
        """Total size when every field has a fixed width."""
        if any(item.type == "remaining_bytes" for item in self.fields):
            return None
        return self.minimum_size


class ProfileLimits(BaseModel):
    """Size cap copied onto the profile framing."""

    model_config = ConfigDict(extra="forbid")

    max_frame_bytes: int = Field(default=65536, gt=0, le=16_777_216)


class ProfileSpec(BaseModel):
    """A reusable transport, framing, and optional binary structure."""

    model_config = ConfigDict(extra="forbid")

    transport: Literal["tcp", "udp", "websocket", "http"]
    framing: FramingSpec | None = None
    encoding: str = "utf-8"
    limits: ProfileLimits = Field(default_factory=ProfileLimits)
    structure: StructureSpec | None = None

    @model_validator(mode="before")
    @classmethod
    def _prepare(cls, data: object) -> object:
        if isinstance(data, dict):
            return _prepare_profile(dict(data))
        return data

    @model_validator(mode="after")
    def _check(self) -> ProfileSpec:
        _known_encoding(self.encoding)
        if self.transport == "tcp" and self.framing is None:
            raise ValueError("TCP profiles require framing")
        if self.transport in {"websocket", "http"} and self.framing is not None:
            raise ValueError("WebSocket and HTTP messages already have boundaries")
        if self.framing is not None and self.framing.type == "raw_stream" and self.structure:
            raise ValueError("conflicting framing and structure")
        if self.structure is None or self.framing is None:
            return self
        if self.structure.minimum_size > self.framing.max_frame_bytes:
            raise ValueError("structure exceeds the maximum frame size")
        exact = self.structure.exact_size
        if self.framing.type == "fixed_length":
            size = self.framing.size or 0
            if exact is not None and size != exact:
                raise ValueError("conflicting framing and structure")
            if exact is None and size < self.structure.minimum_size:
                raise ValueError("conflicting framing and structure")
        return self


class EndpointSpec(BaseModel):
    """One end of a bridge."""

    model_config = ConfigDict(extra="forbid")

    protocol: Literal["tcp", "udp", "websocket", "http"]
    host: str = "127.0.0.1"
    port: int | None = Field(default=None, ge=0, le=65535)
    path: str = "/"
    method: str = "POST"
    url: str | None = None
    framing: FramingSpec | None = None
    tls: TlsSpec | None = None
    profile: str | None = None
    structure: StructureSpec | None = None

    @model_validator(mode="after")
    def _check(self) -> EndpointSpec:
        if self.host.strip() == "":
            raise ValueError("host must not be empty")
        if not self.path.startswith("/"):
            raise ValueError("path must start with /")
        self.method = self.method.upper()
        if not self.method.isalpha():
            raise ValueError("method must be an HTTP method name")
        if self.url is not None:
            _apply_url(self)
        if self.port is None:
            raise ValueError("port is required")
        if self.protocol == "tcp" and self.framing is None:
            raise ValueError("TCP endpoints require framing")
        if self.protocol in {"websocket", "http"} and self.framing is not None:
            raise ValueError("WebSocket and HTTP messages already have boundaries")
        raw_stream = self.framing is not None and self.framing.type == "raw_stream"
        if raw_stream and self.protocol != "tcp":
            raise ValueError("raw_stream is only valid on TCP")
        if self.protocol == "udp" and self.tls is not None and self.tls.enabled:
            raise ValueError("TLS is not available on UDP")
        if self.protocol in {"tcp", "udp"} and self.url is not None:
            raise ValueError("url is only valid on WebSocket and HTTP endpoints")
        return self


class LimitsSpec(BaseModel):
    """Timeouts and connection caps for one bridge."""

    model_config = ConfigDict(extra="forbid")

    max_connections: int = Field(default=100, gt=0)
    idle_timeout_seconds: float = Field(default=300, gt=0)
    connect_timeout_seconds: float = Field(default=10, gt=0)
    read_timeout_seconds: float | None = Field(default=None, gt=0)
    write_timeout_seconds: float = Field(default=30, gt=0)
    frame_timeout_seconds: float = Field(default=30, gt=0)
    max_write_buffer_bytes: int = Field(default=1_048_576, gt=0)


class DisconnectSpec(BaseModel):
    """What happens when one side of a TCP session finishes sending."""

    model_config = ConfigDict(extra="forbid")

    on_peer_close: Literal["close_both", "half_close"] = "close_both"


class UdpSpec(BaseModel):
    """Datagram limits. UDP does not gain delivery or ordering guarantees."""

    model_config = ConfigDict(extra="forbid")

    max_datagram_bytes: int = Field(default=65507, gt=0, le=65507)
    overflow: Literal["drop", "reject"] = "drop"


class WhenSpec(BaseModel):
    """One routing comparison. Exactly one operator is set."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    field: str | None = None
    equals: object | None = None
    not_equals: object | None = None
    exists: bool | None = None
    contains: object | None = None
    starts_with: str | None = None
    ends_with: str | None = None
    greater_than: float | None = None
    less_than: float | None = None
    matches: str | None = None
    all: list[WhenSpec] | None = None
    any: list[WhenSpec] | None = None
    negated: WhenSpec | None = Field(default=None, alias="not")

    @model_validator(mode="after")
    def _check(self) -> WhenSpec:
        logic = [name for name in ("all", "any", "negated") if name in self.model_fields_set]
        compare = [
            name
            for name in (
                "equals",
                "not_equals",
                "exists",
                "contains",
                "starts_with",
                "ends_with",
                "greater_than",
                "less_than",
                "matches",
            )
            if name in self.model_fields_set
        ]
        if len(logic) + len(compare) != 1:
            raise ValueError("a rule needs exactly one operator")
        if compare and compare[0] != "exists" and not self.field:
            raise ValueError("field is required")
        if self.matches is not None:
            _check_pattern(self.matches)
        for group in (self.all, self.any):
            if group is not None and not group:
                raise ValueError("a logical operator needs at least one rule")
        return self


class RuleSpec(BaseModel):
    """Send a matching message to the named outputs."""

    model_config = ConfigDict(extra="forbid")

    when: WhenSpec
    send_to: list[str] = Field(min_length=1)


class RouteSpec(BaseModel):
    """Rules attached to one bridge by name."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    from_bridge: str = Field(alias="from")
    rules: list[RuleSpec] = Field(default_factory=list)
    stop_on_match: bool = False


class TransformSpec(BaseModel):
    """One declarative transform. Values are data, not code."""

    model_config = ConfigDict(extra="forbid")

    type: Literal[
        "decode",
        "encode",
        "json-parse",
        "json-serialize",
        "prefix",
        "suffix",
        "replace",
        "regex",
        "field-map",
        "extract",
        "rename",
        "remove",
        "set",
        "timestamp",
        "require",
        "permit",
        "filter",
        "hex-encode",
        "hex-decode",
        "base64-encode",
        "base64-decode",
        "template",
    ]
    encoding: str = "utf-8"
    mappings: dict[str, str] = Field(default_factory=dict)
    keep: bool = True
    path: str = ""
    on_missing: Literal["error", "default"] = "error"
    default: str | None = None
    source: str = ""
    target: str = ""
    field: str = ""
    value: str = ""
    fields: list[str] = Field(default_factory=list)
    values: list[str] = Field(default_factory=list)
    find: str = ""
    replacement: str = ""
    pattern: str = ""
    when: WhenSpec | None = None
    action: Literal["drop", "keep"] = "drop"
    template: str = ""

    @model_validator(mode="after")
    def _check(self) -> TransformSpec:
        if self.pattern:
            _check_pattern(self.pattern)
        if self.type == "regex" and self.pattern == "":
            raise ValueError("regex requires a pattern")
        if self.type == "field-map" and not self.mappings:
            raise ValueError("field-map requires mappings")
        if self.type == "extract" and self.path == "":
            raise ValueError("extract requires a path")
        needs_field = self.type in {"rename", "remove", "set", "permit"}
        if needs_field and self.field == "" and self.source == "":
            raise ValueError("the transform requires a field")
        if self.type == "filter" and self.when is None:
            raise ValueError("filter requires when")
        if self.type == "template" and self.template == "":
            raise ValueError("template requires a template")
        return self


class DirectionSpec(BaseModel):
    """Transforms for one direction of a gateway session."""

    model_config = ConfigDict(extra="forbid")

    transforms: list[TransformSpec] = Field(default_factory=list)


_PIPELINE_TYPES: dict[str, tuple[set[str], str | None]] = {
    "decode": ({"bytes"}, "text"),
    "encode": ({"bytes", "text"}, "bytes"),
    "json-parse": ({"bytes", "text"}, "fields"),
    "json-serialize": ({"fields"}, "bytes"),
    "prefix": ({"bytes", "text"}, "text"),
    "suffix": ({"bytes", "text"}, "text"),
    "replace": ({"bytes", "text"}, "text"),
    "regex": ({"bytes", "text"}, "text"),
    "template": ({"bytes", "text"}, "text"),
    "hex-encode": ({"bytes"}, "text"),
    "hex-decode": ({"bytes", "text"}, "bytes"),
    "base64-encode": ({"bytes"}, "text"),
    "base64-decode": ({"bytes", "text"}, "bytes"),
    "field-map": ({"fields"}, "fields"),
    "extract": ({"fields"}, "fields"),
    "rename": ({"fields"}, "fields"),
    "remove": ({"fields"}, "fields"),
    "set": ({"fields"}, "fields"),
    "timestamp": ({"fields"}, "fields"),
    "require": ({"fields"}, "fields"),
    "permit": ({"fields"}, "fields"),
    "filter": ({"bytes", "text", "fields"}, None),
}


def _check_pipeline(steps: list[TransformSpec]) -> None:
    """Reject a step that does not accept the payload type produced so far."""
    current = "bytes"
    for step in steps:
        accepts, produces = _PIPELINE_TYPES[step.type]
        if current not in accepts:
            raise ValueError("incompatible transformation")
        if produces is not None:
            current = produces


class RetryDeliverySpec(BaseModel):
    """Bounded retries. A payload that may already have been sent is not replayed."""

    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=0, ge=0, le=20)
    initial_delay_ms: int = Field(default=500, gt=0, le=60_000)
    backoff: Literal["fixed", "exponential"] = "fixed"
    max_delay_ms: int = Field(default=5000, gt=0, le=60_000)
    replay: bool = False

    @model_validator(mode="after")
    def _check(self) -> RetryDeliverySpec:
        if self.initial_delay_ms > self.max_delay_ms:
            raise ValueError("initial_delay_ms exceeds max_delay_ms")
        return self


class BufferSpec(BaseModel):
    """In-memory queue limits for one output. This is not durable storage."""

    model_config = ConfigDict(extra="forbid")

    max_messages: int = Field(default=100, gt=0, le=100_000)
    max_bytes: int = Field(default=1_048_576, gt=0, le=16_777_216)
    overflow: Literal["reject", "drop_newest", "drop_oldest"] = "reject"


class DeliverySpec(BaseModel):
    """How a router treats fan-out, retries, and buffering."""

    model_config = ConfigDict(extra="forbid")

    success: Literal["all", "any", "best_effort"] = "best_effort"
    retry: RetryDeliverySpec = Field(default_factory=RetryDeliverySpec)
    buffer: BufferSpec = Field(default_factory=BufferSpec)


class OutputSpec(EndpointSpec):
    """A named router destination."""

    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class BridgeSpec(BaseModel):
    """One configured path from an input listener to one or more outputs."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    enabled: bool = True
    mode: Literal["gateway", "router"]
    input: EndpointSpec
    output: EndpointSpec | None = None
    outputs: list[OutputSpec] = Field(default_factory=list)
    limits: LimitsSpec = Field(default_factory=LimitsSpec)
    disconnect: DisconnectSpec = Field(default_factory=DisconnectSpec)
    framing_error: Literal["close", "drop"] = "close"
    udp: UdpSpec = Field(default_factory=UdpSpec)
    http: HttpSpec = Field(default_factory=HttpSpec)
    websocket: WebSocketSpec = Field(default_factory=WebSocketSpec)
    reconnect: ReconnectSpec = Field(default_factory=ReconnectSpec)
    transforms: list[TransformSpec] = Field(default_factory=list)
    forward: DirectionSpec | None = None
    reverse: DirectionSpec | None = None
    rules: list[RuleSpec] = Field(default_factory=list)
    stop_on_match: bool = False
    on_unmatched: Literal["drop", "error", "default"] = "drop"
    default_output: str | None = None
    transform_error: Literal["drop", "skip", "fail", "close", "route"] = "fail"
    error_output: str | None = None
    max_hops: int = Field(default=8, gt=0, le=64)
    delivery: DeliverySpec = Field(default_factory=DeliverySpec)

    @model_validator(mode="after")
    def _check(self) -> BridgeSpec:
        if self.mode == "gateway":
            if self.output is None:
                raise ValueError("gateway mode requires an output")
            if self.outputs or self.rules or self.transforms:
                raise ValueError("outputs, rules, and transforms require router mode")
        else:
            if self.forward is not None or self.reverse is not None:
                raise ValueError("forward and reverse require gateway mode")
            if self.output is not None:
                raise ValueError("router mode uses outputs")
            if not self.outputs:
                raise ValueError("router mode requires outputs")
        _check_pipeline(self.transforms)
        if self.forward is not None:
            _check_pipeline(self.forward.transforms)
        if self.reverse is not None:
            _check_pipeline(self.reverse.transforms)
            names = [item.name for item in self.outputs]
            if len(names) != len(set(names)):
                raise ValueError("duplicate output name")
        destinations = self.outputs if self.mode == "router" else []
        if self.output is not None:
            _check_pair(self.input, self.output)
        for item in destinations:
            _check_pair(self.input, item)
        output_names = {item.name for item in self.outputs}
        for rule in self.rules:
            if any(target not in output_names for target in rule.send_to):
                raise ValueError("invalid route reference")
        if self.default_output is not None and self.default_output not in output_names:
            raise ValueError("invalid route reference")
        if self.error_output is not None and self.error_output not in output_names:
            raise ValueError("invalid route reference")
        if self.transform_error == "route" and self.error_output is None:
            raise ValueError("route errors require error_output")
        upstream = self.input.protocol == "http" and self.http.response == "upstream"
        if upstream and self.mode == "gateway":
            output = self.output
            if output is None or output.protocol != "http":
                raise ValueError("upstream responses require an HTTP output")
        if upstream and self.mode == "router":
            only = len(self.outputs) == 1 and self.outputs[0].protocol == "http"
            if not only:
                raise ValueError("upstream responses require one HTTP output")
        framed = self.input.framing is not None and self.input.framing.type == "raw_stream"
        if self.mode == "router" and framed:
            raise ValueError("router mode requires message boundaries")
        tls = self.input.tls
        missing_cert = tls is not None and tls.enabled and (not tls.certfile or not tls.keyfile)
        if missing_cert:
            raise ValueError("TLS listeners require certfile and keyfile")
        return self


class ServerSpec(BaseModel):
    """Process-wide limits."""

    model_config = ConfigDict(extra="forbid")

    shutdown_timeout_seconds: float = Field(default=15, gt=0)
    max_connections: int = Field(default=1000, gt=0)


class LoggingSpec(BaseModel):
    """Operator logs. Message payloads are not included."""

    model_config = ConfigDict(extra="forbid")

    level: Literal["debug", "info", "warning", "error"] = "info"
    format: Literal["text", "json"] = "text"


class ManagementSpec(BaseModel):
    """Local control socket. It is not one of the bridged protocols."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = False
    transport: Literal["local", "tcp"] = "local"
    path: str | None = None
    host: str = "127.0.0.1"
    port: int = Field(default=0, ge=0, le=65535)
    token: str | None = None

    @model_validator(mode="after")
    def _check(self) -> ManagementSpec:
        if self.transport == "tcp" and not is_loopback(self.host):
            raise ValueError("management tcp must bind to a loopback address")
        if self.enabled and self.transport == "tcp" and not self.token:
            raise ValueError("management tcp requires a token")
        if self.token is not None and len(self.token) < 16:
            raise ValueError("management token is too short")
        return self


class DiagnosticsSpec(BaseModel):
    """Counters and traces. Payloads stay hidden unless tracing is explicit."""

    model_config = ConfigDict(extra="forbid")

    metrics: bool = True
    trace_payloads: bool = False
    trace_bytes: int = Field(default=16, ge=0, le=64)
    secret_fields: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self) -> DiagnosticsSpec:
        if len(self.secret_fields) > 32:
            raise ValueError("too many secret fields")
        for name in self.secret_fields:
            if not name or len(name) > 64 or any(char in name for char in '"\\'):
                raise ValueError("secret field name is invalid")
        return self


class IdentitySpec(BaseModel):
    """Name shown in status output."""

    model_config = ConfigDict(extra="forbid")

    name: str = "switchboard"


class SwitchboardConfig(BaseModel):
    """The resolved configuration for one process."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1]
    switchboard: IdentitySpec = Field(default_factory=IdentitySpec)
    server: ServerSpec = Field(default_factory=ServerSpec)
    profiles: dict[str, ProfileSpec] = Field(default_factory=dict)
    bridges: list[BridgeSpec] = Field(default_factory=list)
    routes: list[RouteSpec] = Field(default_factory=list)
    logging: LoggingSpec = Field(default_factory=LoggingSpec)
    management: ManagementSpec = Field(default_factory=ManagementSpec)
    diagnostics: DiagnosticsSpec = Field(default_factory=DiagnosticsSpec)

    @model_validator(mode="before")
    @classmethod
    def _bind(cls, data: object) -> object:
        if isinstance(data, dict):
            return _bind_profiles(dict(data))
        return data

    @model_validator(mode="after")
    def _check(self) -> SwitchboardConfig:
        _profile_names(self.profiles)
        names: set[str] = set()
        listeners: dict[tuple[str, str, int], str] = {}
        for bridge in self.bridges:
            if bridge.name in names:
                raise ValueError(f"duplicate bridge name {bridge.name}")
            names.add(bridge.name)
            port = bridge.input.port
            if not bridge.enabled or port is None or port == 0:
                continue
            family = "udp" if bridge.input.protocol == "udp" else "tcp"
            key = (family, bridge.input.host.lower(), port)
            other = listeners.get(key)
            if other is not None:
                listener = f"{family} {bridge.input.host}:{bridge.input.port}"
                raise ValueError(f"listener {listener} is already used by {other}")
            listeners[key] = bridge.name
        known = {bridge.name: bridge for bridge in self.bridges}
        seen_routes: set[str] = set()
        for route in self.routes:
            if route.name in seen_routes:
                raise ValueError(f"duplicate route name {route.name}")
            seen_routes.add(route.name)
            target = known.get(route.from_bridge)
            if target is None or target.mode != "router":
                raise ValueError("invalid route reference")
            names = {item.name for item in target.outputs}
            for rule in route.rules:
                if any(item not in names for item in rule.send_to):
                    raise ValueError("invalid route reference")
        _check_cycles(self.bridges)
        return self

    def enabled_bridges(self) -> list[BridgeSpec]:
        """Bridges that serve should open."""
        return [bridge for bridge in self.bridges if bridge.enabled]


def user_config_path() -> Path:
    """The per-user configuration file."""
    return Path(platformdirs.user_config_dir("switchboard", "Lupaxa")) / "switchboard.yml"


def default_config_path() -> Path:
    """The file serve reads when --config is omitted."""
    local = Path("switchboard.yml")
    if local.is_file():
        return local.resolve()
    return user_config_path()


def load_profiles(path: Path) -> dict[str, ProfileSpec]:
    """Load profiles from a configuration file or a bare profile mapping."""
    if not path.is_file():
        raise ConfigError(f"Configuration file not found: {path}")
    data = _read_yaml(path)
    if "profiles" in data or "version" in data or "bridges" in data:
        return load_config(path, user_path=None).profiles
    try:
        config = SwitchboardConfig.model_validate({"version": 1, "profiles": data})
    except ValidationError as exc:
        raise ConfigError(_format_validation(exc)) from exc
    return config.profiles


def load_config(
    path: Path | None = None,
    *,
    user_path: Path | None | object = _MISSING,
    log_level: str | None = None,
) -> SwitchboardConfig:
    """Load defaults, then the user file, then an explicit file."""
    if user_path is _MISSING:
        resolved_user: Path | None = user_config_path()
    else:
        resolved_user = user_path if isinstance(user_path, Path) else None
    data: dict[str, object] = {}
    if resolved_user is not None and resolved_user.is_file():
        data = _merge(data, _read_yaml(resolved_user))
    if path is not None:
        if not path.is_file():
            raise ConfigError(f"Configuration file not found: {path}")
        data = _merge(data, _read_yaml(path))
    elif resolved_user is None or not resolved_user.is_file():
        local = Path("switchboard.yml")
        if local.is_file():
            data = _merge(data, _read_yaml(local))
    if "version" not in data:
        data["version"] = 1
    if log_level is not None:
        logging_value = data.get("logging")
        logging_data = dict(logging_value) if isinstance(logging_value, dict) else {}
        logging_data["level"] = log_level
        data["logging"] = logging_data
    try:
        return SwitchboardConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(_format_validation(exc)) from exc


def is_loopback(host: str) -> bool:
    """True for localhost and literal loopback addresses."""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def endpoint_label(endpoint: EndpointSpec) -> str:
    """A short description of an endpoint."""
    if endpoint.url:
        return endpoint.url
    if endpoint.protocol == "websocket":
        scheme = "wss" if _tls_on(endpoint) else "ws"
        return f"{scheme}://{endpoint.host}:{endpoint.port}{endpoint.path}"
    if endpoint.protocol == "http":
        scheme = "https" if _tls_on(endpoint) else "http"
        return f"{scheme}://{endpoint.host}:{endpoint.port}{endpoint.path}"
    return f"{endpoint.protocol}://{endpoint.host}:{endpoint.port}"


def _scheme_port(scheme: str) -> int:
    if scheme in {"wss", "https"}:
        return 443
    return 80


def _check_pattern(pattern: str) -> None:
    if len(pattern) > 128 or pattern.count("+") + pattern.count("*") > 6:
        raise ValueError("pattern is too complex")
    if re.search(r"\([^)]*[+*][^)]*\)[+*{]", pattern):
        raise ValueError("pattern is too complex")
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError("pattern is invalid") from exc


def _check_pair(incoming: EndpointSpec, outgoing: EndpointSpec) -> None:
    in_raw = incoming.framing is not None and incoming.framing.type == "raw_stream"
    out_raw = outgoing.framing is not None and outgoing.framing.type == "raw_stream"
    if in_raw != out_raw:
        raise ValueError("raw_stream must be used on both TCP endpoints")
    same = (
        incoming.protocol == outgoing.protocol
        and incoming.host.lower() == outgoing.host.lower()
        and incoming.port == outgoing.port
        and incoming.port not in {None, 0}
        and incoming.path == outgoing.path
    )
    if same:
        raise ValueError("input and output must not be the same listener")


def _endpoint_key(endpoint: EndpointSpec) -> tuple[str, str, int, str] | None:
    port = endpoint.port
    if port is None or port == 0:
        return None
    return (endpoint.protocol, endpoint.host.lower(), port, endpoint.path)


def _check_cycles(bridges: list[BridgeSpec]) -> None:
    listeners: dict[tuple[str, str, int, str], str] = {}
    for bridge in bridges:
        if not bridge.enabled:
            continue
        key = _endpoint_key(bridge.input)
        if key is not None:
            listeners[key] = bridge.name
    edges: dict[str, set[str]] = {bridge.name: set() for bridge in bridges}
    for bridge in bridges:
        if not bridge.enabled:
            continue
        outputs: list[EndpointSpec] = list(bridge.outputs)
        if bridge.output is not None:
            outputs.append(bridge.output)
        for endpoint in outputs:
            key = _endpoint_key(endpoint)
            if key is None:
                continue
            target = listeners.get(key)
            if target is not None:
                edges[bridge.name].add(target)
    visiting: set[str] = set()
    visited: set[str] = set()

    def walk(name: str) -> None:
        if name in visiting:
            raise ValueError("routing cycle")
        if name in visited:
            return
        visiting.add(name)
        for nxt in edges.get(name, set()):
            walk(nxt)
        visiting.remove(name)
        visited.add(name)

    for name in edges:
        walk(name)


def _tls_on(endpoint: EndpointSpec) -> bool:
    return endpoint.tls is not None and endpoint.tls.enabled


def _apply_url(endpoint: EndpointSpec) -> None:
    if endpoint.url is None:
        return
    parts = urlsplit(endpoint.url)
    scheme = parts.scheme.lower()
    expected = {"websocket": {"ws", "wss"}, "http": {"http", "https"}}
    if scheme not in expected.get(endpoint.protocol, set()):
        raise ValueError("url scheme does not match the protocol")
    host = parts.hostname or ""
    if host == "":
        raise ValueError("url must include a host")
    port = parts.port if parts.port is not None else _scheme_port(scheme)
    path = parts.path or "/"
    if endpoint.host not in {"127.0.0.1", host}:
        raise ValueError("host does not match url")
    if endpoint.port not in {None, port}:
        raise ValueError("port does not match url")
    if path != "/" and endpoint.path not in {"/", path}:
        raise ValueError("path does not match url")
    endpoint.host = host
    endpoint.port = port
    endpoint.path = path
    secure = scheme in {"wss", "https"}
    if endpoint.tls is not None and endpoint.tls.enabled != secure:
        raise ValueError("tls does not match the url scheme")
    if secure and endpoint.tls is None:
        endpoint.tls = TlsSpec(enabled=True)


def field_size(field: FieldSpec) -> int | None:
    """Fixed width in bytes. None means the field consumes the rest of the frame."""
    if field.type in _INTEGER_WIDTH:
        return _INTEGER_WIDTH[field.type]
    if field.type == "bool":
        return field.length or 1
    if field.type in {"bytes", "string"}:
        return field.length
    if field.type == "array":
        item_length = field.length if field.item in {"bytes", "string"} else None
        item = FieldSpec(name=field.name, type=field.item or "uint8", length=item_length)
        width = field_size(item)
        if width is None or field.count is None:
            return None
        return width * field.count
    return None


def _known_encoding(name: str) -> None:
    try:
        "\n".encode(name)
    except LookupError as exc:
        raise ValueError(f"unknown encoding {name}") from exc


def _profile_names(profiles: dict[str, ProfileSpec]) -> None:
    pattern = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
    for name in profiles:
        if not pattern.match(name) or not 1 <= len(name) <= 128:
            raise ValueError(f"invalid profile name {name}")


def _prepare_profile(profile: dict[str, object]) -> dict[str, object]:
    encoding = profile.get("encoding", "utf-8")
    framing = profile.get("framing")
    limits = profile.get("limits")
    if isinstance(framing, dict):
        framing = dict(framing)
        if isinstance(encoding, str):
            if "encoding" in framing and framing["encoding"] != encoding:
                raise ValueError("profile encoding does not match framing")
            framing.setdefault("encoding", encoding)
        if isinstance(limits, dict) and "max_frame_bytes" in limits:
            limit = limits["max_frame_bytes"]
            current = framing.get("max_frame_bytes")
            if current not in {None, limit}:
                raise ValueError("profile limit does not match framing")
            framing["max_frame_bytes"] = limit
        profile["framing"] = framing
    structure = profile.get("structure")
    if isinstance(structure, dict) and "encoding" not in structure and isinstance(encoding, str):
        profile["structure"] = {**structure, "encoding": encoding}
    return profile


def _bind_profiles(data: dict[str, object]) -> dict[str, object]:
    profiles = data.get("profiles")
    bridges = data.get("bridges")
    if not isinstance(profiles, dict) or not isinstance(bridges, list):
        return data
    for bridge in bridges:
        if not isinstance(bridge, dict):
            continue
        for key in ("input", "output"):
            endpoint = bridge.get(key)
            if isinstance(endpoint, dict):
                _bind_endpoint(endpoint, profiles)
        outputs = bridge.get("outputs")
        if isinstance(outputs, list):
            for endpoint in outputs:
                if isinstance(endpoint, dict):
                    _bind_endpoint(endpoint, profiles)
    return data


def _bind_endpoint(endpoint: dict[str, object], profiles: dict[object, object]) -> None:
    name = endpoint.get("profile")
    if name is None:
        return
    profile = profiles.get(name)
    if not isinstance(name, str) or not isinstance(profile, dict):
        raise ValueError(f"missing profile {name}")
    prepared = _prepare_profile(dict(profile))
    transport = prepared.get("transport")
    if not isinstance(transport, str):
        raise ValueError(f"missing profile {name}")
    current = endpoint.get("protocol")
    if current not in {None, transport}:
        raise ValueError("profile transport does not match the protocol")
    endpoint["protocol"] = transport
    if endpoint.get("framing") is not None:
        raise ValueError("profile already defines framing")
    framing = prepared.get("framing")
    if framing is not None:
        endpoint["framing"] = framing
    structure = prepared.get("structure")
    if structure is not None:
        if endpoint.get("structure") is not None:
            raise ValueError("profile already defines a structure")
        endpoint["structure"] = structure


class _UniqueLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _unique_map(loader: yaml.SafeLoader, node: yaml.MappingNode) -> dict[object, object]:
    mapping: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=False)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                None,
                None,
                f"found duplicate key {key}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=False)
    return mapping


_UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _unique_map)


def _read_yaml(path: Path) -> dict[str, object]:
    try:
        loaded = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueLoader)
    except OSError as exc:
        raise ConfigError(f"Could not read {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in {path}: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ConfigError(f"Configuration must be a mapping: {path}")
    return {str(key): value for key, value in loaded.items()}


def _merge(base: dict[str, object], overlay: dict[str, object]) -> dict[str, object]:
    merged = dict(base)
    for key, value in overlay.items():
        current = merged.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            merged[key] = _merge(current, value)
        else:
            merged[key] = value
    return merged


def _format_validation(exc: ValidationError) -> str:
    lines: list[str] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        message = str(error["msg"])
        lines.append(f"{location}: {message}" if location else message)
    return "\n".join(lines)
