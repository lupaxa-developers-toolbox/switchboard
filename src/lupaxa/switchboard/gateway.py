"""TCP, UDP, WebSocket, and HTTP gateway.

Each bridge has its own listeners, sessions, and failure state. One bridge
stopping does not stop the others. UDP keeps a peer association for replies
and does not add delivery, ordering, or duplicate suppression. HTTP answers
with the configured request-response policy.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import socket
import ssl
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TypeVar, cast

import aiohttp
from aiohttp import WSMsgType, web

from lupaxa.switchboard.config import (
    BridgeSpec,
    EndpointSpec,
    FramingSpec,
    SwitchboardConfig,
    endpoint_label,
    is_loopback,
)
from lupaxa.switchboard.exceptions import FrameError, SwitchboardError
from lupaxa.switchboard.framing import FrameParser, frames_in_datagram
from lupaxa.switchboard.manage import Endpoint, Server, management_endpoint
from lupaxa.switchboard.message import Message, Source
from lupaxa.switchboard.profile import view_payload, wire_payload
from lupaxa.switchboard.route import select_outputs
from lupaxa.switchboard.router import DeliveryError, DeliveryResult, Lane
from lupaxa.switchboard.tls import client_context, server_context
from lupaxa.switchboard.transform import Document, TransformError, redact_fields, run_pipeline

logger = logging.getLogger("lupaxa.switchboard")

_READ_SIZE = 65536
_WS_DONE = {WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED, WSMsgType.ERROR}
_T = TypeVar("_T")
_HOP_HEADERS = {
    "connection",
    "content-length",
    "host",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}
_SENSITIVE_HEADERS = {"authorization", "cookie", "proxy-authorization", "set-cookie"}
_RUNNER_TAKES_SHUTDOWN = "shutdown_timeout" in inspect.signature(web.BaseRunner.__init__).parameters


def _as_datagram(transport: asyncio.BaseTransport) -> asyncio.DatagramTransport | None:
    """Return a datagram transport, including Python 3.11's selector socket.

    Python 3.11's ``_SelectorDatagramTransport`` implements ``sendto`` but
    does not subclass ``asyncio.DatagramTransport``. 3.12 and later do.
    """
    if isinstance(transport, asyncio.DatagramTransport):
        return transport
    if callable(getattr(transport, "sendto", None)):
        return cast("asyncio.DatagramTransport", transport)
    return None


class _Idle(Exception):
    """The session had no traffic until the idle timeout."""


@dataclass
class Session:
    """One inbound peer and the outbound connection created for it."""

    id: str
    bridge: str
    state: str
    inbound: str
    outbound: str
    opened_at: float
    last_activity: float
    bytes_in: int = 0
    bytes_out: int = 0
    messages: int = 0
    error: str | None = None


@dataclass
class BridgeRuntime:
    """Live state for one configured bridge."""

    spec: BridgeSpec
    input_host: str
    input_port: int
    listening: bool = False
    error: str | None = None
    bytes_in: int = 0
    bytes_out: int = 0
    messages: int = 0
    refused: int = 0
    dropped: int = 0
    failures: int = 0
    unmatched: int = 0
    active: int = 0
    server: asyncio.AbstractServer | None = None
    udp: asyncio.DatagramTransport | None = None
    runner: web.AppRunner | None = None
    client: aiohttp.ClientSession | None = None
    sessions: dict[str, Session] = field(default_factory=dict)
    writers: set[asyncio.StreamWriter] = field(default_factory=set)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)
    started_at: float = field(default_factory=time.monotonic)
    transform_failures: int = 0
    forward_ms: float = 0

    def label(self) -> str:
        """The input address, using the bound port when the socket is open."""
        endpoint = self.spec.input
        if endpoint.protocol == "websocket":
            scheme = "wss" if endpoint.tls is not None and endpoint.tls.enabled else "ws"
            return f"{scheme}://{self.input_host}:{self.input_port}{endpoint.path}"
        if endpoint.protocol == "http":
            scheme = "https" if endpoint.tls is not None and endpoint.tls.enabled else "http"
            return f"{scheme}://{self.input_host}:{self.input_port}{endpoint.path}"
        return f"{endpoint.protocol}://{self.input_host}:{self.input_port}"


class _QueueProtocol(asyncio.DatagramProtocol):
    """A bounded queue of datagrams. Overflow is counted and discarded."""

    def __init__(self, limit: int, on_drop: Callable[[int], None]) -> None:
        self.queue: asyncio.Queue[tuple[bytes, tuple[str, int]]] = asyncio.Queue(
            maxsize=max(1, limit)
        )
        self.transport: asyncio.DatagramTransport | None = None
        self._on_drop = on_drop

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        found = _as_datagram(transport)
        if found is not None:
            self.transport = found

    def datagram_received(self, data: bytes, addr: tuple[str | int, ...] | None) -> None:
        peer = _peer_addr(addr)
        try:
            self.queue.put_nowait((data, peer))
        except asyncio.QueueFull:
            self._on_drop(len(data))

    def error_received(self, exc: Exception) -> None:
        logger.debug("udp socket error: %s", exc.__class__.__name__)


class Gateway:
    """Run every enabled bridge on one event loop."""

    def __init__(self, config: SwitchboardConfig, *, source_path: Path | None = None) -> None:
        self.config = config
        self.source_path = source_path
        self.bridges: dict[str, BridgeRuntime] = {}
        self.management: Endpoint | None = None
        self._control: Server | None = None
        self._events: deque[dict[str, object]] = deque(maxlen=200)
        self._seq = 0
        self._tasks: set[asyncio.Task[None]] = set()
        self._writers: set[asyncio.StreamWriter] = set()
        self._transports: set[asyncio.BaseTransport] = set()
        self._connections = 0
        self._stopping = False
        self._stopped = False
        self._unverified: set[str] = set()
        self._lanes: dict[str, dict[str, Lane]] = {}

    def runtime(self, name: str) -> BridgeRuntime:
        """Return the live bridge, or raise if it was not started."""
        try:
            return self.bridges[name]
        except KeyError as exc:
            raise SwitchboardError(f"Bridge is not running: {name}") from exc

    async def start(self) -> None:
        """Open listeners. A bridge that cannot bind is recorded and skipped."""
        started = 0
        for spec in self.config.enabled_bridges():
            runtime = BridgeRuntime(
                spec=spec,
                input_host=spec.input.host,
                input_port=_port(spec.input),
            )
            self.bridges[spec.name] = runtime
            if not is_loopback(spec.input.host):
                logger.warning(
                    "bridge %s listens on %s, which is not a loopback address",
                    spec.name,
                    spec.input.host,
                )
            try:
                await self._open(runtime)
            except Exception as exc:
                runtime.listening = False
                runtime.error = exc.__class__.__name__
                runtime.failures += 1
                logger.error("bridge %s failed to start: %s", spec.name, exc.__class__.__name__)
                continue
            runtime.listening = True
            runtime.started_at = time.monotonic()
            if spec.mode == "router":
                self._open_lanes(runtime)
            started += 1
        if started == 0:
            raise SwitchboardError("No bridges started.")
        await self._open_management()

    async def stop(self) -> None:
        """Stop listeners and close sessions. Safe to call more than once."""
        if self._stopped:
            return
        self._stopped = True
        self._stopping = True
        if self._control is not None:
            await self._control.close()
            self._control = None
        for runtime in self.bridges.values():
            if runtime.server is not None:
                runtime.server.close()
                await runtime.server.wait_closed()
            if runtime.udp is not None:
                runtime.udp.close()
            if runtime.runner is not None:
                await runtime.runner.cleanup()
        for writer in list(self._writers):
            _close_writer(writer)
        for transport in list(self._transports):
            transport.close()
        if self._tasks:
            _done, pending = await asyncio.wait(
                self._tasks,
                timeout=self.config.server.shutdown_timeout_seconds,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        for runtime in self.bridges.values():
            if runtime.client is not None and not runtime.client.closed:
                await runtime.client.close()

    async def control(self, request: dict[str, object]) -> object:
        """Run one management method. The caller is the local control socket."""
        method = request.get("method")
        params = request.get("params")
        if not isinstance(method, str) or not isinstance(params, dict):
            raise SwitchboardError("management request is invalid")
        if method == "health":
            return {"status": "ok", "bridges": len(self.bridges)}
        if method == "status":
            return self._status()
        if method == "bridge.show":
            return self._bridge_status(self._named(params))
        if method == "connections.list":
            return {"connections": self._connections_for(params.get("bridge"))}
        if method == "connections.show":
            return self._one_connection(params.get("id"))
        if method == "bridge.start":
            await self.start_bridge(_bridge_name(params))
            return {"status": "started"}
        if method == "bridge.stop":
            await self.stop_bridge(_bridge_name(params))
            return {"status": "stopped"}
        if method == "bridge.restart":
            await self.restart_bridge(_bridge_name(params))
            return {"status": "restarted"}
        if method == "reload":
            return await self.reload()
        if method == "metrics":
            return self._metrics()
        if method == "events":
            since = params.get("since", 0)
            bridge = params.get("bridge")
            return {
                "events": self._recent(
                    int(since) if isinstance(since, int) else 0,
                    bridge if isinstance(bridge, str) else None,
                )
            }
        if method == "profiles":
            return {
                "profiles": [
                    {"name": name, "transport": profile.transport}
                    for name, profile in self.config.profiles.items()
                ]
            }
        if method == "config.validate":
            return self._validate_source()
        raise SwitchboardError("Unknown management method.")

    async def start_bridge(self, name: str, spec: BridgeSpec | None = None) -> None:
        """Open one configured bridge without touching the others."""
        spec = spec or self._configured(name)
        runtime = self.bridges.get(name)
        if runtime is not None and runtime.listening:
            raise SwitchboardError("Bridge is already running.")
        if runtime is None:
            runtime = BridgeRuntime(
                spec=spec,
                input_host=spec.input.host,
                input_port=_port(spec.input),
            )
            self.bridges[name] = runtime
        runtime.spec = spec
        try:
            await self._open(runtime)
        except Exception as exc:
            runtime.listening = False
            runtime.error = exc.__class__.__name__
            raise SwitchboardError("Bridge failed to start.") from exc
        runtime.listening = True
        runtime.error = None
        runtime.started_at = time.monotonic()
        if spec.mode == "router":
            self._open_lanes(runtime)

    async def stop_bridge(self, name: str) -> None:
        """Close one bridge and its sessions. Other bridges keep running."""
        runtime = self.bridges.get(name)
        if runtime is None or not runtime.listening:
            raise SwitchboardError("Bridge is not running.")
        await self._close_bridge(runtime)

    async def restart_bridge(self, name: str) -> None:
        """Stop one bridge and open it again from the current configuration."""
        if name in self.bridges and self.bridges[name].listening:
            await self.stop_bridge(name)
        await self.start_bridge(name)

    async def reload(self) -> dict[str, object]:
        """Validate the source file, then replace only the bridges that changed."""
        from lupaxa.switchboard.config import load_config

        if self.source_path is None:
            raise SwitchboardError("Reload needs the configuration file used at startup.")
        proposed = load_config(self.source_path, user_path=None)
        return await self.apply(proposed)

    async def apply(self, proposed: SwitchboardConfig) -> dict[str, object]:
        """Apply a validated configuration. Unchanged bridges are left in place."""
        changes: list[dict[str, str]] = []
        failures: list[dict[str, str]] = []
        wanted = {bridge.name: bridge for bridge in proposed.enabled_bridges()}
        current = {name: runtime.spec for name, runtime in self.bridges.items()}
        for name, spec in wanted.items():
            previous = current.get(name)
            same = previous is not None and previous.model_dump(mode="json") == spec.model_dump(
                mode="json"
            )
            if same:
                changes.append({"bridge": name, "action": "keep"})
                continue
            if previous is not None and name in self.bridges:
                await self._close_bridge(self.bridges[name])
            try:
                await self.start_bridge(name, spec)
                changes.append({"bridge": name, "action": "replace" if previous else "start"})
            except SwitchboardError:
                failures.append({"bridge": name, "error": "start failed"})
                if previous is not None:
                    restored = await self._restore(name, previous)
                    changes.append({"bridge": name, "action": "restored" if restored else "failed"})
        for name in current:
            if name not in wanted and name in self.bridges:
                await self._close_bridge(self.bridges[name])
                self.bridges.pop(name, None)
                changes.append({"bridge": name, "action": "stop"})
        running = {name: runtime.spec for name, runtime in self.bridges.items()}
        bridges = [running.get(bridge.name, bridge) for bridge in proposed.bridges]
        self.config = proposed.model_copy(update={"bridges": bridges})
        return {
            "ok": not failures,
            "changes": changes,
            "failures": failures,
            "note": "Replacing a listener is not atomic.",
        }

    async def _restore(self, name: str, spec: BridgeSpec) -> bool:
        runtime = self.bridges.get(name)
        if runtime is None:
            runtime = BridgeRuntime(
                spec=spec,
                input_host=spec.input.host,
                input_port=_port(spec.input),
            )
            self.bridges[name] = runtime
        runtime.spec = spec
        try:
            await self._open(runtime)
        except Exception:
            runtime.listening = False
            runtime.error = "restore failed"
            return False
        runtime.listening = True
        runtime.error = None
        if spec.mode == "router":
            self._open_lanes(runtime)
        return True

    async def _close_bridge(self, runtime: BridgeRuntime) -> None:
        if runtime.server is not None:
            runtime.server.close()
            await runtime.server.wait_closed()
            runtime.server = None
        if runtime.udp is not None:
            runtime.udp.close()
            runtime.udp = None
        if runtime.runner is not None:
            await runtime.runner.cleanup()
            runtime.runner = None
        for writer in list(runtime.writers):
            _close_writer(writer)
        for task in list(runtime.tasks):
            task.cancel()
        if runtime.tasks:
            await asyncio.gather(*runtime.tasks, return_exceptions=True)
        runtime.tasks.clear()
        runtime.listening = False
        runtime.sessions.clear()
        self._lanes.pop(runtime.spec.name, None)

    async def _open_management(self) -> None:
        endpoint = management_endpoint(self.config)
        if endpoint is None:
            return
        server = Server(endpoint, self.control)
        await server.start()
        self._control = server
        self.management = server.endpoint

    def _status(self) -> dict[str, object]:
        return {
            "source": "management",
            "name": self.config.switchboard.name,
            "bridges": [self._bridge_status(runtime) for runtime in self.bridges.values()],
        }

    def _bridge_status(self, runtime: BridgeRuntime) -> dict[str, object]:
        lanes = self._lanes.get(runtime.spec.name, {})
        uptime = time.monotonic() - runtime.started_at if runtime.listening else 0
        return {
            "name": runtime.spec.name,
            "mode": runtime.spec.mode,
            "enabled": runtime.spec.enabled,
            "listening": runtime.listening,
            "input": runtime.label(),
            "output": _output_label(runtime.spec),
            "active": runtime.active,
            "messages": runtime.messages,
            "bytes_in": runtime.bytes_in,
            "bytes_out": runtime.bytes_out,
            "transform_failures": runtime.transform_failures,
            "failures": runtime.failures,
            "dropped": runtime.dropped,
            "unmatched": runtime.unmatched,
            "last_error": runtime.error,
            "uptime_seconds": round(uptime, 3),
            "queued": sum(lane.queued for lane in lanes.values()),
        }

    def _metrics(self) -> dict[str, object]:
        lanes = [lane for group in self._lanes.values() for lane in group.values()]
        bridges = list(self.bridges.values())
        return {
            "active": sum(runtime.active for runtime in bridges),
            "messages": sum(runtime.messages for runtime in bridges),
            "bytes_in": sum(runtime.bytes_in for runtime in bridges),
            "bytes_out": sum(runtime.bytes_out for runtime in bridges),
            "dropped": sum(runtime.dropped for runtime in bridges),
            "failures": sum(runtime.failures for runtime in bridges),
            "transform_failures": sum(runtime.transform_failures for runtime in bridges),
            "retries": sum(lane.retries for lane in lanes),
            "queued": sum(lane.queued for lane in lanes),
            "forward_ms": max((runtime.forward_ms for runtime in bridges), default=0),
        }

    def _connections_for(self, bridge: object) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for runtime in self.bridges.values():
            if isinstance(bridge, str) and runtime.spec.name != bridge:
                continue
            rows.extend(_connection_row(session) for session in runtime.sessions.values())
        return rows

    def _one_connection(self, identity: object) -> dict[str, object]:
        if not isinstance(identity, str):
            raise SwitchboardError("Connection id is required.")
        for runtime in self.bridges.values():
            session = runtime.sessions.get(identity)
            if session is not None:
                return _connection_row(session)
        raise SwitchboardError("Connection is not active.")

    def _recent(self, since: int, bridge: str | None) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for event in self._events:
            raw_seq = event.get("seq", 0)
            sequence = raw_seq if isinstance(raw_seq, int) else 0
            if sequence > since and (bridge is None or event.get("bridge") == bridge):
                rows.append(dict(event))
        return rows

    def _validate_source(self) -> dict[str, object]:
        from lupaxa.switchboard.config import load_config

        if self.source_path is None:
            raise SwitchboardError("No configuration file is loaded.")
        load_config(self.source_path, user_path=None)
        return {"ok": True, "path": str(self.source_path)}

    def _named(self, params: dict[str, object]) -> BridgeRuntime:
        name = params.get("name")
        if not isinstance(name, str):
            raise SwitchboardError("Bridge name is required.")
        runtime = self.bridges.get(name)
        if runtime is None:
            raise SwitchboardError(f"Bridge is not running: {name}")
        return runtime

    def _configured(self, name: str) -> BridgeSpec:
        for bridge in self.config.bridges:
            if bridge.name == name:
                return bridge
        raise SwitchboardError(f"Bridge is not configured: {name}")

    def _note(
        self,
        runtime: BridgeRuntime,
        stage: str,
        *,
        outcome: str = "ok",
        session: str | None = None,
        size: int = 0,
        detail: str = "",
        payload: bytes | None = None,
        duration_ms: float | None = None,
    ) -> None:
        self._seq += 1
        preview = None
        if payload is not None and self.config.diagnostics.trace_payloads:
            shown = redact_fields(payload, self.config.diagnostics.secret_fields)
            preview = shown[: self.config.diagnostics.trace_bytes].hex()
        if duration_ms is not None:
            runtime.forward_ms = duration_ms
        self._events.append(
            {
                "seq": self._seq,
                "time": datetime.now(UTC).isoformat(timespec="milliseconds"),
                "bridge": runtime.spec.name,
                "session": session,
                "stage": stage,
                "outcome": outcome,
                "size": size,
                "detail": detail,
                "duration_ms": duration_ms,
                "preview": preview,
            }
        )

    async def _open(self, runtime: BridgeRuntime) -> None:
        spec = runtime.spec
        if spec.input.protocol in {"websocket", "http"}:
            await self._open_site(runtime)
            return
        if spec.input.protocol == "tcp":
            tls = spec.input.tls
            ssl_ctx = server_context(tls) if tls is not None and tls.enabled else None
            server = await asyncio.start_server(
                lambda reader, writer: self._spawn(self._on_tcp(runtime, reader, writer)),
                spec.input.host,
                _port(spec.input),
                reuse_address=False,
                ssl=ssl_ctx,
            )
            runtime.server = server
            host, port = _sockname(server.sockets[0].getsockname() if server.sockets else None)
            runtime.input_host = host
            runtime.input_port = port
            return
        protocol = _QueueProtocol(spec.limits.max_connections, lambda _size: self._drop(runtime))
        transport, _protocol = await asyncio.get_running_loop().create_datagram_endpoint(
            lambda: protocol,
            local_addr=(spec.input.host, _port(spec.input)),
        )
        runtime.udp = transport
        self._transports.add(transport)
        host, port = _sockname(transport.get_extra_info("sockname"))
        runtime.input_host = host
        runtime.input_port = port
        self._spawn(self._udp_ingress(runtime, protocol), runtime)

    async def _on_tcp(
        self,
        runtime: BridgeRuntime,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        spec = runtime.spec
        _nodelay(writer)
        self._writers.add(writer)
        runtime.writers.add(writer)
        if self._stopping or not self._acquire(runtime):
            runtime.refused += 1
            _close_writer(writer)
            self._writers.discard(writer)
            runtime.writers.discard(writer)
            return
        peer = writer.get_extra_info("peername")
        host, port = _sockname(peer)
        session = Session(
            id=str(uuid.uuid4()),
            bridge=spec.name,
            state="connecting",
            inbound=f"tcp://{host}:{port}",
            outbound=_outbound_label(spec),
            opened_at=time.monotonic(),
            last_activity=time.monotonic(),
        )
        runtime.sessions[session.id] = session
        self._note(runtime, "connect", session=session.id, detail=session.inbound)
        try:
            if spec.mode == "router":
                await self._route_frames(runtime, session, reader)
                return
            output = _out(spec).protocol
            if output == "tcp":
                await self._tcp_to_tcp(runtime, session, reader, writer)
            elif output == "udp":
                await self._tcp_to_udp(runtime, session, reader, writer)
            elif output == "websocket":
                await self._tcp_to_ws(runtime, session, reader, writer)
            elif output == "http":
                await self._tcp_to_http(runtime, session, reader, writer)
            else:
                raise SwitchboardError("This protocol combination is not available.")
        except _Idle:
            session.error = "idle timeout"
            session.state = "closed"
        except FrameError as exc:
            session.error = exc.code
            session.state = "failed"
            runtime.failures += 1
            runtime.error = exc.code
            self._note(runtime, "error", outcome="error", session=session.id, detail=exc.code)
            logger.warning("bridge %s framing error %s", spec.name, exc.code)
        except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError):
            session.error = "connection failed"
            session.state = "failed"
            runtime.failures += 1
            runtime.error = "connection failed"
            logger.warning("bridge %s connection failed", spec.name)
        except Exception:
            session.error = "internal error"
            runtime.error = "internal error"
            session.state = "failed"
            runtime.failures += 1
            logger.exception("bridge %s session failed", spec.name)
        else:
            if session.state != "failed":
                session.state = "closed"
        finally:
            runtime.sessions.pop(session.id, None)
            _close_writer(writer)
            self._writers.discard(writer)
            runtime.writers.discard(writer)
            self._release(runtime)

    async def _tcp_to_tcp(
        self,
        runtime: BridgeRuntime,
        session: Session,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        out_reader, out_writer = await self._connect_tcp(runtime)
        session.state = "active"
        try:
            await self._bridge_streams(runtime, session, reader, writer, out_reader, out_writer)
        finally:
            _close_writer(out_writer)
            self._writers.discard(out_writer)

    async def _tcp_to_udp(
        self,
        runtime: BridgeRuntime,
        session: Session,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        spec = runtime.spec
        protocol = _QueueProtocol(4, lambda _size: self._drop(runtime))
        transport, _protocol = await asyncio.wait_for(
            asyncio.get_running_loop().create_datagram_endpoint(
                lambda: protocol,
                remote_addr=(_out(spec).host, _port(_out(spec))),
            ),
            spec.limits.connect_timeout_seconds,
        )
        self._transports.add(transport)
        session.state = "active"
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(self._tcp_out(runtime, session, reader, transport.sendto, stop)),
            asyncio.create_task(
                self._udp_in(runtime, session, protocol, writer, spec.input.framing, stop)
            ),
            asyncio.create_task(self._watch_idle(runtime, session, stop)),
        ]
        try:
            await self._finish_tasks(
                tasks,
                stop,
                session,
                half_close=spec.disconnect.on_peer_close == "half_close",
            )
        finally:
            transport.close()
            self._transports.discard(transport)

    async def _bridge_streams(
        self,
        runtime: BridgeRuntime,
        session: Session,
        left_reader: asyncio.StreamReader,
        left_writer: asyncio.StreamWriter,
        right_reader: asyncio.StreamReader,
        right_writer: asyncio.StreamWriter,
    ) -> None:
        spec = runtime.spec
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(
                self._pump(
                    runtime,
                    session,
                    left_reader,
                    right_writer,
                    spec.input.framing,
                    _out(spec).framing,
                    stop,
                    inbound=True,
                )
            ),
            asyncio.create_task(
                self._pump(
                    runtime,
                    session,
                    right_reader,
                    left_writer,
                    _out(spec).framing,
                    spec.input.framing,
                    stop,
                    inbound=False,
                )
            ),
            asyncio.create_task(self._watch_idle(runtime, session, stop)),
        ]
        await self._finish_tasks(
            tasks,
            stop,
            session,
            half_close=spec.disconnect.on_peer_close == "half_close",
        )

    async def _finish_tasks(
        self,
        tasks: list[asyncio.Task[None]],
        stop: asyncio.Event,
        session: Session,
        *,
        half_close: bool,
    ) -> None:
        data = tasks[:-1]
        idle = tasks[-1]
        error: BaseException | None = None
        try:
            while not all(task.done() for task in data):
                pending = [task for task in (*data, idle) if not task.done()]
                if not pending:
                    break
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
                if session.error == "idle timeout":
                    error = _Idle()
                    break
                for task in data:
                    if task.done() and not task.cancelled() and task.exception() is not None:
                        error = task.exception()
                        half_close = False
                        break
                if error is not None or not half_close:
                    break
            stop.set()
        finally:
            stop.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        if error is not None:
            raise error

    async def _pump(
        self,
        runtime: BridgeRuntime,
        session: Session,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        decode: FramingSpec | None,
        encode: FramingSpec | None,
        stop: asyncio.Event,
        *,
        inbound: bool,
    ) -> None:
        spec = runtime.spec
        parser = FrameParser(decode, on_error=spec.framing_error) if decode is not None else None
        encoder = FrameParser(encode, on_error=spec.framing_error) if encode is not None else None
        partial_since: float | None = None
        while not stop.is_set():
            timeout = _wait_budget(spec, session, parser, partial_since)
            _raise_if_due(spec, session, parser, partial_since)
            try:
                chunk = await _read(reader, timeout)
            except TimeoutError:
                _raise_if_due(spec, session, parser, partial_since, forced=True)
                continue
            if stop.is_set():
                return
            if not chunk:
                if parser is not None:
                    parser.finish()
                half = spec.disconnect.on_peer_close == "half_close"
                if half and writer.can_write_eof() and not writer.is_closing():
                    writer.write_eof()
                    return
                stop.set()
                return
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(chunk), inbound=inbound)
            payloads = parser.feed(chunk) if parser is not None else [chunk]
            if parser is not None and parser.pending:
                partial_since = partial_since or time.monotonic()
            else:
                partial_since = None
            source = spec.input if inbound else _out(spec)
            target = _out(spec) if inbound else spec.input
            for payload in payloads:
                self._note(runtime, "frame", session=session.id, size=len(payload))
                await self._write_payload(
                    runtime,
                    session,
                    writer,
                    encoder,
                    view_payload(source, payload),
                    target=target,
                    inbound=inbound,
                )

    async def _tcp_out(
        self,
        runtime: BridgeRuntime,
        session: Session,
        reader: asyncio.StreamReader,
        send: Callable[[bytes], None],
        stop: asyncio.Event,
    ) -> None:
        spec = runtime.spec
        framing = spec.input.framing
        parser = FrameParser(framing, on_error=spec.framing_error) if framing is not None else None
        partial_since: float | None = None
        while not stop.is_set():
            _raise_if_due(spec, session, parser, partial_since)
            timeout = _wait_budget(spec, session, parser, partial_since)
            try:
                chunk = await _read(reader, timeout)
            except TimeoutError:
                _raise_if_due(spec, session, parser, partial_since, forced=True)
                continue
            if not chunk:
                if parser is not None:
                    parser.finish()
                stop.set()
                return
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(chunk), inbound=True)
            payloads = parser.feed(chunk) if parser is not None else [chunk]
            partial_since = time.monotonic() if parser is not None and parser.pending else None
            for payload in payloads:
                viewed = view_payload(spec.input, payload)
                prepared = self._apply_direction(runtime, viewed, forward=True)
                if prepared is None:
                    continue
                payload = wire_payload(_out(spec), prepared)
                if len(payload) > spec.udp.max_datagram_bytes:
                    self._drop(runtime)
                    if spec.udp.overflow == "reject":
                        logger.warning("bridge %s rejected an oversized udp datagram", spec.name)
                    continue
                send(payload)
                session.messages += 1
                runtime.messages += 1

    async def _udp_in(
        self,
        runtime: BridgeRuntime,
        session: Session,
        protocol: _QueueProtocol,
        writer: asyncio.StreamWriter,
        encode: FramingSpec | None,
        stop: asyncio.Event,
    ) -> None:
        encoder = None
        if encode is not None:
            encoder = FrameParser(encode, on_error=runtime.spec.framing_error)
        while not stop.is_set():
            try:
                data, _peer = await asyncio.wait_for(protocol.queue.get(), 0.05)
            except TimeoutError:
                _raise_if_due(runtime.spec, session, None, None)
                continue
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(data), inbound=False)
            viewed = view_payload(_out(runtime.spec), data)
            await self._write_payload(
                runtime,
                session,
                writer,
                encoder,
                viewed,
                target=runtime.spec.input,
                inbound=False,
            )

    async def _watch_idle(
        self,
        runtime: BridgeRuntime,
        session: Session,
        stop: asyncio.Event,
    ) -> None:
        limit = runtime.spec.limits.idle_timeout_seconds
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), 0.05)
            except TimeoutError:
                if time.monotonic() - session.last_activity >= limit:
                    session.error = "idle timeout"
                    stop.set()
                    return

    async def _udp_ingress(self, runtime: BridgeRuntime, protocol: _QueueProtocol) -> None:
        spec = runtime.spec
        peers: dict[tuple[str, int], _Peer] = {}
        while not self._stopping:
            try:
                data, addr = await asyncio.wait_for(protocol.queue.get(), 0.05)
            except TimeoutError:
                self._expire_peers(runtime, peers)
                continue
            if len(data) > spec.udp.max_datagram_bytes:
                self._drop(runtime)
                if spec.udp.overflow == "reject":
                    logger.warning("bridge %s rejected an oversized udp datagram", spec.name)
                continue
            data = view_payload(spec.input, data)
            if spec.mode == "router":
                if not self._acquire(runtime):
                    runtime.refused += 1
                    continue
                try:
                    await self._route_bytes(runtime, data, {}, 0, wait=True)
                finally:
                    self._release(runtime)
                continue
            peer = peers.get(addr)
            if peer is None:
                if not self._acquire(runtime):
                    runtime.refused += 1
                    continue
                try:
                    peer = await self._open_peer(runtime, addr)
                except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError):
                    self._release(runtime)
                    runtime.failures += 1
                    logger.warning("bridge %s could not open an output for a udp peer", spec.name)
                    continue
                peers[addr] = peer
            peer.last_activity = time.monotonic()
            try:
                await peer.deliver(data)
            except (ConnectionError, OSError, FrameError):
                runtime.failures += 1
                await peer.shutdown()
                peers.pop(addr, None)
                self._release(runtime)
        for peer in peers.values():
            await peer.shutdown()
            self._release(runtime)

    async def _open_peer(self, runtime: BridgeRuntime, addr: tuple[str, int]) -> _Peer:
        spec = runtime.spec
        if _out(spec).protocol == "tcp":
            reader, writer = await self._connect_tcp(runtime)
            peer = _Peer(
                addr,
                send_fn=lambda data: self._tcp_send(runtime, writer, data),
                close_fn=lambda: self._drop_writer(writer),
            )
            self._spawn(self._udp_from_tcp(runtime, reader, addr, peer))
            return peer
        if _out(spec).protocol == "websocket":
            socket_out = await self._connect_ws(runtime)
            peer = _Peer(
                addr,
                send_fn=lambda data: self._forward_ws(runtime, socket_out, data),
                close_fn=socket_out.close,
            )
            self._spawn(self._ws_replies_to_udp(runtime, socket_out, addr, peer))
            return peer
        if _out(spec).protocol == "http":
            return _Peer(
                addr,
                send_fn=lambda data: self._udp_http(runtime, addr, data),
                close_fn=self._close_idle,
            )
        protocol = _QueueProtocol(4, lambda _size: self._drop(runtime))
        transport, _protocol = await asyncio.wait_for(
            asyncio.get_running_loop().create_datagram_endpoint(
                lambda: protocol,
                remote_addr=(_out(spec).host, _port(_out(spec))),
            ),
            spec.limits.connect_timeout_seconds,
        )
        self._transports.add(transport)
        self._spawn(self._udp_from_udp(runtime, protocol, addr))
        return _Peer(
            addr,
            send_fn=lambda data: self._forward_udp(runtime, transport, data),
            close_fn=lambda: self._drop_transport(transport),
        )

    async def _tcp_send(
        self,
        runtime: BridgeRuntime,
        writer: asyncio.StreamWriter,
        data: bytes,
    ) -> None:
        framing = _out(runtime.spec).framing
        if framing is None:
            payloads = [data]
            encoder = None
        else:
            payloads = frames_in_datagram(data, framing, on_error=runtime.spec.framing_error)
            encoder = FrameParser(framing, on_error=runtime.spec.framing_error)
        for payload in payloads:
            prepared = self._apply_direction(runtime, payload, forward=True)
            if prepared is None:
                continue
            body = wire_payload(_out(runtime.spec), prepared)
            wire = encoder.encode(body) if encoder is not None else body
            _remember(runtime, wire, inbound=False)
            writer.write(wire)
            await asyncio.wait_for(writer.drain(), runtime.spec.limits.write_timeout_seconds)

    async def _udp_from_tcp(
        self,
        runtime: BridgeRuntime,
        reader: asyncio.StreamReader,
        addr: tuple[str, int],
        peer: _Peer,
    ) -> None:
        framing = _out(runtime.spec).framing
        parser = None
        if framing is not None:
            parser = FrameParser(framing, on_error=runtime.spec.framing_error)
        transport = runtime.udp
        while not self._stopping and transport is not None:
            chunk = await reader.read(_READ_SIZE)
            if not chunk:
                return
            peer.last_activity = time.monotonic()
            payloads = parser.feed(chunk) if parser is not None else [chunk]
            for payload in payloads:
                source = _out(runtime.spec)
                viewed = view_payload(source, payload)
                prepared = self._apply_direction(runtime, viewed, forward=False)
                if prepared is None:
                    continue
                body = wire_payload(runtime.spec.input, prepared)
                transport.sendto(body, addr)
                _remember(runtime, payload, inbound=False)

    async def _udp_from_udp(
        self,
        runtime: BridgeRuntime,
        protocol: _QueueProtocol,
        addr: tuple[str, int],
    ) -> None:
        transport = runtime.udp
        while not self._stopping and transport is not None:
            try:
                data, _peer = await asyncio.wait_for(protocol.queue.get(), 0.05)
            except TimeoutError:
                continue
            source = _out(runtime.spec)
            viewed = view_payload(source, data)
            prepared = self._apply_direction(runtime, viewed, forward=False)
            if prepared is None:
                continue
            body = wire_payload(runtime.spec.input, prepared)
            transport.sendto(body, addr)
            _remember(runtime, data, inbound=False)

    def _expire_peers(self, runtime: BridgeRuntime, peers: dict[tuple[str, int], _Peer]) -> None:
        limit = runtime.spec.limits.idle_timeout_seconds
        now = time.monotonic()
        expired = [addr for addr, peer in peers.items() if now - peer.last_activity >= limit]
        for addr in expired:
            peer = peers.pop(addr)
            self._spawn(peer.shutdown())
            self._release(runtime)

    def _apply_direction(
        self,
        runtime: BridgeRuntime,
        payload: bytes,
        *,
        forward: bool,
    ) -> bytes | None:
        """Run the gateway pipeline for one direction. Router mode is unchanged."""
        spec = runtime.spec
        if spec.mode != "gateway":
            return payload
        block = spec.forward if forward else spec.reverse
        if block is None or not block.transforms:
            return payload
        try:
            transformed = run_pipeline(
                Document(data=payload),
                list(block.transforms),
                on_error=spec.transform_error,
            )
        except TransformError:
            runtime.failures += 1
            runtime.transform_failures += 1
            self._note(runtime, "transform", outcome="error", detail="transform failed")
            logger.warning("bridge %s transform failed", spec.name)
            if spec.transform_error == "close":
                raise FrameError("The transform failed.", code="TRANSFORM") from None
            return None
        if transformed is None:
            runtime.dropped += 1
            return None
        return transformed.data

    async def _forward_ws(
        self,
        runtime: BridgeRuntime,
        socket_out: aiohttp.ClientWebSocketResponse,
        payload: bytes,
    ) -> None:
        prepared = self._apply_direction(runtime, payload, forward=True)
        if prepared is None:
            return
        body = wire_payload(_out(runtime.spec), prepared)
        if len(body) > runtime.spec.websocket.max_message_bytes:
            raise FrameError("The frame exceeds the size limit.", code="FRAME_TOO_LARGE")
        await socket_out.send_bytes(body)

    async def _forward_udp(
        self,
        runtime: BridgeRuntime,
        transport: asyncio.DatagramTransport,
        payload: bytes,
    ) -> None:
        prepared = self._apply_direction(runtime, payload, forward=True)
        if prepared is None:
            return
        body = wire_payload(_out(runtime.spec), prepared)
        if len(body) > runtime.spec.udp.max_datagram_bytes:
            self._drop(runtime)
            if runtime.spec.udp.overflow == "reject":
                logger.warning("bridge %s rejected an oversized udp datagram", runtime.spec.name)
            return
        transport.sendto(body)

    async def _write_payload(
        self,
        runtime: BridgeRuntime,
        session: Session,
        writer: asyncio.StreamWriter,
        encoder: FrameParser | None,
        payload: bytes,
        *,
        target: EndpointSpec,
        inbound: bool,
    ) -> None:
        source = Source(
            protocol=runtime.spec.input.protocol,
            address=runtime.input_host,
            port=runtime.input_port,
        )
        forward = target is _out(runtime.spec)
        prepared = self._apply_direction(runtime, payload, forward=forward)
        if prepared is None:
            return
        started = time.perf_counter()
        message = Message.create(runtime.spec.name, source, prepared, session_id=session.id)
        body = wire_payload(target, message.wire_payload())
        wire = encoder.encode(body) if encoder is not None else body
        transport = writer.transport
        buffered = transport.get_write_buffer_size() if transport is not None else 0
        if buffered > runtime.spec.limits.max_write_buffer_bytes:
            await asyncio.wait_for(writer.drain(), runtime.spec.limits.write_timeout_seconds)
        writer.write(wire)
        await asyncio.wait_for(writer.drain(), runtime.spec.limits.write_timeout_seconds)
        session.messages += 1
        runtime.messages += 1
        _add_bytes(runtime, session, len(wire), inbound=not inbound)
        elapsed = (time.perf_counter() - started) * 1000
        self._note(
            runtime,
            "send",
            session=session.id,
            size=len(wire),
            duration_ms=round(elapsed, 3),
            payload=wire,
        )

    async def _open_site(self, runtime: BridgeRuntime) -> None:
        spec = runtime.spec
        app = web.Application(client_max_size=spec.http.max_body_bytes)
        if spec.input.protocol == "websocket":
            app.router.add_get(spec.input.path, self._bind_ws(runtime))
        else:
            app.router.add_route("*", spec.input.path, self._bind_http(runtime))
        timeout = self.config.server.shutdown_timeout_seconds
        if _RUNNER_TAKES_SHUTDOWN:
            runner = web.AppRunner(app, shutdown_timeout=timeout)
        else:
            runner = web.AppRunner(app)
        await runner.setup()
        runtime.runner = runner
        tls = spec.input.tls
        ssl_ctx = server_context(tls) if tls is not None and tls.enabled else None
        if _RUNNER_TAKES_SHUTDOWN:
            site = web.TCPSite(
                runner,
                spec.input.host,
                _port(spec.input),
                ssl_context=ssl_ctx,
                reuse_address=False,
            )
        else:
            site = web.TCPSite(
                runner,
                spec.input.host,
                _port(spec.input),
                ssl_context=ssl_ctx,
                reuse_address=False,
                shutdown_timeout=timeout,
            )
        await site.start()
        names = runner.addresses
        if not names:
            raise SwitchboardError("The listener did not report an address.")
        host, port = _sockname(names[0])
        runtime.input_host = host
        runtime.input_port = port

    def _bind_ws(self, runtime: BridgeRuntime):
        async def handler(request: web.Request) -> web.WebSocketResponse:
            return await self._on_websocket(runtime, request)

        return handler

    def _bind_http(self, runtime: BridgeRuntime):
        async def handler(request: web.Request) -> web.StreamResponse:
            return await self._on_http(runtime, request)

        return handler

    async def _on_websocket(
        self,
        runtime: BridgeRuntime,
        request: web.Request,
    ) -> web.WebSocketResponse:
        spec = runtime.spec
        socket_in = web.WebSocketResponse(
            heartbeat=spec.websocket.heartbeat_seconds,
            max_msg_size=spec.websocket.max_message_bytes,
            autoping=True,
        )
        await socket_in.prepare(request)
        if self._stopping or not self._acquire(runtime):
            runtime.refused += 1
            await socket_in.close(code=1013)
            return socket_in
        session = _new_session(runtime, f"ws://{request.remote}", _outbound_label(spec))
        runtime.sessions[session.id] = session
        self._note(runtime, "connect", session=session.id, detail=session.inbound)
        try:
            if spec.mode == "router":
                await self._route_socket(runtime, session, socket_in)
                return socket_in
            output = _out(spec).protocol
            if output == "tcp":
                await self._ws_to_tcp(runtime, session, socket_in)
            elif output == "websocket":
                await self._ws_to_ws(runtime, session, socket_in)
            elif output == "udp":
                await self._ws_to_udp(runtime, session, socket_in)
            elif output == "http":
                await self._ws_to_http(runtime, session, socket_in)
            else:
                raise SwitchboardError("This protocol combination is not available.")
        except _Idle:
            session.error = "idle timeout"
            session.state = "closed"
        except FrameError as exc:
            session.error = exc.code
            session.state = "failed"
            runtime.failures += 1
            logger.warning("bridge %s framing error %s", spec.name, exc.code)
        except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError):
            session.error = "connection failed"
            session.state = "failed"
            runtime.failures += 1
            logger.warning("bridge %s connection failed", spec.name)
        except Exception as exc:
            session.error = "internal error"
            session.state = "failed"
            runtime.failures += 1
            logger.warning("bridge %s session failed: %s", spec.name, exc.__class__.__name__)
        else:
            if session.state != "failed":
                session.state = "closed"
        finally:
            runtime.sessions.pop(session.id, None)
            if not socket_in.closed:
                await socket_in.close()
            self._release(runtime)
        return socket_in

    async def _on_http(self, runtime: BridgeRuntime, request: web.Request) -> web.StreamResponse:
        spec = runtime.spec
        if request.method not in spec.http.allowed_methods:
            return web.Response(status=405)
        if _header_size(request.headers) > spec.http.max_header_bytes:
            return web.Response(status=431)
        body = await request.read()
        if len(body) > spec.http.max_body_bytes:
            return web.Response(status=413)
        if self._stopping or not self._acquire(runtime):
            runtime.refused += 1
            return web.Response(status=503)
        headers = _select_headers(request.headers, spec.http.forward_headers)
        try:
            body = view_payload(spec.input, body)
        except FrameError:
            runtime.failures += 1
            self._release(runtime)
            return web.Response(status=422)
        if spec.mode == "router":
            try:
                return await self._route_http(runtime, body, headers, _hop_count(request.headers))
            finally:
                self._release(runtime)
        if spec.http.response == "accepted":
            self._spawn(self._accepted(runtime, body, headers))
            return web.Response(status=spec.http.response_status)
        try:
            response = await self._deliver(runtime, body, headers)
        except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError, FrameError):
            runtime.failures += 1
            logger.warning("bridge %s forwarding failed", spec.name)
            return web.Response(status=502)
        except Exception as exc:
            runtime.failures += 1
            logger.warning("bridge %s forwarding failed: %s", spec.name, exc.__class__.__name__)
            return web.Response(status=500)
        finally:
            self._release(runtime)
        if spec.http.response == "upstream" and response is not None:
            return response
        if response is not None and response.status >= 400:
            runtime.failures += 1
            logger.warning("bridge %s output returned status %s", spec.name, response.status)
            return web.Response(status=502)
        return web.Response(status=spec.http.response_status)

    async def _accepted(
        self,
        runtime: BridgeRuntime,
        body: bytes,
        headers: dict[str, str],
    ) -> None:
        try:
            response = await self._deliver(runtime, body, headers)
            if response is not None and response.status >= 400:
                runtime.failures += 1
                logger.warning(
                    "bridge %s output returned status %s",
                    runtime.spec.name,
                    response.status,
                )
        except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError, FrameError):
            runtime.failures += 1
            logger.warning("bridge %s forwarding failed", runtime.spec.name)
        except Exception as exc:
            runtime.failures += 1
            logger.warning(
                "bridge %s forwarding failed: %s",
                runtime.spec.name,
                exc.__class__.__name__,
            )
        finally:
            self._release(runtime)

    async def _deliver(
        self,
        runtime: BridgeRuntime,
        body: bytes,
        headers: dict[str, str],
    ) -> web.Response | None:
        prepared = self._apply_direction(runtime, body, forward=True)
        if prepared is None:
            return None
        protocol = _out(runtime.spec).protocol
        if protocol == "http":
            return await self._http_response(runtime, prepared, headers)
        if protocol == "tcp":
            await self._deliver_tcp(runtime, prepared)
            return None
        if protocol == "udp":
            await self._deliver_udp(runtime, prepared)
            return None
        if protocol == "websocket":
            await self._deliver_ws(runtime, prepared)
            return None
        raise SwitchboardError("This protocol combination is not available.")

    async def _tcp_to_ws(
        self,
        runtime: BridgeRuntime,
        session: Session,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        socket_out = await self._connect_ws(runtime)
        session.state = "active"
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(self._tcp_frames_to_ws(runtime, session, reader, socket_out, stop)),
            asyncio.create_task(self._ws_frames_to_tcp(runtime, session, socket_out, writer, stop)),
            asyncio.create_task(self._watch_idle(runtime, session, stop)),
        ]
        try:
            await self._finish_tasks(tasks, stop, session, half_close=False)
        finally:
            await socket_out.close()

    async def _tcp_to_http(
        self,
        runtime: BridgeRuntime,
        session: Session,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        spec = runtime.spec
        session.state = "active"
        framing = spec.input.framing
        parser = FrameParser(framing, on_error=spec.framing_error) if framing is not None else None
        encoder = FrameParser(framing, on_error=spec.framing_error) if framing is not None else None
        partial_since: float | None = None
        while not self._stopping:
            _raise_if_due(spec, session, parser, partial_since)
            timeout = _wait_budget(spec, session, parser, partial_since)
            try:
                chunk = await _read(reader, timeout)
            except TimeoutError:
                _raise_if_due(spec, session, parser, partial_since, forced=True)
                continue
            if not chunk:
                if parser is not None:
                    parser.finish()
                return
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(chunk), inbound=True)
            payloads = parser.feed(chunk) if parser is not None else [chunk]
            pending = parser is not None and parser.pending
            partial_since = time.monotonic() if pending else None
            for payload in payloads:
                viewed = view_payload(spec.input, payload)
                prepared = self._apply_direction(runtime, viewed, forward=True)
                if prepared is None:
                    continue
                await self._reply_http(runtime, session, writer, encoder, prepared)

    async def _ws_to_tcp(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse,
    ) -> None:
        reader, writer = await self._connect_tcp(runtime)
        session.state = "active"
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(self._ws_frames_out(runtime, session, socket_in, writer, stop)),
            asyncio.create_task(self._tcp_frames_to_ws(runtime, session, reader, socket_in, stop)),
            asyncio.create_task(self._watch_idle(runtime, session, stop)),
        ]
        try:
            await self._finish_tasks(tasks, stop, session, half_close=False)
        finally:
            _close_writer(writer)
            self._writers.discard(writer)

    async def _ws_to_ws(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse,
    ) -> None:
        socket_out = await self._connect_ws(runtime)
        session.state = "active"
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(self._copy_ws(runtime, session, socket_in, socket_out, stop, True)),
            asyncio.create_task(
                self._copy_ws(runtime, session, socket_out, socket_in, stop, False)
            ),
            asyncio.create_task(self._watch_idle(runtime, session, stop)),
        ]
        try:
            await self._finish_tasks(tasks, stop, session, half_close=False)
        finally:
            await socket_out.close()

    async def _ws_to_http(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse,
    ) -> None:
        session.state = "active"
        while not self._stopping:
            payload = await self._ws_receive(runtime, session, socket_in)
            if payload is None:
                return
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(payload), inbound=True)
            viewed = view_payload(runtime.spec.input, payload)
            prepared = self._apply_direction(runtime, viewed, forward=True)
            if prepared is None:
                continue
            status, body, _headers = await self._http_exchange(runtime, prepared, {})
            if status >= 400:
                runtime.failures += 1
                logger.warning("bridge %s output returned status %s", runtime.spec.name, status)
                continue
            returned = self._apply_direction(runtime, body, forward=False)
            if returned:
                await socket_in.send_bytes(wire_payload(runtime.spec.input, returned))
                _remember(runtime, body, inbound=False)

    async def _ws_to_udp(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse,
    ) -> None:
        queue = _QueueProtocol(4, lambda _size: self._drop(runtime))
        transport = await self._connect_udp(runtime, queue)
        session.state = "active"
        stop = asyncio.Event()
        tasks = [
            asyncio.create_task(self._ws_to_udp_out(runtime, session, socket_in, transport, stop)),
            asyncio.create_task(self._udp_to_ws(runtime, session, queue, socket_in, stop)),
            asyncio.create_task(self._watch_idle(runtime, session, stop)),
        ]
        try:
            await self._finish_tasks(tasks, stop, session, half_close=False)
        finally:
            transport.close()
            self._transports.discard(transport)

    async def _connect_tcp(
        self,
        runtime: BridgeRuntime,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        endpoint = _out(runtime.spec)
        self._warn_unverified(runtime, endpoint)
        tls = endpoint.tls
        ssl_ctx = client_context(tls) if tls is not None and tls.enabled else None

        async def connect() -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
            return await asyncio.wait_for(
                asyncio.open_connection(endpoint.host, _port(endpoint), ssl=ssl_ctx),
                runtime.spec.limits.connect_timeout_seconds,
            )

        reader, writer = await self._retry(runtime, connect)
        _nodelay(writer)
        self._writers.add(writer)
        return reader, writer

    async def _connect_ws(self, runtime: BridgeRuntime) -> aiohttp.ClientWebSocketResponse:
        endpoint = _out(runtime.spec)
        self._warn_unverified(runtime, endpoint)
        session = await self._client(runtime)
        url = endpoint.url or endpoint_label(endpoint)
        ssl_arg = _ssl_arg(endpoint)

        async def connect() -> aiohttp.ClientWebSocketResponse:
            timeout = aiohttp.ClientWSTimeout(ws_close=runtime.spec.limits.connect_timeout_seconds)
            return await session.ws_connect(
                url,
                heartbeat=runtime.spec.websocket.heartbeat_seconds,
                max_msg_size=runtime.spec.websocket.max_message_bytes,
                ssl=ssl_arg,
                timeout=timeout,
            )

        return await self._retry(runtime, connect)

    async def _connect_udp(
        self,
        runtime: BridgeRuntime,
        protocol: _QueueProtocol,
    ) -> asyncio.DatagramTransport:
        endpoint = _out(runtime.spec)

        async def connect() -> asyncio.DatagramTransport:
            transport, _protocol = await asyncio.wait_for(
                asyncio.get_running_loop().create_datagram_endpoint(
                    lambda: protocol,
                    remote_addr=(endpoint.host, _port(endpoint)),
                ),
                runtime.spec.limits.connect_timeout_seconds,
            )
            opened = _as_datagram(transport)
            if opened is None:
                transport.close()
                raise SwitchboardError("The UDP output did not open.")
            return opened

        opened = await self._retry(runtime, connect)
        self._transports.add(opened)
        return opened

    async def _retry(self, runtime: BridgeRuntime, connect: Callable[[], Awaitable[_T]]) -> _T:
        policy = runtime.spec.reconnect
        delay = policy.initial_delay_seconds
        attempt = 0
        while True:
            try:
                return await connect()
            except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError):
                attempt += 1
                if not policy.enabled or attempt > policy.max_attempts:
                    raise
                logger.warning(
                    "bridge %s outbound connect failed, attempt %s",
                    runtime.spec.name,
                    attempt,
                )
                await asyncio.sleep(delay)
                if policy.policy == "exponential":
                    delay = min(policy.max_delay_seconds, delay * 2)

    async def _client(self, runtime: BridgeRuntime) -> aiohttp.ClientSession:
        if runtime.client is None or runtime.client.closed:
            timeout = aiohttp.ClientTimeout(
                total=runtime.spec.http.timeout_seconds,
                connect=runtime.spec.limits.connect_timeout_seconds,
            )
            runtime.client = aiohttp.ClientSession(timeout=timeout)
        return runtime.client

    def _warn_unverified(self, runtime: BridgeRuntime, endpoint: EndpointSpec) -> None:
        tls = endpoint.tls
        if tls is None or not tls.enabled or tls.verify:
            return
        if runtime.spec.name in self._unverified:
            return
        self._unverified.add(runtime.spec.name)
        logger.warning("bridge %s disables TLS certificate verification", runtime.spec.name)

    async def _http_exchange(
        self,
        runtime: BridgeRuntime,
        payload: bytes,
        headers: dict[str, str],
        target: EndpointSpec | None = None,
    ) -> tuple[int, bytes, dict[str, str]]:
        endpoint = target if target is not None else _out(runtime.spec)
        self._warn_unverified(runtime, endpoint)
        session = await self._client(runtime)
        url = endpoint.url or endpoint_label(endpoint)
        timeout = aiohttp.ClientTimeout(
            total=runtime.spec.http.timeout_seconds,
            connect=runtime.spec.limits.connect_timeout_seconds,
        )
        async with session.request(
            endpoint.method,
            url,
            data=wire_payload(endpoint, payload),
            headers=headers,
            ssl=_ssl_arg(endpoint),
            timeout=timeout,
        ) as response:
            body = await response.read()
            copied = _select_headers(response.headers, runtime.spec.http.forward_headers)
            return response.status, view_payload(endpoint, body), copied

    async def _http_response(
        self,
        runtime: BridgeRuntime,
        payload: bytes,
        headers: dict[str, str],
    ) -> web.Response:
        status, body, copied = await self._http_exchange(runtime, payload, headers)
        returned = self._apply_direction(runtime, body, forward=False)
        if returned is None:
            returned = b""
        if len(returned) > runtime.spec.http.max_body_bytes:
            return web.Response(status=502)
        return web.Response(status=status, body=returned, headers=copied)

    async def _deliver_tcp(self, runtime: BridgeRuntime, payload: bytes) -> None:
        _reader, writer = await self._connect_tcp(runtime)
        try:
            endpoint = _out(runtime.spec)
            framing = endpoint.framing
            encoder = FrameParser(framing, on_error=runtime.spec.framing_error) if framing else None
            body = wire_payload(endpoint, payload)
            wire = encoder.encode(body) if encoder is not None else body
            writer.write(wire)
            await asyncio.wait_for(writer.drain(), runtime.spec.limits.write_timeout_seconds)
            _remember(runtime, wire, inbound=False)
        finally:
            _close_writer(writer)
            self._writers.discard(writer)

    async def _deliver_udp(self, runtime: BridgeRuntime, payload: bytes) -> None:
        spec = runtime.spec
        if len(payload) > spec.udp.max_datagram_bytes:
            self._drop(runtime)
            if spec.udp.overflow == "reject":
                logger.warning("bridge %s rejected an oversized udp datagram", spec.name)
            raise FrameError("The frame exceeds the size limit.", code="FRAME_TOO_LARGE")
        queue = _QueueProtocol(1, lambda _size: self._drop(runtime))
        transport = await self._connect_udp(runtime, queue)
        try:
            body = wire_payload(_out(runtime.spec), payload)
            transport.sendto(body)
            _remember(runtime, body, inbound=False)
        finally:
            transport.close()
            self._transports.discard(transport)

    async def _deliver_ws(self, runtime: BridgeRuntime, payload: bytes) -> None:
        if len(payload) > runtime.spec.websocket.max_message_bytes:
            raise FrameError("The frame exceeds the size limit.", code="FRAME_TOO_LARGE")
        socket_out = await self._connect_ws(runtime)
        try:
            body = wire_payload(_out(runtime.spec), payload)
            await socket_out.send_bytes(body)
            _remember(runtime, body, inbound=False)
        finally:
            await socket_out.close()

    async def _reply_http(
        self,
        runtime: BridgeRuntime,
        session: Session,
        writer: asyncio.StreamWriter,
        encoder: FrameParser | None,
        payload: bytes,
    ) -> None:
        status, body, _headers = await self._http_exchange(runtime, payload, {})
        if status >= 400:
            runtime.failures += 1
            logger.warning("bridge %s output returned status %s", runtime.spec.name, status)
            return
        if body:
            await self._write_payload(
                runtime,
                session,
                writer,
                encoder,
                body,
                target=runtime.spec.input,
                inbound=False,
            )

    async def _tcp_frames_to_ws(
        self,
        runtime: BridgeRuntime,
        session: Session,
        reader: asyncio.StreamReader,
        socket_out: web.WebSocketResponse | aiohttp.ClientWebSocketResponse,
        stop: asyncio.Event,
    ) -> None:
        spec = runtime.spec
        framing = spec.input.framing if spec.input.protocol == "tcp" else _out(spec).framing
        parser = FrameParser(framing, on_error=spec.framing_error) if framing is not None else None
        partial_since: float | None = None
        while not stop.is_set():
            _raise_if_due(spec, session, parser, partial_since)
            timeout = _wait_budget(spec, session, parser, partial_since)
            try:
                chunk = await _read(reader, timeout)
            except TimeoutError:
                _raise_if_due(spec, session, parser, partial_since, forced=True)
                continue
            if stop.is_set():
                return
            if not chunk:
                if parser is not None:
                    parser.finish()
                stop.set()
                return
            session.last_activity = time.monotonic()
            inbound = spec.input.protocol == "tcp"
            _add_bytes(runtime, session, len(chunk), inbound=inbound)
            payloads = parser.feed(chunk) if parser is not None else [chunk]
            pending = parser is not None and parser.pending
            partial_since = time.monotonic() if pending else None
            tcp_side = spec.input if spec.input.protocol == "tcp" else _out(spec)
            ws_side = _out(spec) if spec.input.protocol == "tcp" else spec.input
            for payload in payloads:
                viewed = view_payload(tcp_side, payload)
                forward = ws_side is _out(spec)
                prepared = self._apply_direction(runtime, viewed, forward=forward)
                if prepared is None:
                    continue
                payload = wire_payload(ws_side, prepared)
                if len(payload) > spec.websocket.max_message_bytes:
                    raise FrameError("The frame exceeds the size limit.", code="FRAME_TOO_LARGE")
                await socket_out.send_bytes(payload)
                session.messages += 1
                runtime.messages += 1

    async def _ws_frames_to_tcp(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse | aiohttp.ClientWebSocketResponse,
        writer: asyncio.StreamWriter,
        stop: asyncio.Event,
    ) -> None:
        framing = runtime.spec.input.framing
        encoder = FrameParser(framing, on_error=runtime.spec.framing_error) if framing else None
        while not stop.is_set():
            payload = await self._ws_receive(runtime, session, socket_in)
            if payload is None or stop.is_set():
                stop.set()
                return
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(payload), inbound=True)
            viewed = view_payload(_out(runtime.spec), payload)
            await self._write_payload(
                runtime,
                session,
                writer,
                encoder,
                viewed,
                target=runtime.spec.input,
                inbound=True,
            )

    async def _ws_frames_out(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse,
        writer: asyncio.StreamWriter,
        stop: asyncio.Event,
    ) -> None:
        framing = _out(runtime.spec).framing
        encoder = FrameParser(framing, on_error=runtime.spec.framing_error) if framing else None
        while not stop.is_set():
            payload = await self._ws_receive(runtime, session, socket_in)
            if payload is None or stop.is_set():
                stop.set()
                return
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(payload), inbound=True)
            viewed = view_payload(runtime.spec.input, payload)
            await self._write_payload(
                runtime,
                session,
                writer,
                encoder,
                viewed,
                target=_out(runtime.spec),
                inbound=True,
            )

    async def _copy_ws(
        self,
        runtime: BridgeRuntime,
        session: Session,
        source: web.WebSocketResponse | aiohttp.ClientWebSocketResponse,
        target: web.WebSocketResponse | aiohttp.ClientWebSocketResponse,
        stop: asyncio.Event,
        inbound: bool,
    ) -> None:
        while not stop.is_set():
            payload = await self._ws_receive(runtime, session, source)
            if payload is None or stop.is_set():
                stop.set()
                return
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(payload), inbound=inbound)
            origin = runtime.spec.input if inbound else _out(runtime.spec)
            destination = _out(runtime.spec) if inbound else runtime.spec.input
            viewed = view_payload(origin, payload)
            prepared = self._apply_direction(runtime, viewed, forward=inbound)
            if prepared is None:
                continue
            await target.send_bytes(wire_payload(destination, prepared))
            _remember(runtime, payload, inbound=not inbound)

    async def _ws_to_udp_out(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse,
        transport: asyncio.DatagramTransport,
        stop: asyncio.Event,
    ) -> None:
        spec = runtime.spec
        while not stop.is_set():
            payload = await self._ws_receive(runtime, session, socket_in)
            if payload is None or stop.is_set():
                stop.set()
                return
            viewed = view_payload(spec.input, payload)
            prepared = self._apply_direction(runtime, viewed, forward=True)
            if prepared is None:
                continue
            payload = wire_payload(_out(spec), prepared)
            if len(payload) > spec.udp.max_datagram_bytes:
                self._drop(runtime)
                if spec.udp.overflow == "reject":
                    logger.warning("bridge %s rejected an oversized udp datagram", spec.name)
                continue
            transport.sendto(payload)
            session.last_activity = time.monotonic()
            _add_bytes(runtime, session, len(payload), inbound=True)

    async def _udp_to_ws(
        self,
        runtime: BridgeRuntime,
        session: Session,
        protocol: _QueueProtocol,
        socket_out: web.WebSocketResponse | aiohttp.ClientWebSocketResponse,
        stop: asyncio.Event,
    ) -> None:
        while not stop.is_set():
            try:
                data, _peer = await asyncio.wait_for(protocol.queue.get(), 0.05)
            except TimeoutError:
                _raise_if_due(runtime.spec, session, None, None)
                continue
            session.last_activity = time.monotonic()
            viewed = view_payload(_out(runtime.spec), data)
            prepared = self._apply_direction(runtime, viewed, forward=False)
            if prepared is None:
                continue
            data = wire_payload(runtime.spec.input, prepared)
            await socket_out.send_bytes(data)
            _add_bytes(runtime, session, len(data), inbound=False)

    async def _ws_receive(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse | aiohttp.ClientWebSocketResponse,
    ) -> bytes | None:
        while True:
            _raise_if_due(runtime.spec, session, None, None)
            timeout = _wait_budget(runtime.spec, session, None, None)
            try:
                message = await asyncio.wait_for(socket_in.receive(), timeout)
            except TimeoutError:
                _raise_if_due(runtime.spec, session, None, None, forced=True)
                continue
            if message.type in _WS_DONE:
                return None
            payload = _ws_bytes(message)
            if payload is None:
                continue
            if len(payload) > runtime.spec.websocket.max_message_bytes:
                raise FrameError("The frame exceeds the size limit.", code="FRAME_TOO_LARGE")
            return payload

    async def _ws_replies_to_udp(
        self,
        runtime: BridgeRuntime,
        socket_out: aiohttp.ClientWebSocketResponse,
        addr: tuple[str, int],
        peer: _Peer,
    ) -> None:
        transport = runtime.udp
        while not self._stopping and transport is not None:
            message = await socket_out.receive()
            if message.type in _WS_DONE:
                return
            payload = _ws_bytes(message)
            if payload is None:
                continue
            peer.last_activity = time.monotonic()
            viewed = view_payload(_out(runtime.spec), payload)
            prepared = self._apply_direction(runtime, viewed, forward=False)
            if prepared is None:
                continue
            body = wire_payload(runtime.spec.input, prepared)
            transport.sendto(body, addr)
            _remember(runtime, payload, inbound=False)

    async def _udp_http(
        self,
        runtime: BridgeRuntime,
        addr: tuple[str, int],
        payload: bytes,
    ) -> None:
        prepared = self._apply_direction(runtime, payload, forward=True)
        if prepared is None:
            return
        status, body, _headers = await self._http_exchange(runtime, prepared, {})
        if status >= 400:
            runtime.failures += 1
            logger.warning("bridge %s output returned status %s", runtime.spec.name, status)
            return
        returned = self._apply_direction(runtime, body, forward=False)
        if returned and runtime.udp is not None:
            runtime.udp.sendto(wire_payload(runtime.spec.input, returned), addr)
            _remember(runtime, body, inbound=False)

    def _open_lanes(self, runtime: BridgeRuntime) -> None:
        lanes: dict[str, Lane] = {}
        send = self._make_send(runtime)
        for output in runtime.spec.outputs:
            lanes[output.name] = Lane(output.name, output, runtime.spec.delivery, send)
        self._lanes[runtime.spec.name] = lanes
        for lane in lanes.values():
            self._spawn(lane.run(lambda: self._stopping), runtime)

    def _make_send(self, runtime: BridgeRuntime):
        async def send(
            endpoint: EndpointSpec,
            payload: bytes,
            headers: dict[str, str],
        ) -> DeliveryResult:
            return await self._send_routed(runtime, endpoint, payload, headers)

        return send

    async def _route_http(
        self,
        runtime: BridgeRuntime,
        body: bytes,
        headers: dict[str, str],
        hops: int,
    ) -> web.StreamResponse:
        spec = runtime.spec
        wait = spec.http.response != "accepted"
        status, results = await self._route_bytes(runtime, body, headers, hops, wait=wait)
        if status == "loop":
            return web.Response(status=508)
        if status == "unmatched":
            return web.Response(status=422)
        if status == "drop":
            code = spec.http.response_status if spec.http.response == "accepted" else 204
            return web.Response(status=code)
        if status != "ok":
            return web.Response(status=503 if _buffer_refused(results) else 502)
        if spec.http.response == "upstream" and results:
            item = results[0]
            if item.ok:
                return web.Response(status=item.status or 200, body=item.body, headers=item.headers)
            return web.Response(status=502)
        return web.Response(status=spec.http.response_status)

    async def _route_frames(
        self,
        runtime: BridgeRuntime,
        session: Session,
        reader: asyncio.StreamReader,
    ) -> None:
        spec = runtime.spec
        framing = spec.input.framing
        parser = FrameParser(framing, on_error=spec.framing_error) if framing is not None else None
        session.state = "active"
        partial_since: float | None = None
        while not self._stopping:
            _raise_if_due(spec, session, parser, partial_since)
            timeout = _wait_budget(spec, session, parser, partial_since)
            try:
                chunk = await _read(reader, timeout)
            except TimeoutError:
                _raise_if_due(spec, session, parser, partial_since, forced=True)
                continue
            if not chunk:
                if parser is not None:
                    parser.finish()
                return
            session.last_activity = time.monotonic()
            payloads = parser.feed(chunk) if parser is not None else [chunk]
            pending = parser is not None and parser.pending
            partial_since = time.monotonic() if pending else None
            for payload in payloads:
                viewed = view_payload(spec.input, payload)
                status, _results = await self._route_bytes(runtime, viewed, {}, 0, wait=True)
                if status == "close":
                    return

    async def _route_socket(
        self,
        runtime: BridgeRuntime,
        session: Session,
        socket_in: web.WebSocketResponse,
    ) -> None:
        session.state = "active"
        while not self._stopping:
            payload = await self._ws_receive(runtime, session, socket_in)
            if payload is None:
                return
            session.last_activity = time.monotonic()
            viewed = view_payload(runtime.spec.input, payload)
            status, _results = await self._route_bytes(runtime, viewed, {}, 0, wait=True)
            if status == "close":
                return

    async def _route_bytes(
        self,
        runtime: BridgeRuntime,
        payload: bytes,
        headers: dict[str, str],
        hops: int,
        *,
        wait: bool,
    ) -> tuple[str, list[DeliveryResult]]:
        spec = runtime.spec
        if hops >= spec.max_hops:
            runtime.dropped += 1
            return "loop", []
        document = Document(data=payload, metadata={"hops": str(hops)})
        try:
            transformed = run_pipeline(
                document,
                list(spec.transforms),
                on_error=spec.transform_error,
            )
        except TransformError:
            runtime.failures += 1
            runtime.transform_failures += 1
            self._note(runtime, "transform", outcome="error", detail="transform failed")
            logger.warning("bridge %s transform failed", spec.name)
            if spec.transform_error == "route":
                await self._deliver_diagnostic(runtime, hops)
            if spec.transform_error == "close":
                return "close", []
            return "fail", []
        if transformed is None:
            runtime.dropped += 1
            return "drop", []
        selected = select_outputs(spec, list(self.config.routes), transformed)
        if selected is None:
            runtime.unmatched += 1
            if spec.on_unmatched == "default" and spec.default_output is not None:
                selected = [spec.default_output]
            elif spec.on_unmatched == "error":
                return "unmatched", []
            else:
                return "drop", []
        self._note(runtime, "route", detail=",".join(selected)[:200])
        outgoing = dict(headers)
        outgoing["X-Switchboard-Hops"] = str(hops + 1)
        results = await self._fanout(runtime, selected, transformed.data, outgoing, wait=wait)
        if spec.delivery.success == "all" and not all(item.ok for item in results):
            runtime.failures += 1
            return "fail", results
        if spec.delivery.success == "any" and not any(item.ok for item in results):
            runtime.failures += 1
            return "fail", results
        if spec.delivery.success == "best_effort" and not any(item.ok for item in results):
            runtime.failures += 1
            return "fail", results
        runtime.messages += 1
        return "ok", results

    async def _fanout(
        self,
        runtime: BridgeRuntime,
        names: list[str],
        payload: bytes,
        headers: dict[str, str],
        *,
        wait: bool,
    ) -> list[DeliveryResult]:
        lanes = [self._lanes[runtime.spec.name][name] for name in names]
        blocked = runtime.spec.delivery.success == "all" and any(
            not lane.fits(len(payload)) for lane in lanes
        )
        if blocked:
            runtime.dropped += 1
            return [DeliveryResult(ok=False, status=503)]
        results: list[DeliveryResult] = []
        waiting: list[asyncio.Future[DeliveryResult]] = []
        for lane in lanes:
            before = lane.dropped
            future = lane.admit(payload, headers)
            runtime.dropped += lane.dropped - before
            if future is None:
                results.append(DeliveryResult(ok=False, status=503))
            elif wait:
                waiting.append(future)
            else:
                results.append(DeliveryResult(ok=True))
        if waiting:
            results.extend(await asyncio.gather(*waiting))
        return results

    async def _deliver_diagnostic(self, runtime: BridgeRuntime, hops: int) -> None:
        name = runtime.spec.error_output
        if name is None or hops + 1 >= runtime.spec.max_hops:
            return
        payload = b'{"error":"transform"}'
        headers = {"X-Switchboard-Hops": str(hops + 1), "X-Switchboard-Error": "transform"}
        await self._fanout(runtime, [name], payload, headers, wait=True)

    async def _send_routed(
        self,
        runtime: BridgeRuntime,
        endpoint: EndpointSpec,
        payload: bytes,
        headers: dict[str, str],
    ) -> DeliveryResult:
        if endpoint.protocol == "http":
            return await self._send_http(runtime, endpoint, payload, headers)
        if endpoint.protocol == "tcp":
            await self._send_tcp(runtime, endpoint, payload)
            return DeliveryResult(ok=True)
        if endpoint.protocol == "websocket":
            await self._send_ws(runtime, endpoint, payload)
            return DeliveryResult(ok=True)
        if endpoint.protocol == "udp":
            await self._send_udp(runtime, endpoint, payload)
            return DeliveryResult(ok=True)
        raise DeliveryError("delivery failed", retry="no")

    async def _send_http(
        self,
        runtime: BridgeRuntime,
        endpoint: EndpointSpec,
        payload: bytes,
        headers: dict[str, str],
    ) -> DeliveryResult:
        try:
            status, body, copied = await self._http_exchange(runtime, payload, headers, endpoint)
        except aiohttp.ClientConnectorError as exc:
            raise DeliveryError("delivery failed", retry="safe") from exc
        except (TimeoutError, aiohttp.ClientError) as exc:
            raise DeliveryError("delivery failed", retry="replay") from exc
        if status >= 500:
            raise DeliveryError("delivery failed", retry="replay")
        if status >= 400:
            raise DeliveryError("delivery failed", retry="no")
        return DeliveryResult(ok=True, status=status, body=body, headers=copied)

    async def _send_tcp(
        self,
        runtime: BridgeRuntime,
        endpoint: EndpointSpec,
        payload: bytes,
    ) -> None:
        self._warn_unverified(runtime, endpoint)
        tls = endpoint.tls
        ssl_ctx = client_context(tls) if tls is not None and tls.enabled else None
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(endpoint.host, _port(endpoint), ssl=ssl_ctx),
                runtime.spec.limits.connect_timeout_seconds,
            )
        except (ConnectionError, OSError, TimeoutError) as exc:
            raise DeliveryError("delivery failed", retry="safe") from exc
        try:
            framing = endpoint.framing
            body = wire_payload(endpoint, payload)
            if framing is None:
                wire = body
            else:
                wire = FrameParser(framing, on_error=runtime.spec.framing_error).encode(body)
            writer.write(wire)
            await asyncio.wait_for(writer.drain(), runtime.spec.limits.write_timeout_seconds)
        except FrameError as exc:
            raise DeliveryError("delivery failed", retry="no") from exc
        except (ConnectionError, OSError, TimeoutError) as exc:
            raise DeliveryError("delivery failed", retry="replay") from exc
        finally:
            _close_writer(writer)

    async def _send_ws(
        self,
        runtime: BridgeRuntime,
        endpoint: EndpointSpec,
        payload: bytes,
    ) -> None:
        self._warn_unverified(runtime, endpoint)
        payload = wire_payload(endpoint, payload)
        if len(payload) > runtime.spec.websocket.max_message_bytes:
            raise DeliveryError("delivery failed", retry="no")
        session = await self._client(runtime)
        url = endpoint.url or endpoint_label(endpoint)
        try:
            socket_out = await session.ws_connect(
                url,
                heartbeat=runtime.spec.websocket.heartbeat_seconds,
                max_msg_size=runtime.spec.websocket.max_message_bytes,
                ssl=_ssl_arg(endpoint),
            )
        except (ConnectionError, OSError, TimeoutError, aiohttp.ClientError) as exc:
            raise DeliveryError("delivery failed", retry="safe") from exc
        try:
            await socket_out.send_bytes(payload)
        except (ConnectionError, OSError, aiohttp.ClientError) as exc:
            raise DeliveryError("delivery failed", retry="replay") from exc
        finally:
            await socket_out.close()

    async def _send_udp(
        self,
        runtime: BridgeRuntime,
        endpoint: EndpointSpec,
        payload: bytes,
    ) -> None:
        payload = wire_payload(endpoint, payload)
        if len(payload) > runtime.spec.udp.max_datagram_bytes:
            runtime.dropped += 1
            raise DeliveryError("delivery failed", retry="no")
        loop = asyncio.get_running_loop()
        try:
            transport, _protocol = await asyncio.wait_for(
                loop.create_datagram_endpoint(
                    asyncio.DatagramProtocol,
                    remote_addr=(endpoint.host, _port(endpoint)),
                ),
                runtime.spec.limits.connect_timeout_seconds,
            )
        except (ConnectionError, OSError, TimeoutError) as exc:
            raise DeliveryError("delivery failed", retry="safe") from exc
        try:
            opened = _as_datagram(transport)
            if opened is not None:
                opened.sendto(payload)
        finally:
            transport.close()

    def _acquire(self, runtime: BridgeRuntime) -> bool:
        if runtime.active >= runtime.spec.limits.max_connections:
            return False
        if self._connections >= self.config.server.max_connections:
            return False
        runtime.active += 1
        self._connections += 1
        return True

    def _release(self, runtime: BridgeRuntime) -> None:
        runtime.active = max(0, runtime.active - 1)
        self._connections = max(0, self._connections - 1)

    def _drop(self, runtime: BridgeRuntime) -> None:
        runtime.dropped += 1

    async def _drop_writer(self, writer: asyncio.StreamWriter) -> None:
        _close_writer(writer)
        self._writers.discard(writer)

    async def _drop_transport(self, transport: asyncio.BaseTransport) -> None:
        transport.close()
        self._transports.discard(transport)

    async def _close_idle(self) -> None:
        return None

    def _spawn(self, coro: object, runtime: BridgeRuntime | None = None) -> None:
        task: asyncio.Task[None] = asyncio.create_task(coro)  # type: ignore[arg-type]
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        if runtime is not None:
            runtime.tasks.add(task)
            task.add_done_callback(runtime.tasks.discard)


@dataclass
class _Peer:
    """One UDP client and the output connection kept for its replies."""

    addr: tuple[str, int]
    send_fn: Callable[[bytes], Awaitable[None]]
    close_fn: Callable[[], Awaitable[object]]
    last_activity: float = field(default_factory=time.monotonic)

    async def deliver(self, data: bytes) -> None:
        await self.send_fn(data)

    async def shutdown(self) -> None:
        await self.close_fn()


def _remember(runtime: BridgeRuntime, payload: bytes, *, inbound: bool) -> None:
    if inbound:
        runtime.bytes_in += len(payload)
    else:
        runtime.bytes_out += len(payload)
    runtime.messages += 1


def _add_bytes(runtime: BridgeRuntime, session: Session, size: int, *, inbound: bool) -> None:
    if inbound:
        session.bytes_in += size
        runtime.bytes_in += size
    else:
        session.bytes_out += size
        runtime.bytes_out += size


def _wait_budget(
    spec: BridgeSpec,
    session: Session,
    parser: FrameParser | None,
    partial_since: float | None,
) -> float:
    now = time.monotonic()
    waits = [spec.limits.idle_timeout_seconds - (now - session.last_activity)]
    if parser is not None and parser.pending and partial_since is not None:
        waits.append(spec.limits.frame_timeout_seconds - (now - partial_since))
    if spec.limits.read_timeout_seconds is not None:
        waits.append(spec.limits.read_timeout_seconds)
    return max(0.0, min(waits))


def _raise_if_due(
    spec: BridgeSpec,
    session: Session,
    parser: FrameParser | None,
    partial_since: float | None,
    *,
    forced: bool = False,
) -> None:
    now = time.monotonic()
    if (
        parser is not None
        and parser.pending
        and partial_since is not None
        and now - partial_since >= spec.limits.frame_timeout_seconds
    ):
        raise FrameError("The frame was not completed in time.", code="FRAME_TIMEOUT")
    if now - session.last_activity >= spec.limits.idle_timeout_seconds:
        raise _Idle()
    if forced and spec.limits.read_timeout_seconds is not None:
        raise FrameError("The read timed out.", code="READ_TIMEOUT")


async def _read(reader: asyncio.StreamReader, timeout: float) -> bytes:
    if timeout <= 0:
        raise TimeoutError
    return await asyncio.wait_for(reader.read(_READ_SIZE), timeout)


def _nodelay(writer: asyncio.StreamWriter) -> None:
    sock = writer.get_extra_info("socket")
    if isinstance(sock, socket.socket):
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            logger.debug("could not enable TCP_NODELAY")


def _close_writer(writer: asyncio.StreamWriter) -> None:
    if writer.is_closing():
        return
    writer.close()


def _peer_addr(addr: tuple[str | int, ...] | None) -> tuple[str, int]:
    if not addr:
        return ("", 0)
    host = str(addr[0])
    port = addr[1] if len(addr) > 1 and isinstance(addr[1], int) else 0
    return (host, port)


def _bridge_name(params: dict[str, object]) -> str:
    name = params.get("name")
    if not isinstance(name, str) or name == "":
        raise SwitchboardError("Bridge name is required.")
    return name


def _output_label(spec: BridgeSpec) -> str:
    if spec.output is not None:
        return endpoint_label(spec.output)
    return ", ".join(endpoint_label(item) for item in spec.outputs)


def _connection_row(session: Session) -> dict[str, object]:
    return {
        "id": session.id,
        "bridge": session.bridge,
        "state": session.state,
        "inbound": session.inbound,
        "outbound": session.outbound,
        "messages": session.messages,
        "bytes_in": session.bytes_in,
        "bytes_out": session.bytes_out,
        "error": session.error,
    }


def _sockname(value: object) -> tuple[str, int]:
    if isinstance(value, tuple) and len(value) >= 2 and isinstance(value[1], int):
        return (str(value[0]), value[1])
    raise SwitchboardError("The socket did not report an address.")


def _out(spec: BridgeSpec) -> EndpointSpec:
    output = spec.output
    if output is None:
        raise SwitchboardError("gateway mode requires an output")
    return output


def _outbound_label(spec: BridgeSpec) -> str:
    if spec.mode == "router":
        return "router"
    return endpoint_label(_out(spec))


def _hop_count(headers: Mapping[str, str]) -> int:
    raw = headers.get("X-Switchboard-Hops", "0")
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _buffer_refused(results: list[DeliveryResult]) -> bool:
    return any(item.status == 503 for item in results)


def _port(endpoint: EndpointSpec) -> int:
    if endpoint.port is None:
        raise SwitchboardError("port is required")
    return endpoint.port


def _ssl_arg(endpoint: EndpointSpec) -> ssl.SSLContext | bool:
    tls = endpoint.tls
    if tls is None or not tls.enabled:
        return False
    return client_context(tls)


def _new_session(runtime: BridgeRuntime, inbound: str, outbound: str) -> Session:
    now = time.monotonic()
    return Session(
        id=str(uuid.uuid4()),
        bridge=runtime.spec.name,
        state="connecting",
        inbound=inbound,
        outbound=outbound,
        opened_at=now,
        last_activity=now,
    )


def _header_size(headers: Mapping[str, str]) -> int:
    return sum(len(name) + len(value) for name, value in headers.items())


def _select_headers(headers: Mapping[str, str], allow: list[str]) -> dict[str, str]:
    allowed = set(allow)
    selected: dict[str, str] = {}
    for name, value in headers.items():
        key = name.lower()
        if key in _HOP_HEADERS or key in _SENSITIVE_HEADERS and key not in allowed:
            continue
        if key == "content-type" or key in allowed:
            selected[str(name)] = str(value)
    return selected


def _ws_bytes(message: aiohttp.WSMessage) -> bytes | None:
    if message.type == WSMsgType.BINARY and isinstance(message.data, bytes):
        return message.data
    if message.type == WSMsgType.TEXT and isinstance(message.data, str):
        return message.data.encode("utf-8")
    return None
