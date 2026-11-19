"""Local management API.

The control connection is a Unix socket, or loopback TCP with a token.
Requests and responses are one JSON object per line. Payloads, headers, and
tokens are not written to the log.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import stat
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import platformdirs

from lupaxa.switchboard.config import SwitchboardConfig
from lupaxa.switchboard.exceptions import SwitchboardError

_MAX_LINE = 65536
Handler = Callable[[dict[str, object]], Awaitable[object]]


@dataclass
class Endpoint:
    """Where a management client connects."""

    transport: str
    path: str = ""
    host: str = "127.0.0.1"
    port: int = 0
    token: str | None = None


class Server:
    """Accept management clients for one running gateway."""

    def __init__(self, endpoint: Endpoint, handle: Handler) -> None:
        self.endpoint = endpoint
        self._handle = handle
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        """Bind the control socket."""
        if self.endpoint.transport == "local":
            path = Path(self.endpoint.path)
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists() and not await _reachable(self.endpoint):
                path.unlink()
            elif path.exists():
                raise SwitchboardError("Management socket is already in use.")
            self._server = await asyncio.start_unix_server(self._client, path)
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
            return
        self._server = await asyncio.start_server(
            self._client,
            self.endpoint.host,
            self.endpoint.port,
            reuse_address=False,
        )
        names = self._server.sockets
        if names:
            self.endpoint.port = int(names[0].getsockname()[1])

    async def close(self) -> None:
        """Stop accepting control connections and remove a Unix socket."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        if self.endpoint.transport == "local":
            path = Path(self.endpoint.path)
            if path.exists():
                path.unlink()

    async def _client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                if len(line) > _MAX_LINE:
                    self._write(writer, {"ok": False, "error": "management request is too large"})
                    return
                self._write(writer, await self._dispatch(line))
                await writer.drain()
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            return
        finally:
            writer.close()

    async def _dispatch(self, line: bytes) -> dict[str, object]:
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            return {"version": 1, "ok": False, "error": "management request is invalid"}
        if not isinstance(request, dict):
            return {"version": 1, "ok": False, "error": "management request is invalid"}
        identity = request.get("id")
        method = request.get("method")
        params = request.get("params", {})
        version_ok = request.get("version") == 1
        params_ok = isinstance(method, str) and isinstance(params, dict)
        if not version_ok or not params_ok:
            return _reply(identity, ok=False, error="management request is invalid")
        supplied = params.get("token") if isinstance(params, dict) else None
        if self.endpoint.transport == "tcp" and not _token_ok(self.endpoint.token, supplied):
            return _reply(identity, ok=False, error="management request was rejected")
        try:
            result = await self._handle({"method": method, "params": params})
        except SwitchboardError as exc:
            return _reply(identity, ok=False, error=str(exc))
        except Exception:
            return _reply(identity, ok=False, error="management request failed")
        return _reply(identity, ok=True, result=result)

    def _write(self, writer: asyncio.StreamWriter, payload: dict[str, object]) -> None:
        writer.write(json.dumps(payload, separators=(",", ":")).encode("utf-8") + b"\n")


def management_endpoint(config: SwitchboardConfig) -> Endpoint | None:
    """The control address from configuration. None when management is off."""
    spec = config.management
    if not spec.enabled:
        return None
    if spec.transport == "local":
        path = spec.path or str(_default_socket())
        return Endpoint(transport="local", path=path)
    return Endpoint(
        transport="tcp",
        host=spec.host,
        port=spec.port,
        token=spec.token,
    )


async def request(
    endpoint: Endpoint,
    method: str,
    params: dict[str, object] | None = None,
) -> object:
    """Send one management request and return the result."""
    values = dict(params or {})
    if endpoint.transport == "tcp":
        values["token"] = endpoint.token or ""
    body = {"version": 1, "id": "1", "method": method, "params": values}
    if endpoint.transport == "tcp":
        reader, writer = await asyncio.open_connection(endpoint.host, endpoint.port)
    else:
        reader, writer = await asyncio.open_unix_connection(endpoint.path)
    try:
        writer.write(json.dumps(body, separators=(",", ":")).encode("utf-8") + b"\n")
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), 5)
    finally:
        writer.close()
    if not line:
        raise SwitchboardError("Management connection closed.")
    try:
        payload = json.loads(line)
    except json.JSONDecodeError as exc:
        raise SwitchboardError("Management response is invalid.") from exc
    if not isinstance(payload, dict) or not payload.get("ok"):
        message = payload.get("error") if isinstance(payload, dict) else None
        raise SwitchboardError(str(message or "Management request failed."))
    return payload.get("result")


def call(endpoint: Endpoint, method: str, params: dict[str, object] | None = None) -> object:
    """Synchronous management call for the command line."""
    try:
        return asyncio.run(request(endpoint, method, params))
    except OSError as exc:
        raise SwitchboardError("Switchboard is not running.") from exc


def _reply(
    identity: object,
    *,
    ok: bool,
    result: object = None,
    error: str = "",
) -> dict[str, object]:
    reply_id = identity if isinstance(identity, str) else ""
    payload: dict[str, object] = {"version": 1, "id": reply_id, "ok": ok}
    if ok:
        payload["result"] = result
    else:
        payload["error"] = error
    return payload


def _token_ok(expected: str | None, given: object) -> bool:
    if not expected or not isinstance(given, str):
        return False
    left = hashlib.sha256(expected.encode("utf-8")).digest()
    right = hashlib.sha256(given.encode("utf-8")).digest()
    return hmac.compare_digest(left, right)


def _default_socket() -> Path:
    return Path(platformdirs.user_runtime_dir("switchboard", "Lupaxa")) / "switchboard.sock"


async def _reachable(endpoint: Endpoint) -> bool:
    try:
        if endpoint.transport == "local":
            opened = asyncio.open_unix_connection(endpoint.path)
            _reader, writer = await asyncio.wait_for(opened, 0.2)
        else:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(endpoint.host, endpoint.port),
                0.2,
            )
    except (ConnectionError, OSError, TimeoutError):
        return False
    writer.close()
    return True
