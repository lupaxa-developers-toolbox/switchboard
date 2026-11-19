"""Optional text interface. It talks only to the management socket."""

from __future__ import annotations

from typing import Protocol

from lupaxa.switchboard.exceptions import SwitchboardError
from lupaxa.switchboard.manage import Endpoint, request

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.widgets import DataTable, Footer, Header, Static
except ImportError as exc:  # pragma: no cover - the extra is optional
    raise ImportError("textual is not installed") from exc


class Client(Protocol):
    """The calls the interface is allowed to make."""

    async def call(self, method: str, params: dict[str, object] | None = None) -> object:
        """Run one management method."""


class SocketClient:
    """Management client used by the running interface."""

    def __init__(self, endpoint: Endpoint) -> None:
        self.endpoint = endpoint

    async def call(self, method: str, params: dict[str, object] | None = None) -> object:
        return await request(self.endpoint, method, params)


class Dashboard(App[None]):
    """Gateway overview driven by management responses."""

    BINDINGS = [
        Binding("q", "quit", "Quit"),
        Binding("b", "screen('bridges')", "Bridges"),
        Binding("c", "screen('connections')", "Connections"),
        Binding("t", "screen('trace')", "Trace"),
        Binding("p", "screen('profiles')", "Profiles"),
        Binding("l", "screen('logs')", "Logs"),
        Binding("v", "screen('validation')", "Validation"),
    ]

    def __init__(self, client: Client) -> None:
        super().__init__()
        self.client = client
        self.screen_name = "bridges"
        self.connected = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield Static("Not connected", id="state")
        yield DataTable(id="rows")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_columns("Name", "Detail", "State")
        self.run_worker(self._refresh())
        self.set_interval(1, self._refresh)

    async def _refresh(self) -> None:
        table = self.query_one(DataTable)
        state = self.query_one("#state", Static)
        try:
            rows = await self._rows()
        except (SwitchboardError, OSError, TimeoutError):
            self.connected = False
            state.update("Not connected")
            table.clear()
            return
        self.connected = True
        state.update(f"Connected  {self.screen_name}")
        table.clear()
        for row in rows:
            table.add_row(*row)

    async def _rows(self) -> list[tuple[str, str, str]]:
        if self.screen_name == "connections":
            payload = await self.client.call("connections.list")
            items = _list(payload, "connections")
            return [
                (str(item.get("id")), str(item.get("bridge")), str(item.get("state")))
                for item in items
            ]
        if self.screen_name == "trace":
            payload = await self.client.call("events")
            items = _list(payload, "events")
            return [
                (str(item.get("bridge")), str(item.get("stage")), str(item.get("outcome")))
                for item in items
            ]
        if self.screen_name == "profiles":
            payload = await self.client.call("profiles")
            items = _list(payload, "profiles")
            return [(str(item.get("name")), str(item.get("transport")), "") for item in items]
        if self.screen_name == "logs":
            payload = await self.client.call("events")
            items = _list(payload, "events")
            errors = [item for item in items if item.get("outcome") != "ok"]
            return [
                (str(item.get("time")), str(item.get("stage")), str(item.get("detail")))
                for item in errors
            ]
        if self.screen_name == "validation":
            payload = await self.client.call("config.validate")
            ok = isinstance(payload, dict) and payload.get("ok") is True
            return [("configuration", "reload source", "pass" if ok else "fail")]
        payload = await self.client.call("status")
        items = _list(payload, "bridges")
        return [
            (
                str(item.get("name")),
                str(item.get("input")),
                "running" if item.get("listening") else "stopped",
            )
            for item in items
        ]

    def action_screen(self, name: str) -> None:
        self.screen_name = name
        self.run_worker(self._refresh())


def run_tui(endpoint: Endpoint) -> None:
    """Run the interface until the operator quits."""
    Dashboard(SocketClient(endpoint)).run()


def _list(payload: object, key: str) -> list[dict[str, object]]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get(key)
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]
