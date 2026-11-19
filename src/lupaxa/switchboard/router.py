"""Bounded, ordered delivery for router outputs."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from lupaxa.switchboard.config import DeliverySpec, EndpointSpec


class DeliveryError(Exception):
    """A delivery attempt failed. retry says whether another attempt is allowed."""

    def __init__(self, message: str, *, retry: str) -> None:
        super().__init__(message)
        self.retry = retry


@dataclass
class DeliveryResult:
    """The outcome of one output. Bodies are only kept for an upstream HTTP reply."""

    ok: bool
    status: int = 0
    body: bytes = b""
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class _Item:
    payload: bytes
    headers: dict[str, str]
    future: asyncio.Future[DeliveryResult]


Send = Callable[[EndpointSpec, bytes, dict[str, str]], Awaitable[DeliveryResult]]


class Lane:
    """One output. Messages leave in admission order."""

    def __init__(
        self,
        name: str,
        endpoint: EndpointSpec,
        delivery: DeliverySpec,
        send: Send,
    ) -> None:
        self.name = name
        self.endpoint = endpoint
        self.delivery = delivery
        self.send = send
        self.dropped = 0
        self.retries = 0
        self._items: deque[_Item] = deque()
        self._bytes = 0
        self._ready = asyncio.Event()

    def admit(
        self,
        payload: bytes,
        headers: dict[str, str],
    ) -> asyncio.Future[DeliveryResult] | None:
        """Queue a payload. None means the buffer refused it."""
        limit = self.delivery.buffer
        size = len(payload)
        if size > limit.max_bytes or not self.fits(size):
            self.dropped += 1
            return None
        while self._items and self._full(size) and limit.overflow == "drop_oldest":
            old = self._items.popleft()
            self._bytes -= len(old.payload)
            self.dropped += 1
            if not old.future.done():
                old.future.set_result(DeliveryResult(ok=False))
        loop = asyncio.get_running_loop()
        future: asyncio.Future[DeliveryResult] = loop.create_future()
        self._items.append(_Item(payload, dict(headers), future))
        self._bytes += size
        self._ready.set()
        return future

    async def run(self, stopping: Callable[[], bool]) -> None:
        """Deliver queued messages until the gateway stops."""
        try:
            while not stopping():
                if not self._items:
                    self._ready.clear()
                    if self._items:
                        continue
                    try:
                        await asyncio.wait_for(self._ready.wait(), 0.05)
                    except TimeoutError:
                        continue
                item = self._items.popleft()
                self._bytes -= len(item.payload)
                try:
                    result = await self._send_with_retry(item)
                except Exception:
                    result = DeliveryResult(ok=False)
                if not item.future.done():
                    item.future.set_result(result)
        finally:
            while self._items:
                item = self._items.popleft()
                self._bytes -= len(item.payload)
                if not item.future.done():
                    item.future.set_result(DeliveryResult(ok=False))

    @property
    def queued(self) -> int:
        """Messages waiting on this output."""
        return len(self._items)

    def fits(self, size: int) -> bool:
        """True when this payload can be queued without exceeding a reject limit."""
        limit = self.delivery.buffer
        if size > limit.max_bytes:
            return False
        if limit.overflow == "drop_oldest":
            return True
        return not self._full(size)

    def _full(self, size: int) -> bool:
        limit = self.delivery.buffer
        return len(self._items) >= limit.max_messages or self._bytes + size > limit.max_bytes

    async def _send_with_retry(self, item: _Item) -> DeliveryResult:
        retry = self.delivery.retry
        delay = retry.initial_delay_ms / 1000
        attempt = 0
        while True:
            try:
                return await self.send(self.endpoint, item.payload, item.headers)
            except DeliveryError as exc:
                allowed = exc.retry == "safe" or (exc.retry == "replay" and retry.replay)
                attempt += 1
                if not allowed or attempt > retry.max_attempts:
                    return DeliveryResult(ok=False)
            self.retries += 1
            await asyncio.sleep(delay)
            if retry.backoff == "exponential":
                delay = min(retry.max_delay_ms / 1000, delay * 2)
