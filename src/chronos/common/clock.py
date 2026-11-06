"""Injected time source (ADR-0006).

Every timeout, lease and tick in Chronos goes through a `Clock`. This module is
the only place allowed to read the system clock (enforced by ruff's banned-api
rule), so tests can drive time with `FakeClock` and the simulator with its own.
`monotonic()` is used for all durations; `wall()` is for display only.
"""

from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def wall(self) -> datetime: ...

    async def sleep(self, seconds: float) -> None: ...


class RealClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def wall(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class FakeClock:
    """A manually advanced clock. `sleep()` blocks until `advance()` passes its deadline."""

    def __init__(
        self,
        start: float = 0.0,
        wall_start: datetime = datetime(2026, 1, 1, tzinfo=UTC),
    ) -> None:
        self._start = start
        self._now = start
        self._wall_start = wall_start
        self._seq = itertools.count()
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []

    def monotonic(self) -> float:
        return self._now

    def wall(self) -> datetime:
        return self._wall_start + timedelta(seconds=self._now - self._start)

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        heapq.heappush(self._sleepers, (self._now + seconds, next(self._seq), fut))
        await fut

    def advance(self, seconds: float) -> None:
        """Move time forward and wake every sleeper whose deadline has passed, earliest first."""
        if seconds < 0:
            raise ValueError("time cannot go backwards")
        self._now += seconds
        while self._sleepers and self._sleepers[0][0] <= self._now:
            _, _, fut = heapq.heappop(self._sleepers)
            if not fut.done():
                fut.set_result(None)

    @property
    def pending_sleepers(self) -> int:
        return sum(1 for _, _, fut in self._sleepers if not fut.done())


def stopwatch() -> Callable[[], float]:
    """Start timing; the returned function gives the seconds elapsed (for metrics only:
    real time, even in tests and the simulator)."""
    start = time.perf_counter()
    return lambda: time.perf_counter() - start
