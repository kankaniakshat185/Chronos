"""Virtual time, one seeded RNG, and a way to run real coroutines on them.

Every action in a run is an event on one priority queue, ordered by (time, sequence
number), so equal times break ties deterministically. Nothing reads the wall clock.

Real Chronos code is `async`. It runs here two ways:

* `run_sync(coro)`: for code that must not wait at all (the control plane's
  controllers and in-process client in full-sync mode). If it tries to suspend,
  that's a sim error, not a hang.
* `Task`: for code that does wait (the agent). Everything it can await, a network
  reply or `clock.sleep`, is a `SimFuture` resolved by a later event, and the task
  is resumed by an event too. So the agent's real coroutines interleave with
  everything else exactly as the RNG dictates.
"""

from __future__ import annotations

import hashlib
import heapq
import itertools
import random
from collections import deque
from collections.abc import Callable, Coroutine, Generator
from datetime import UTC, datetime, timedelta
from typing import Any

EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
MAX_EVENTS = 2_000_000  # a normal run is 10-40 thousand
MAX_EVENTS_AT_ONE_INSTANT = 100_000


class SimError(Exception):
    """A bug in the simulation itself (or code doing something the sim can't drive)."""


class SimFuture:
    """A one-shot result that a `Task` can await."""

    __slots__ = ("_callbacks", "_exc", "_value", "done")

    def __init__(self) -> None:
        self.done = False
        self._value: Any = None
        self._exc: BaseException | None = None
        self._callbacks: list[Callable[[], None]] = []

    def set_result(self, value: Any = None) -> None:
        if not self.done:
            self.done, self._value = True, value
            self._fire()

    def set_exception(self, exc: BaseException) -> None:
        if not self.done:
            self.done, self._exc = True, exc
            self._fire()

    def add_callback(self, fn: Callable[[], None]) -> None:
        if self.done:
            fn()
        else:
            self._callbacks.append(fn)

    def _fire(self) -> None:
        callbacks, self._callbacks = self._callbacks, []
        for fn in callbacks:
            fn()

    def __await__(self) -> Generator[SimFuture, None, Any]:
        if not self.done:
            yield self
        if self._exc is not None:
            raise self._exc
        return self._value


def run_sync[R](coro: Coroutine[Any, Any, R]) -> R:
    """Run a coroutine that must finish without waiting for anything."""
    try:
        coro.send(None)
    except StopIteration as done:
        return done.value  # type: ignore[no-any-return]
    coro.close()
    raise SimError(f"{coro!r} tried to wait; the sim drives it synchronously")


class Actor:
    """What owns tasks: an agent. Paused, its tasks don't run; dead, they never will."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.paused = False
        self.dead = False
        self.deferred: list[Callable[[], None]] = []
        self.tasks: list[Task] = []

    def run(self, fn: Callable[[], None]) -> None:
        """Run `fn` as this actor: later if it's paused, never if it's dead."""
        if self.dead:
            return
        if self.paused:
            self.deferred.append(fn)
        else:
            fn()

    def resume(self) -> list[Callable[[], None]]:
        self.paused = False
        deferred, self.deferred = self.deferred, []
        return deferred

    def kill(self) -> None:
        self.dead = True
        self.deferred.clear()
        for task in self.tasks:
            task.kill()


class Task:
    def __init__(self, sim: Sim, actor: Actor, coro: Coroutine[Any, Any, Any], name: str) -> None:
        self.sim, self.actor, self.coro, self.name = sim, actor, coro, name
        self.dead = False
        actor.tasks.append(self)
        sim.after(0.0, self._wake)

    def _wake(self) -> None:
        self.actor.run(self._step)

    def _step(self) -> None:
        if self.dead:
            return
        try:
            awaited = self.coro.send(None)
        except StopIteration:
            self.dead = True
            return
        except Exception as exc:
            self.dead = True
            raise SimError(f"{self.actor.name}: task {self.name} crashed: {exc!r}") from exc
        if not isinstance(awaited, SimFuture):
            self.kill()
            raise SimError(f"{self.actor.name}: task {self.name} awaited {awaited!r}")
        awaited.add_callback(lambda: self.sim.after(0.0, self._wake))

    def kill(self) -> None:
        if not self.dead:
            self.dead = True
            self.coro.close()


class Sim:
    def __init__(self, seed: int) -> None:
        self.rng = random.Random(seed)
        self.now = 0.0
        self.events = 0
        self._queue: list[tuple[float, int, Callable[[], None], str | None]] = []
        self._seq = itertools.count()
        self._hash = hashlib.sha256()
        self.tail: deque[str] = deque(maxlen=200)
        self.full_trace: list[str] | None = None  # set to a list to keep everything
        self.clock = SimClock(self)

    def at(self, when: float, fn: Callable[[], None], label: str | None = None) -> None:
        heapq.heappush(self._queue, (max(when, self.now), next(self._seq), fn, label))

    def after(self, delay: float, fn: Callable[[], None], label: str | None = None) -> None:
        self.at(self.now + delay, fn, label)

    def timer(self, delay: float) -> SimFuture:
        future = SimFuture()
        self.after(delay, future.set_result)
        return future

    def log(self, text: str) -> None:
        line = f"{self.now:10.4f} {text}"
        self.tail.append(line)
        self._hash.update(line.encode() + b"\n")
        if self.full_trace is not None:
            self.full_trace.append(line)

    @property
    def trace_hash(self) -> str:
        return self._hash.hexdigest()

    def run_until(self, until: float, after_each: Callable[[], None]) -> None:
        same_instant, last = 0, -1.0
        while self._queue and self._queue[0][0] <= until:
            if self._queue[0][0] == last:
                same_instant += 1
                if same_instant > MAX_EVENTS_AT_ONE_INSTANT:
                    raise SimError(f"livelock: {same_instant} events at t={last}; time stopped")
            else:
                same_instant, last = 0, self._queue[0][0]
            if self.events > MAX_EVENTS:
                raise SimError(f"runaway: more than {MAX_EVENTS} events")
            self.now, _, fn, label = heapq.heappop(self._queue)
            if label is not None:
                self.log(label)
            fn()
            self.events += 1
            after_each()
        self.now = max(self.now, until)


class SimClock:
    """The `Clock` protocol on virtual time."""

    def __init__(self, sim: Sim) -> None:
        self._sim = sim

    def monotonic(self) -> float:
        return self._sim.now

    def wall(self) -> datetime:
        return EPOCH + timedelta(seconds=self._sim.now)

    async def sleep(self, seconds: float) -> None:
        await self._sim.timer(max(seconds, 0.0))
