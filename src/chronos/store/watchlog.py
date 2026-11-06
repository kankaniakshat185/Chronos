"""Watch log: the last N changes, for `GET /watch/...?since=` (ADR-0008, S6).

Every write bumps the revision by exactly one and appends exactly one event, so the
history is contiguous: the event for revision r sits in slot `(r - 1) % capacity`,
and no search is needed. The log knows every event after `floor` (the revision it
started at, or the newest one evicted). Asking for anything older raises
`Compacted`: the caller relists. It never answers with a partial history, which is
what makes "a watch never silently skips" (S6) true.

After a restart the log is empty and `floor` is the recovered revision, so every
watcher from before the restart gets `Compacted` and relists (ADR-0008 4).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from chronos.api.meta import ApiObject
from chronos.store.errors import Compacted

type EventType = Literal["ADDED", "MODIFIED", "DELETED"]

DEFAULT_HISTORY = 10_000


@dataclass(frozen=True)
class Event:
    type: EventType
    revision: int
    object: ApiObject  # as stored; for DELETED, the last state (resource_version = revision)
    old: ApiObject | None = None  # the previous state, for filters that match either


class WatchLog:
    def __init__(self, capacity: int = DEFAULT_HISTORY, floor: int = 0) -> None:
        if capacity < 1:
            raise ValueError("watch history must hold at least one event")
        self._capacity = capacity
        self._slots: list[Event | None] = [None] * capacity
        self._waiters: list[asyncio.Future[None]] = []
        self.reset(floor)

    def reset(self, floor: int) -> None:
        """Forget everything: the next event must be `floor + 1`."""
        self._slots = [None] * self._capacity
        self._floor = floor
        self._latest = floor

    @property
    def floor(self) -> int:
        """Every event after this revision is in the log."""
        return self._floor

    @property
    def latest(self) -> int:
        return self._latest

    def append(self, event: Event) -> None:
        if event.revision != self._latest + 1:
            raise AssertionError(f"watch log gap: {self._latest} -> {event.revision}")
        self._slots[(event.revision - 1) % self._capacity] = event
        self._latest = event.revision
        self._floor = max(self._floor, self._latest - self._capacity)
        waiting, self._waiters = self._waiters, []
        for future in waiting:
            if not future.done():
                future.set_result(None)

    def since(
        self, revision: int, match: Callable[[Event], bool], limit: int | None = None
    ) -> list[Event]:
        """Matching events after `revision`, oldest first, at most `limit` of them."""
        if revision < self._floor:
            raise Compacted(
                f"revision {revision} is older than the watch history (oldest {self._floor}); "
                "relist"
            )
        if revision > self._latest:
            # A store restored from an older state (only possible with --wal-sync none):
            # revision numbers are being reused, so the caller's position means nothing.
            raise Compacted(f"revision {revision} is newer than the store ({self._latest}); relist")
        out = []
        for rev in range(revision + 1, self._latest + 1):
            event = self._slots[(rev - 1) % self._capacity]
            assert event is not None and event.revision == rev
            if match(event):
                out.append(event)
                if limit is not None and len(out) >= limit:
                    break
        return out

    def next_append(self) -> asyncio.Future[None]:
        """A future that completes on the next append.

        Registered before this returns, so a caller that checks `since()` and then
        calls this, with no await in between, can't miss a write that lands in the
        gap (a lost wakeup would leave a watch asleep until its timeout).
        """
        future = asyncio.get_running_loop().create_future()
        self._waiters.append(future)
        return future
