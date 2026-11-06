"""Controller runtime (ADR-0009): informers, a work queue, expectations, a registry (X-6).

The client-go design, small:

* `Informer`: list once, then long-poll watch from the list's revision (ADR-0008),
  keeping a local cache and calling handlers with (old, new) for every change. On
  `Compacted` (410) it relists: it never guesses what it missed (S6). A relist
  calls the handlers for the difference, so consumers can't tell it apart from
  having seen every event.
* `WorkQueue`: keys, not events. A key is queued at most once (dedup), handed to
  at most one worker at a time, and a key re-added while it's being processed is
  queued again when it's done. Failures back off per key.
* `Controller`: handlers map changes to keys; one worker takes keys and runs
  `reconcile(key)`, which reads the current state of that key from the caches and
  computes the whole difference (level-triggered). Every `resync_period` every key
  is queued again, as a safety net for anything a handler didn't map.
* `Expectations`: what a controller has just created or deleted and not yet seen in
  its cache. While a key has unmet expectations its reconcile waits, so a stale
  cache can't make it create the same pods twice (ADR-0009 3.C).

Cached objects are shared: `get`, `list` and `by_index` return copies, and handlers
must treat what they're given as read-only.
"""

from __future__ import annotations

import asyncio
import builtins
import logging
import random
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass
from typing import Any, ClassVar, cast

from chronos.api.meta import ApiObject
from chronos.client.local import LocalStoreClient
from chronos.client.protocol import WATCH_TIMEOUT, StoreClient
from chronos.common.backoff import exponential, full_jitter
from chronos.common.clock import Clock
from chronos.common.config import Timings
from chronos.store.errors import Compacted, StoreError
from chronos.store.kinds import NODE_INDEX, info

log = logging.getLogger(__name__)


class Signal:
    """A wake-up that isn't lost: `notify()` before `wait()` makes the wait return at once."""

    def __init__(self) -> None:
        self._pending = False
        self._waiters: list[asyncio.Future[None]] = []

    def notify(self) -> None:
        self._pending = True
        waiting, self._waiters = self._waiters, []
        for future in waiting:
            if not future.done():
                future.set_result(None)

    async def wait(self, clock: Clock | None = None, at_most: float | None = None) -> None:
        """Until notified, or until `at_most` seconds pass on `clock`."""
        if not self._pending:
            future = asyncio.get_running_loop().create_future()
            self._waiters.append(future)
            timer = (
                asyncio.ensure_future(clock.sleep(at_most))
                if clock is not None and at_most is not None
                else None
            )
            try:
                await asyncio.wait(
                    {future, *([timer] if timer else [])}, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                future.cancel()
                if timer is not None:
                    timer.cancel()
        self._pending = False


# ---- informer ------------------------------------------------------------------------

type Handler[T] = Callable[[T | None, T | None], None]  # (old, new); new is None: gone
type IndexKey[T] = Callable[[T], str | None]  # None: not in the index


class Informer[T: ApiObject]:
    def __init__(
        self,
        client: StoreClient,
        kind: type[T],
        clock: Clock,
        *,
        node_name: str | None = None,
        watch_wait: float = WATCH_TIMEOUT,
        retry_base: float = 0.1,
        retry_cap: float = 2.0,
    ) -> None:
        self.kind = kind
        self.revision = 0
        self.has_synced = False
        self.relists = 0
        self._client = client
        self._clock = clock
        self._node = node_name
        self._node_key = info(kind).indexes.get(NODE_INDEX) if node_name is not None else None
        self._wait = watch_wait
        self._retry = (retry_base, retry_cap)
        self._rng = random.Random()
        self._items: dict[str, T] = {}
        self._handlers: list[Handler[T]] = []
        self._indexes: dict[str, tuple[IndexKey[T], dict[str, set[str]]]] = {}
        self._synced: list[asyncio.Future[None]] = []
        self.needs_relist = True

    # ---- consumers

    def add_handler(self, handler: Handler[T]) -> None:
        self._handlers.append(handler)

    def add_index(self, name: str, key: IndexKey[T]) -> None:
        if name in self._indexes:
            return  # shared informers: every controller asks for the indexes it needs
        keys: dict[str, set[str]] = {}
        for obj in self._items.values():
            if (k := key(obj)) is not None:
                keys.setdefault(k, set()).add(obj.metadata.name)
        self._indexes[name] = (key, keys)

    def get(self, name: str) -> T | None:
        obj = self._items.get(name)
        return obj.model_copy(deep=True) if obj is not None else None

    def list(self) -> builtins.list[T]:
        return [self._items[n].model_copy(deep=True) for n in sorted(self._items)]

    def by_index(self, index: str, key: str) -> builtins.list[T]:
        names = self._indexes[index][1].get(key, set())
        return [self._items[n].model_copy(deep=True) for n in sorted(names)]

    def keys(self) -> builtins.list[str]:
        return sorted(self._items)

    async def wait_synced(self) -> None:
        if not self.has_synced:
            future = asyncio.get_running_loop().create_future()
            self._synced.append(future)
            await future

    # ---- the loop

    async def run(self) -> None:
        """List, then watch forever; relist on Compacted, back off while unreachable."""
        attempt = 0
        while True:
            try:
                await self.step()
                attempt = 0
            except StoreError as exc:  # Unavailable, mostly: retry from the same revision
                delay = full_jitter(attempt, *self._retry, self._rng)
                log.info("%s informer: %s; retrying in %.2fs", self.kind.__name__, exc, delay)
                attempt += 1
                await self._clock.sleep(delay)

    async def step(self) -> None:
        """One round: a (re)list if one is due, else one watch. After Compacted the next
        round relists: the cache is never patched over a gap (S6)."""
        if self.needs_relist:
            await self.relist()
            return
        try:
            await self.watch_once()
        except Compacted as exc:
            log.info("%s informer: %s", self.kind.__name__, exc)
            self.needs_relist = True

    async def relist(self) -> None:
        items, revision = await self._client.list(self.kind, node_name=self._node)
        fresh = {o.metadata.name: o for o in items}
        for name in sorted(self._items.keys() - fresh.keys()):
            self._remove(name)
        for name, obj in sorted(fresh.items()):
            old = self._items.get(name)
            if old is None or old.metadata.resource_version != obj.metadata.resource_version:
                self._put(obj)
        self.revision = revision
        self.relists += 1
        self.needs_relist = False
        if not self.has_synced:
            self.has_synced = True
            waiting, self._synced = self._synced, []
            for future in waiting:
                if not future.done():
                    future.set_result(None)

    async def watch_once(self) -> None:
        batch = await self._client.watch(
            self.kind, self.revision, wait=self._wait, node_name=self._node
        )
        for event in batch.events:
            obj = event.object
            current = self._items.get(obj.metadata.name)
            if current is not None and obj.metadata.resource_version <= (
                current.metadata.resource_version
            ):
                continue  # already in the cache (e.g. from the relist)
            if event.type == "DELETED" or not self._wanted(obj):
                self._remove(obj.metadata.name)
            else:
                self._put(obj)
        self.revision = max(self.revision, batch.revision)

    def _wanted(self, obj: T) -> bool:
        return self._node_key is None or self._node_key(obj) == self._node

    def _put(self, obj: T) -> None:
        name = obj.metadata.name
        old = self._items.get(name)
        self._items[name] = obj
        for key, keys in self._indexes.values():
            if old is not None and (k := key(old)) is not None:
                keys.get(k, set()).discard(name)
            if (k := key(obj)) is not None:
                keys.setdefault(k, set()).add(name)
        for handler in self._handlers:
            handler(old, obj)

    def _remove(self, name: str) -> None:
        old = self._items.pop(name, None)
        if old is None:
            return
        for key, keys in self._indexes.values():
            if (k := key(old)) is not None:
                keys.get(k, set()).discard(name)
        for handler in self._handlers:
            handler(old, None)


class Informers:
    """One shared informer per kind (like client-go's SharedInformerFactory)."""

    def __init__(self, client: StoreClient, clock: Clock, **options: Any) -> None:
        self._client = client
        self._clock = clock
        self._options = options
        self._by_kind: dict[type[ApiObject], Informer[Any]] = {}

    def get[T: ApiObject](self, kind: type[T]) -> Informer[T]:
        if kind not in self._by_kind:
            self._by_kind[kind] = Informer(self._client, kind, self._clock, **self._options)
        return cast(Informer[T], self._by_kind[kind])

    async def run(self) -> None:
        async with asyncio.TaskGroup() as tasks:
            for informer in self._by_kind.values():
                tasks.create_task(informer.run())


# ---- work queue ----------------------------------------------------------------------


class WorkQueue:
    def __init__(self, clock: Clock, *, base_delay: float = 0.005, max_delay: float = 30.0) -> None:
        self._clock = clock
        self._delays = (base_delay, max_delay)
        self._queue: deque[str] = deque()
        self._queued: set[str] = set()
        self._processing: set[str] = set()
        self._dirty: set[str] = set()  # re-added while processing: queue again when done
        self._failures: dict[str, int] = {}
        self._ready = Signal()
        self._timers: set[asyncio.Task[None]] = set()

    def __len__(self) -> int:
        return len(self._queue)

    def add(self, key: str) -> None:
        if key in self._processing:
            self._dirty.add(key)
        elif key not in self._queued:
            self._queued.add(key)
            self._queue.append(key)
            self._ready.notify()

    def add_after(self, key: str, delay: float) -> None:
        if delay <= 0:
            self.add(key)
            return

        async def later() -> None:
            await self._clock.sleep(delay)
            self.add(key)

        task = asyncio.create_task(later())
        self._timers.add(task)
        task.add_done_callback(self._timers.discard)

    def add_rate_limited(self, key: str) -> None:
        """Back off per key: base, 2·base, 4·base, ... up to max_delay."""
        attempt = self._failures.get(key, 0)
        self._failures[key] = attempt + 1
        self.add_after(key, exponential(attempt, *self._delays))

    def failures(self, key: str) -> int:
        return self._failures.get(key, 0)

    def forget(self, key: str) -> None:
        """The key succeeded: its next failure starts the backoff over."""
        self._failures.pop(key, None)

    async def get(self) -> str:
        while not self._queue:
            await self._ready.wait()
        key = self._queue.popleft()
        self._queued.discard(key)
        self._processing.add(key)
        return key

    def done(self, key: str) -> None:
        self._processing.discard(key)
        if key in self._dirty:
            self._dirty.discard(key)
            self.add(key)

    def close(self) -> None:
        for task in self._timers:
            task.cancel()


# ---- expectations --------------------------------------------------------------------


class Expectations:
    """Per key: how many creates, and which deletes, are still to be seen (ADR-0009).

    Met when everything expected was observed, or after `ttl` (an event that was
    really lost: the next reconcile, or resync, puts things right).
    """

    def __init__(self, clock: Clock, ttl: float = 30.0) -> None:
        self._clock = clock
        self._ttl = ttl
        self._creates: dict[str, int] = {}
        self._deletes: dict[str, set[str]] = {}
        self._since: dict[str, float] = {}
        self.expired = 0

    def expect(self, key: str, creates: int = 0, deletes: Iterable[str] = ()) -> None:
        names = set(deletes)
        if creates <= 0 and not names:
            return
        self._creates[key] = self._creates.get(key, 0) + creates
        self._deletes.setdefault(key, set()).update(names)
        self._since[key] = self._clock.monotonic()

    def created(self, key: str) -> None:
        if key in self._creates:
            self._creates[key] -= 1

    def deleted(self, key: str, name: str) -> None:
        if key in self._deletes:
            self._deletes[key].discard(name)

    def pending(self, key: str) -> tuple[int, int]:
        return max(self._creates.get(key, 0), 0), len(self._deletes.get(key, ()))

    def satisfied(self, key: str) -> bool:
        if key not in self._since:
            return True
        if self.pending(key) == (0, 0):
            self.forget(key)
            return True
        if self._clock.monotonic() - self._since[key] > self._ttl:
            log.warning("expectations for %s expired with %s still unseen", key, self.pending(key))
            self.expired += 1
            self.forget(key)
            return True
        return False

    def forget(self, key: str) -> None:
        self._creates.pop(key, None)
        self._deletes.pop(key, None)
        self._since.pop(key, None)


# ---- controllers ---------------------------------------------------------------------


class Controller(ABC):
    """Subclasses map informer changes to keys and implement `reconcile` and `keys`."""

    name: ClassVar[str]

    def __init__(
        self, clock: Clock, informers: Iterable[Informer[Any]], *, resync_period: float
    ) -> None:
        self.clock = clock
        self.queue = WorkQueue(clock)
        self.resync_period = resync_period
        self._informers = list(informers)

    @abstractmethod
    async def reconcile(self, key: str) -> float | None:
        """Make `key` right from the cached state. Return a delay to look again after it.

        A StoreError (e.g. a Conflict from a stale cache) requeues the key with backoff.
        """

    @abstractmethod
    def keys(self) -> Iterable[str]:
        """Every key, for resync."""

    def resync(self) -> None:
        for key in self.keys():
            self.queue.add(key)

    async def process_one(self) -> None:
        key = await self.queue.get()
        try:
            after = await self.reconcile(key)
        except StoreError as exc:
            log.info("%s %s: requeued: %s", self.name, key, exc)
            self.queue.add_rate_limited(key)
        except Exception:
            log.exception("%s %s: reconcile failed", self.name, key)
            self.queue.add_rate_limited(key)
        else:
            self.queue.forget(key)
            if after is not None:
                self.queue.add_after(key, after)
        finally:
            self.queue.done(key)

    async def run(self) -> None:
        """Wait for the caches, then work and resync forever (the informers run elsewhere)."""
        for informer in self._informers:
            await informer.wait_synced()
        self.resync()
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._work())
                tasks.create_task(self._resync_forever())
        finally:
            self.queue.close()

    async def run_once(self) -> None:
        """One full pass (tests): refresh the caches, queue every key, drain the queue."""
        for informer in self._informers:
            await informer.relist()
        self.resync()
        while len(self.queue):
            await self.process_one()

    async def _work(self) -> None:
        while True:
            await self.process_one()

    async def _resync_forever(self) -> None:
        while True:
            await self.clock.sleep(self.resync_period)
            self.resync()


@dataclass(frozen=True)
class ControllerContext:
    """What a controller factory gets from `chronosd`."""

    client: LocalStoreClient
    informers: Informers
    clock: Clock
    timings: Timings


type ControllerFactory = Callable[[ControllerContext], Controller]

CONTROLLERS: dict[str, ControllerFactory] = {}


def register_controller(name: str, factory: ControllerFactory) -> None:
    if name in CONTROLLERS:
        raise ValueError(f"controller {name!r} is already registered")
    CONTROLLERS[name] = factory


def start_all(ctx: ControllerContext) -> list[Callable[[], Coroutine[None, None, None]]]:
    """Build every registered controller; return the loops to run (informers first)."""
    controllers = [factory(ctx) for factory in CONTROLLERS.values()]
    return [ctx.informers.run, *(c.run for c in controllers)]
