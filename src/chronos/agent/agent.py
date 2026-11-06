"""The node agent: register, heartbeat, fence, and keep local processes in sync.

Three independent loops (ADR-0005, ADR-0011, ADR-0012), so no loop can delay another:

* heartbeat, every `heartbeat_interval`: on success, `last_ok_send` advances to the
  **send** time of that heartbeat and is written to the lease file the shims read.
  If the server no longer recognizes this incarnation (EpochMismatch, e.g. after the
  node went NotReady) or the node is gone (NotFound), re-register with a new epoch.
* fence watch: once `fence_timeout` has passed since `last_ok_send`, enter Fenced
  mode: stop every workload and start nothing. Leave it only when a heartbeat is
  accepted again. This runs on its own because the heartbeat loop can sit in a
  register retry while the server is down, which is exactly when fencing matters.
* pod informer: watches `pods?node=<self>` (ADR-0008) into a local cache; relists
  on Compacted, e.g. after the control plane restarts.
* sync: plan with `core.agent_sync` from the cache and the local processes, carry the
  actions out. It runs whenever the cache changes or a local process exits, and at
  least every `resync_interval` (crash-loop backoffs expire with no event at all).
  Failed writes need no bookkeeping: the plan is recomputed from scratch each time.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import uuid
from collections.abc import Awaitable, Callable, Mapping

from chronos.agent.providers import ProviderError
from chronos.agent.runtime import ProcessRuntime
from chronos.api import Node, ObjectMeta, Pod, PodPhase
from chronos.client.events import EventRecorder
from chronos.client.protocol import StoreClient
from chronos.common.backoff import full_jitter
from chronos.common.clock import Clock
from chronos.common.config import FencingConfig
from chronos.common.metrics import AGENT_FENCE_EVENTS, AGENT_HEARTBEAT_FAILURES
from chronos.controllers.runtime import Informer, Signal
from chronos.core.agent_sync import CRASH_LOOP, Backoff, Finalize, Forget, Report, Start, Stop, plan
from chronos.core.fencing import should_fence
from chronos.store import Invalid, NotFound, StoreError
from chronos.store.errors import EpochMismatch, Unavailable

log = logging.getLogger(__name__)


class AgentRejected(Exception):
    """The server refused this agent (e.g. its fencing timings are unsafe there)."""


class Agent:
    def __init__(
        self,
        *,
        name: str,
        capacity: Mapping[str, int],
        client: StoreClient,
        runtime: ProcessRuntime,
        clock: Clock,
        fencing: FencingConfig,
        resync_interval: float = 1.0,
        backoff: Backoff | None = None,
        watch_wait: float = 30.0,
        rng: random.Random | None = None,
        new_epoch: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        self.name = name
        self.epoch = ""
        self.fenced = False
        self.registered_at = 0  # the store revision of the latest registration, see sync_once
        self.last_ok_send = -math.inf
        self._capacity = dict(capacity)
        self._client = client
        self._runtime = runtime
        self._clock = clock
        self._fencing = fencing
        self._resync_interval = resync_interval
        self._backoff = backoff or Backoff()
        self._rng = rng or random.Random()
        self._new_epoch = new_epoch
        self.events = EventRecorder(client, clock, f"agent/{name}")
        self.stats: dict[str, int] = {}  # for /metrics (read on another thread)
        self._fenced_at = 0.0
        self.pods = Informer(
            client, Pod, clock, node_name=name, watch_wait=watch_wait, rng=self._rng
        )
        self._wake = Signal()
        self.pods.add_handler(lambda _old, _new: self._wake.notify())
        runtime.on_change = self._wake.notify

    async def register(self) -> None:
        """Register under a new epoch, retrying (with jittered backoff) until answered."""
        epoch = self._new_epoch()
        attempt = 0
        while True:
            sent = self._clock.monotonic()
            try:
                reply = await self._client.register(self.name, epoch, self._capacity, self._fencing)
            except Unavailable as exc:
                delay = full_jitter(attempt, self._fencing.heartbeat_interval, 5.0, self._rng)
                log.warning("register: %s; retrying in %.1fs", exc, delay)
                attempt += 1
                await self._clock.sleep(delay)
                continue
            except Invalid as exc:
                raise AgentRejected(str(exc)) from None
            self._runtime.eviction_timeout = reply.eviction_timeout
            self.epoch = epoch
            self.registered_at = reply.node.metadata.resource_version
            self._accepted(sent)
            log.info("registered node %s (epoch %s)", self.name, epoch[:8])
            return

    def _accepted(self, sent: float) -> None:
        # Coming back from beyond E (e.g. the whole node was frozen): the server may
        # already have replaced these workloads. Kill them *before* the lease looks
        # fresh, or the shims would see the new lease and let them run (found by the
        # simulator: a re-register within one shim poll of a thaw beat the shim).
        self._kill_if_past_eviction()
        self.last_ok_send = max(self.last_ok_send, sent)
        self._runtime.write_lease(self.last_ok_send)

    def _kill_if_past_eviction(self) -> None:
        """The shim's second rule, applied by the agent too: a lease older than E means
        replacements may be running, so no grace period: SIGKILL now."""
        evict = self._runtime.eviction_timeout
        if evict is None or self._clock.monotonic() - self.last_ok_send <= evict:
            return
        for uid, proc in self._runtime.snapshot().items():
            if proc.running:
                log.warning("pod %s: lease older than eviction timeout: SIGKILL", proc.pod_name)
                self._runtime.kill_now(uid)

    async def heartbeat_once(self) -> None:
        sent = self._clock.monotonic()  # the SEND time is what fencing may rely on
        try:
            await self._client.heartbeat(self.name, self.epoch)
        except (EpochMismatch, NotFound) as exc:
            log.warning("heartbeat refused (%s); re-registering", exc)
            await self.register()
            return
        except StoreError as exc:  # Unavailable, timeouts: keep beating
            AGENT_HEARTBEAT_FAILURES.inc()
            log.warning("heartbeat failed: %s", exc)
            return
        self._accepted(sent)

    async def fence_check(self) -> None:
        self._kill_if_past_eviction()
        stale = should_fence(
            self.last_ok_send, self._clock.monotonic(), self._fencing.fence_timeout
        )
        if stale and not self.fenced:
            self.fenced = True
            self._fenced_at = self._clock.monotonic()
            AGENT_FENCE_EVENTS.inc()
            log.warning("no accepted heartbeat for %.1fs: Fenced, stopping all workloads",
                        self._clock.monotonic() - self.last_ok_send)  # fmt: skip
            for uid, proc in self._runtime.snapshot().items():
                if proc.running:
                    self._runtime.stop(uid, fenced=True)
        elif not stale and self.fenced:
            self.fenced = False
            log.info("heartbeat accepted again: leaving Fenced")
            # Recorded now: while fenced, the control plane was unreachable by definition.
            took = self._clock.monotonic() - self._fenced_at
            node = Node(metadata=ObjectMeta(name=self.name))
            await self.events.record(
                node, "Fenced", f"stopped all workloads; fenced for {took:.1f}s", "Warning"
            )

    async def _caught_up_with_registration(self) -> bool:
        """A new epoch means the server may have evicted everything the old one ran, and
        the watch may still be catching up. Act on nothing older than this registration
        (found by the simulator: a resumed agent restarted evicted pods from its stale
        cache, next to their replacements)."""
        if self.pods.revision >= self.registered_at:
            return True
        try:
            await self.pods.relist()
        except StoreError as exc:
            log.warning("sync: can't list pods after registering: %s", exc)
            return False
        return self.pods.revision >= self.registered_at

    async def sync_once(self) -> None:
        if not self.pods.has_synced:
            return  # an empty cache would mean "stop everything"
        if not await self._caught_up_with_registration():
            return
        pods = self.pods.list()
        await self.fence_check()  # never start anything on a stale lease
        procs = self._runtime.snapshot()
        running = sum(p.running for p in procs.values())
        self.stats = {"running": running, "exited": len(procs) - running}  # replaced, not mutated
        now = self._clock.monotonic()
        for action in plan(self.name, pods, self._runtime.snapshot(), now, self._backoff):
            match action:
                case Start(pod=pod, restarts=restarts, crashes=crashes) if not self.fenced:
                    await self._start(pod, restarts, crashes)
                case Stop(uid=uid):
                    self._runtime.stop(uid)
                case Forget(uid=uid):
                    self._runtime.forget(uid)
                case Finalize(name=name, resource_version=rv):
                    await self._finalize(name, rv)
                case Report(pod=pod) if not self.fenced:
                    await self._report(pod)
                case _:
                    pass  # Fenced: start and report nothing

    async def _start(self, pod: Pod, restarts: int, crashes: int) -> None:
        try:
            await self._runtime.start(pod, restarts=restarts, crashes=crashes)
        except ProviderError as exc:
            await self._fail(pod, "ProviderError", str(exc))
            return
        await self.events.record(pod, "Started", f"on {self.name}")

    async def _finalize(self, name: str, rv: int) -> None:
        try:
            await self._client.delete(Pod, name, expected_rv=rv, force=True)
        except StoreError as exc:  # changed meanwhile: the next sync recomputes
            log.info("pod %s: final delete deferred: %s", name, exc)

    async def _report(self, pod: Pod) -> None:
        try:
            await self._client.update_status(pod)
        except StoreError as exc:  # Conflict, NotFound: the next sync recomputes
            log.info("pod %s: status not written: %s", pod.metadata.name, exc)
            return
        if pod.status.reason == CRASH_LOOP:
            await self.events.record(pod, "BackOff", pod.status.message, "Warning")

    async def _fail(self, pod: Pod, reason: str, message: str) -> None:
        log.warning("pod %s: %s: %s", pod.metadata.name, reason, message)
        pod.status.phase, pod.status.reason, pod.status.message = PodPhase.FAILED, reason, message
        try:
            await self._client.update_status(pod)
        except StoreError as exc:
            log.info("pod %s: failure not written: %s", pod.metadata.name, exc)

    async def run(self) -> None:
        # Before registering: once this incarnation registers, the pods still bound to
        # the node are started again, so the old processes must already be gone.
        self._runtime.cleanup_orphans()
        await self.register()
        interval = self._fencing.heartbeat_interval
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._every(interval, self.heartbeat_once))
                tasks.create_task(
                    self._every(min(interval, self._fencing.shim_poll), self.fence_check)
                )
                tasks.create_task(self.pods.run())
                tasks.create_task(self._sync_forever())
        finally:
            await self._runtime.stop_all()

    async def _sync_forever(self) -> None:
        await self.pods.wait_synced()
        while True:
            await self.sync_once()
            await self._wake.wait(self._clock, self._resync_interval)

    async def _every(self, interval: float, step: Callable[[], Awaitable[None]]) -> None:
        while True:
            await step()
            await self._clock.sleep(interval)
