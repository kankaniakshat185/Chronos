"""The node agent: register, heartbeat, and keep local processes in sync (ADR-0005, ADR-0011).

Two independent loops, so a slow sync (or a slow stop) never delays a heartbeat:

* heartbeat: every `heartbeat_interval`. If the server no longer recognizes this
  incarnation (EpochMismatch, e.g. after the node went NotReady) or the node is
  gone (NotFound), it re-registers with a fresh epoch.
* sync (MVP polling, ADR-0022): every `poll_interval`, read the pods bound to this
  node, plan with `core.agent_sync`, and carry the actions out. Failed writes are
  simply retried on the next tick: the plan is recomputed from scratch each time.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable, Mapping

from chronos.agent.runtime import ProcessRuntime
from chronos.api import Pod
from chronos.client.protocol import StoreClient
from chronos.common.clock import Clock
from chronos.core.agent_sync import Forget, Report, Start, Stop, plan
from chronos.store import NotFound, StoreError
from chronos.store.errors import EpochMismatch, Unavailable

log = logging.getLogger(__name__)


class Agent:
    def __init__(
        self,
        *,
        name: str,
        capacity: Mapping[str, int],
        client: StoreClient,
        runtime: ProcessRuntime,
        clock: Clock,
        heartbeat_interval: float,
        poll_interval: float,
    ) -> None:
        self.name = name
        self.epoch = ""
        self._capacity = dict(capacity)
        self._client = client
        self._runtime = runtime
        self._clock = clock
        self._heartbeat_interval = heartbeat_interval
        self._poll_interval = poll_interval

    async def register(self) -> None:
        """Register under a new epoch, retrying until the server answers."""
        epoch = uuid.uuid4().hex
        while True:
            try:
                await self._client.register(self.name, epoch, self._capacity)
            except Unavailable as exc:
                log.warning("register: %s; retrying", exc)
                await self._clock.sleep(self._heartbeat_interval)
                continue
            self.epoch = epoch
            log.info("registered node %s (epoch %s)", self.name, epoch[:8])
            return

    async def heartbeat_once(self) -> None:
        try:
            await self._client.heartbeat(self.name, self.epoch)
        except (EpochMismatch, NotFound) as exc:
            log.warning("heartbeat refused (%s); re-registering", exc)
            await self.register()
        except StoreError as exc:  # Unavailable, timeouts: keep beating
            log.warning("heartbeat failed: %s", exc)

    async def sync_once(self) -> None:
        try:
            pods, _ = await self._client.list(Pod, node_name=self.name)
        except StoreError as exc:
            log.warning("sync: can't list pods: %s", exc)
            return
        for action in plan(self.name, pods, self._runtime.snapshot()):
            match action:
                case Start(pod=pod):
                    await self._runtime.start(pod)
                case Stop(uid=uid):
                    self._runtime.stop(uid)
                case Forget(uid=uid):
                    self._runtime.forget(uid)
                case Report(pod=pod):
                    try:
                        await self._client.update_status(pod)
                    except StoreError as exc:  # Conflict, NotFound: next tick recomputes
                        log.info("pod %s: status not written: %s", pod.metadata.name, exc)

    async def run(self) -> None:
        await self.register()
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._every(self._heartbeat_interval, self.heartbeat_once))
                tasks.create_task(self._every(self._poll_interval, self.sync_once))
        finally:
            await self._runtime.stop_all()

    async def _every(self, interval: float, step: Callable[[], Awaitable[None]]) -> None:
        while True:
            await step()
            await self._clock.sleep(interval)
