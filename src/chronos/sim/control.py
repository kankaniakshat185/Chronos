"""`chronosd` in the simulator: the real store, client and controllers, stepped by the RNG.

What's real: `Store.open` (WAL recovery included) on the simulated disk,
`LocalStoreClient`, `Leases`, and the three built-in controllers on shared
`Informers`. What's driven: instead of informer and worker tasks, each control-plane
step performs **one** action chosen by the RNG among those available: let one
informer catch up with the store, or let one controller process one key. So caches
are stale for random stretches and controllers interleave differently every seed.

Agents' requests arrive as network messages; `serve()` runs them through the real
client. Watches are long polls: held until there's something to send or `wait`
passes.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from chronos.api import Deployment, Node, Pod
from chronos.client.local import LocalStoreClient
from chronos.common.config import Timings
from chronos.controllers.deployment import DeploymentController
from chronos.controllers.node_lifecycle import NodeLifecycleController
from chronos.controllers.runtime import Controller, Informer, Informers
from chronos.controllers.scheduler import SchedulerController
from chronos.core.schedule import Profile
from chronos.sim.engine import Sim, run_sync
from chronos.sim.network import Respond
from chronos.store import Store, StoreError
from chronos.store.leases import Leases


class AckingClient(LocalStoreClient):
    """Reports the revision every successful answer revealed (what S8 must keep)."""

    def __init__(self, *args: Any, on_ack: Callable[[int], None], **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._on_ack = on_ack

    async def _run[R](self, call: Callable[[], R]) -> R:
        result = await super()._run(call)
        self._on_ack(self.store.revision)
        return result


@dataclass(eq=False)
class HeldWatch:
    node: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    deadline: float
    respond: Respond


class SimControlPlane:
    def __init__(
        self,
        sim: Sim,
        data_dir: Path,
        timings: Timings,
        profile: Profile,
        *,
        uid: Callable[[], str],
        pod_name: Callable[[str], str],
        on_ack: Callable[[int], None],
        on_heartbeat: Callable[[str, float], None],
        watch_history: int,
        snapshot_every: int,
    ) -> None:
        self.sim = sim
        self.alive = True
        self._on_heartbeat = on_heartbeat
        clock = sim.clock
        self.store = Store.open(
            data_dir,
            clock,
            sync="full",
            snapshot_every=snapshot_every,
            watch_history=watch_history,
            uid_factory=uid,
        )
        self.client = AckingClient(
            self.store, Leases(clock), timings.eviction_timeout, clock=clock, on_ack=on_ack
        )
        informers = Informers(self.client, clock, watch_wait=0.0)
        self.lifecycle = NodeLifecycleController(self.client, clock, timings, informers)
        self.scheduler = SchedulerController(
            self.client, clock, profile, informers, resync_period=timings.resync_period
        )
        self.deployments = DeploymentController(
            self.client, clock, informers, resync_period=timings.resync_period, new_name=pod_name
        )
        self.controllers: list[Controller] = [self.lifecycle, self.scheduler, self.deployments]
        self.informers: list[Informer[Any]] = [
            informers.get(Node), informers.get(Pod), informers.get(Deployment)
        ]  # fmt: skip
        self._held: list[HeldWatch] = []
        self._kicked = False
        for informer in self.informers:
            run_sync(informer.relist())
        for controller in self.controllers:
            controller.resync()
        self._every(timings.lifecycle_tick, self.lifecycle.resync)
        self._every(timings.resync_period, self.scheduler.resync)
        self._every(timings.resync_period, self.deployments.resync)
        self.kick()

    # ---- lifecycle

    def crash(self) -> None:
        """Gone at once: nothing is flushed, held watches are never answered."""
        self.alive = False
        self._held.clear()
        wal = self.store._wal
        if wal is not None:
            import os  # noqa: PLC0415

            os.close(wal._fd)

    def _every(self, period: float, fn: Callable[[], None]) -> None:
        def tick() -> None:
            if self.alive:
                fn()
                self.kick()
                self.sim.after(period, tick)

        self.sim.after(period, tick)

    # ---- stepping the controllers

    def kick(self) -> None:
        """Make sure a control-plane step is coming."""
        if self.alive and not self._kicked:
            self._kicked = True
            self.sim.after(self.sim.rng.uniform(0.0002, 0.01), self._step)

    def _step(self) -> None:
        self._kicked = False
        if not self.alive:
            return
        behind = [i for i in self.informers if i.needs_relist or i.revision < self.store.revision]
        busy = [c for c in self.controllers if c.queue.ready()]
        choices: list[tuple[str, Any]] = [("sync", i) for i in behind] + [("work", c) for c in busy]
        if not choices:
            due = [d for c in self.controllers if (d := c.queue.next_due()) is not None]
            if due:
                self.sim.at(min(due), self.kick)
            return
        kind, target = self.sim.rng.choice(choices)
        if kind == "sync":
            self.sim.log(f"cp informer {target.kind.__name__} catches up")
            run_sync(target.step())
        else:
            self.sim.log(f"cp {target.name} processes a key")
            run_sync(target.process_one())
        self.changed()

    def changed(self) -> None:
        """After anything that may have written: answer held watches, keep stepping."""
        self._answer_watches()
        self.kick()

    # ---- serving agents and users

    def serve(  # noqa: PLR0917 (the Server protocol: one call per network message)
        self,
        node: str,
        method: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        sent_at: float,
        respond: Respond,
    ) -> None:
        if method == "watch":
            wait = kwargs.pop("wait")
            held = HeldWatch(node, args, kwargs, self.sim.now + wait, respond)
            if not self._try_answer(held, final=False):
                self._held.append(held)
                self.sim.at(held.deadline, lambda: self._expire(held))
            return
        try:
            result = run_sync(getattr(self.client, method)(*args, **kwargs))
        except StoreError as exc:
            respond(None, exc)
        else:
            if method in ("heartbeat", "register"):
                self._on_heartbeat(node, sent_at)
            respond(result, None)
        self.changed()

    def _try_answer(self, held: HeldWatch, *, final: bool) -> bool:
        try:
            batch = run_sync(self.client.watch(*held.args, wait=0.0, **held.kwargs))
        except StoreError as exc:
            held.respond(None, exc)
            return True
        if batch.events or final:
            held.respond(batch, None)
            return True
        return False

    def _answer_watches(self) -> None:
        still = []
        for held in self._held:
            if not self._try_answer(held, final=False):
                still.append(held)
        self._held = still

    def _expire(self, held: HeldWatch) -> None:
        if self.alive and held in self._held:
            self._held.remove(held)
            self._try_answer(held, final=True)
