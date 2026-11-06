"""Nodes in the simulator: machines, processes, shims, and the real `Agent` on them.

* `SimNode` is the machine. It outlives its agent: when the agent crashes, the
  workloads and their shims keep running, as real processes would. The shims apply
  the real `core.fencing.shim_action` to the node's lease. Rather than polling every
  100 ms, a shim check is scheduled for the moment its decision could change (lease
  + F, lease + E), plus a random part of one poll interval.
* `SimRuntime` has the interface the agent uses from `ProcessRuntime`: start, stop,
  forget, snapshot, write_lease, cleanup_orphans. Units come from the real
  `Providers`, so S12 is checked against real allocation code.
* `SimAgent` runs the real `chronos.agent.Agent` (register, heartbeat, fence check,
  pod informer, `sync_once`) as `Task`s owned by one `Actor`, so it can be paused
  or killed mid-step.
"""

from __future__ import annotations

import math
import random
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from chronos.agent.agent import Agent
from chronos.agent.providers import Providers
from chronos.api import Pod
from chronos.common.config import FencingConfig
from chronos.core.agent_sync import Backoff, LocalProc
from chronos.core.fencing import FENCED_EXIT_CODE, ShimAction, shim_action
from chronos.sim.engine import Actor, Sim, SimFuture, Task
from chronos.sim.network import Network, Server, SimNetClient


@dataclass(eq=False)
class SimProcess:
    pid: int
    uid: str
    pod: str
    owner: str | None
    node: str
    assigned: dict[str, list[str]]
    started_at: float
    notify: Callable[[SimProcess], None]  # the runtime that started it hears its exit
    alive: bool = True
    stopping: bool = False
    exit_code: int | None = None


class SimNode:
    def __init__(
        self,
        sim: Sim,
        name: str,
        capacity: Mapping[str, int],
        fencing: FencingConfig,
        eviction_timeout: float,
        *,
        shims_enforce: bool = True,
    ) -> None:
        self.sim, self.name, self.capacity = sim, name, dict(capacity)
        self.fencing, self.eviction_timeout = fencing, eviction_timeout
        self.shims_enforce = shims_enforce
        self.lease: float | None = None  # the agent's lease file: survives the agent
        self.procs: dict[int, SimProcess] = {}  # live ones
        self.frozen = False
        self.resumed_at = -math.inf
        self.agent: SimAgent | None = None
        self._deferred: list[Callable[[], None]] = []
        self._shim_gen = 0
        self._next_pid = 1

    # ---- processes

    def spawn(
        self,
        uid: str,
        pod: Pod,
        assigned: dict[str, list[str]],
        notify: Callable[[SimProcess], None],
    ) -> SimProcess:
        pid = self._next_pid
        self._next_pid += 1
        proc = SimProcess(
            pid,
            uid,
            pod.metadata.name,
            pod.metadata.owner,
            self.name,
            assigned,
            self.sim.now,
            notify,
        )
        self.procs[pid] = proc
        self.sim.log(f"{self.name} start pid {pid} for {pod.metadata.name}")
        self.schedule_shims()
        return proc

    def _when_running(self, fn: Callable[[], None]) -> None:
        """Process-level things (exits, kills) wait while the whole node is frozen."""
        if self.frozen:
            self._deferred.append(fn)
        else:
            fn()

    def exit(self, proc: SimProcess, code: int) -> None:
        if not proc.alive:
            return
        proc.alive, proc.exit_code = False, code
        del self.procs[proc.pid]
        self.sim.log(f"{self.name} pid {proc.pid} ({proc.pod}) exits {code}")
        proc.notify(proc)

    def terminate(self, proc: SimProcess, *, fenced: bool, grace: float) -> None:
        """SIGTERM; the workload may take a while (or ignore it); SIGKILL after `grace`."""
        if not proc.alive or proc.stopping:
            return
        proc.stopping = True
        reaction = self.sim.rng.uniform(0.0, 1.5 * grace)
        code = FENCED_EXIT_CODE if fenced else -15
        if reaction < grace:
            self.sim.after(reaction, lambda: self._when_running(lambda: self.exit(proc, code)))
        else:
            kill_code = FENCED_EXIT_CODE if fenced else -9
            self.sim.after(grace, lambda: self._when_running(lambda: self.exit(proc, kill_code)))

    def kill(self, proc: SimProcess, code: int = -9) -> None:
        self._when_running(lambda: self.exit(proc, code))

    def crash_process(self, proc: SimProcess) -> None:
        """The workload dies on its own (a crash storm)."""
        if proc.alive and not proc.stopping and not self.frozen:
            self.exit(proc, 1)

    def power_off(self) -> None:
        """Everything on the machine is gone, with no exits reported to anyone."""
        for proc in list(self.procs.values()):
            proc.alive, proc.exit_code = False, -9
        self.procs.clear()
        self._deferred.clear()
        self.frozen = False

    # ---- freeze

    def freeze(self) -> None:
        self.frozen = True

    def thaw(self) -> None:
        self.frozen = False
        self.resumed_at = self.sim.now
        deferred, self._deferred = self._deferred, []
        for fn in deferred:
            fn()
        self.schedule_shims()

    # ---- shims

    def write_lease(self, t: float) -> None:
        self.lease = t
        self.schedule_shims()

    def schedule_shims(self) -> None:
        if not self.shims_enforce or not self.procs:
            return
        self._shim_gen += 1
        gen = self._shim_gen
        f, e = self.fencing.fence_timeout, self.eviction_timeout
        if self.lease is None:
            threshold = self.sim.now
        elif any(not p.stopping for p in self.procs.values()):
            threshold = self.lease + f
        else:
            threshold = self.lease + e  # all already TERMed: only the KILL rule is left
        when = max(threshold, self.sim.now) + self.sim.rng.uniform(1e-6, self.fencing.shim_poll)
        self.sim.at(when, lambda: self._shim_check(gen))

    def _shim_check(self, gen: int) -> None:
        if gen != self._shim_gen or self.frozen:
            return
        age = math.inf if self.lease is None else self.sim.now - self.lease
        action = shim_action(age, self.fencing.fence_timeout, self.eviction_timeout)
        for proc in list(self.procs.values()):
            if action == ShimAction.KILL:
                self.sim.log(f"{self.name} shim kills pid {proc.pid} (lease age {age:.3f})")
                self.exit(proc, FENCED_EXIT_CODE)
            elif action == ShimAction.FENCE and not proc.stopping:
                self.sim.log(f"{self.name} shim fences pid {proc.pid} (lease age {age:.3f})")
                self.terminate(proc, fenced=True, grace=self.fencing.termination_grace)
        self.schedule_shims()


class SimRuntime:
    """What `Agent` needs from `ProcessRuntime`, on a `SimNode`."""

    def __init__(self, node: SimNode, actor: Actor, providers: Providers, grace: float) -> None:
        self._node, self._actor, self._providers, self._grace = node, actor, providers, grace
        self.eviction_timeout: float | None = None
        self.on_change: Callable[[], None] | None = None
        self._procs: dict[str, LocalProc] = {}
        self._live: dict[str, SimProcess] = {}
        self._fenced: set[str] = set()

    def cleanup_orphans(self) -> list[int]:
        """The previous incarnation's processes: SIGKILL, as `ProcessRuntime` does."""
        killed = []
        for proc in list(self._node.procs.values()):
            self._node.kill(proc)
            killed.append(proc.pid)
        return killed

    def write_lease(self, last_ok_send: float) -> None:
        self._node.write_lease(last_ok_send)

    def snapshot(self) -> dict[str, LocalProc]:
        return dict(self._procs)

    async def start(self, pod: Pod, *, restarts: int = 0, crashes: int = 0) -> None:
        uid = pod.metadata.uid
        assigned, _env = self._providers.allocate(uid, pod.requests)  # may raise ProviderError
        await self._node.sim.timer(self._node.sim.rng.uniform(0.001, 0.02))  # fork + exec
        proc = self._node.spawn(uid, pod, assigned, self._exited)
        self._live[uid] = proc
        self._procs[uid] = LocalProc(
            uid, pod.metadata.name, proc.pid, started_at=self._node.sim.now, restarts=restarts,
            crashes=crashes, assigned=assigned,
        )  # fmt: skip
        self._changed()

    def stop(self, uid: str, *, fenced: bool = False) -> None:
        if fenced:
            self._fenced.add(uid)
        proc = self._live.get(uid)
        if proc is not None:
            self._node.terminate(proc, fenced=fenced, grace=self._grace)

    def kill_now(self, uid: str) -> None:
        self._fenced.add(uid)
        proc = self._live.get(uid)
        if proc is not None:
            self._node.kill(proc, FENCED_EXIT_CODE)

    def forget(self, uid: str) -> None:
        self._procs.pop(uid, None)

    async def stop_all(self) -> None:
        for uid in list(self._live):
            self.stop(uid)

    def _exited(self, proc: SimProcess) -> None:
        def reap() -> None:  # the agent's waiter task: it runs only when the agent does
            self._providers.release(proc.uid)
            code = proc.exit_code if proc.exit_code is not None else -9
            if proc.uid in self._fenced:
                self._fenced.discard(proc.uid)
                code = FENCED_EXIT_CODE
            if self._live.get(proc.uid) is proc:
                del self._live[proc.uid]
            current = self._procs.get(proc.uid)
            if current is not None and current.pid == proc.pid:
                self._procs[proc.uid] = replace(
                    current, exit_code=code, exited_at=self._node.sim.now
                )
            self._changed()

        self._actor.run(reap)

    def _changed(self) -> None:
        if self.on_change is not None:
            self.on_change()


@dataclass
class AgentSettings:
    fencing: FencingConfig
    backoff: Backoff
    resync_interval: float
    watch_wait: float
    request_timeout: float
    heartbeat_timeout: float
    patches: list[Callable[[Agent], None]] = field(default_factory=list)  # canaries


class SimAgent:
    """One incarnation of `chronos-agent` on a node."""

    def __init__(  # noqa: PLR0917
        self,
        sim: Sim,
        net: Network,
        node: SimNode,
        server: Callable[[], Server | None],
        settings: AgentSettings,
        new_epoch: Callable[[], str],
    ) -> None:
        self.sim, self.node = sim, node
        self.actor = Actor(node.name)
        self.runtime = SimRuntime(
            node, self.actor, Providers(node.capacity), settings.fencing.termination_grace
        )
        client = SimNetClient(
            sim, net, node.name, server,
            request_timeout=settings.request_timeout, heartbeat_timeout=settings.heartbeat_timeout,
        )  # fmt: skip
        self.agent = Agent(
            name=node.name,
            capacity=node.capacity,
            client=client,
            runtime=self.runtime,  # type: ignore[arg-type]
            clock=sim.clock,
            fencing=settings.fencing,
            resync_interval=settings.resync_interval,
            backoff=settings.backoff,
            watch_wait=settings.watch_wait,
            rng=random.Random(sim.rng.random()),
            new_epoch=new_epoch,
        )
        for patch in settings.patches:
            patch(self.agent)
        self._settings = settings
        self._wake: SimFuture | None = None
        self._woken = False
        self.runtime.on_change = self.wake
        self.agent.pods.add_handler(lambda _old, _new: self.wake())
        node.agent = self
        self._spawn(self._main(), "main")

    def _spawn(self, coro: Any, name: str) -> None:
        Task(self.sim, self.actor, coro, name)

    def wake(self) -> None:
        """Like the real agent's Signal: a change during a sync isn't lost."""
        self._woken = True
        if self._wake is not None:
            self._wake.set_result()

    async def _main(self) -> None:
        self.runtime.cleanup_orphans()
        await self.agent.register()
        f = self._settings.fencing
        self._spawn(self.agent._every(f.heartbeat_interval, self.agent.heartbeat_once), "heartbeat")
        self._spawn(
            self.agent._every(min(f.heartbeat_interval, f.shim_poll), self.agent.fence_check),
            "fence",
        )
        self._spawn(self.agent.pods.run(), "informer")
        self._spawn(self._sync(), "sync")

    async def _sync(self) -> None:
        while True:
            self._woken = False
            await self.agent.sync_once()
            if self._woken:
                continue
            self._wake = SimFuture()
            self.sim.after(self._settings.resync_interval, self._wake.set_result)
            await self._wake
            self._wake = None

    def kill(self) -> None:
        self.actor.kill()
        if self.node.agent is self:
            self.node.agent = None
