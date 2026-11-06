"""One simulated run: build the world from a scenario, play it, check it (P5.1-P5.5).

Run shape (TESTING.md 6.3): faults and user actions during the first ~90 virtual
seconds, safety checked after every event; then everything has healed, the run goes
quiet for `QUIET` seconds, and liveness is checked. The same seed always produces the
same trace, byte for byte; `trace_hash` is what the determinism test compares.
"""

from __future__ import annotations

import itertools
import logging
import math
import tempfile
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from chronos.api import Deployment, DeploymentSpec, ObjectMeta, PodTemplate
from chronos.api.resources import format_resources
from chronos.common.config import FencingConfig, Timings
from chronos.controllers.runtime import Informer
from chronos.core.agent_sync import Backoff
from chronos.core.schedule import SchedulerConfig, resolve
from chronos.sim import canaries
from chronos.sim.control import SimControlPlane
from chronos.sim.disk import SimDisk
from chronos.sim.engine import Sim, SimError, run_sync
from chronos.sim.faults import Entry, Scenario, generate
from chronos.sim.invariants import Checker, Violation
from chronos.sim.network import CP, Network, Server
from chronos.sim.nodes import AgentSettings, SimAgent, SimNode
from chronos.store import Conflict, NotFound, StoreError

TIMINGS = Timings(
    heartbeat_interval=0.5, heartbeat_timeout=0.4, node_grace=2.0, eviction_timeout=5.0,
    lifecycle_tick=0.25, resync_period=10.0,
)  # fmt: skip
# F + K + drift = 2.5 + 1.0 + 0.5 = 4.0 < E = 5.0
FENCING = FencingConfig(
    heartbeat_interval=0.5,
    fence_timeout=2.5,
    termination_grace=1.0,
    drift_margin=0.5,
    shim_poll=0.1,
)
BACKOFF = Backoff(base=0.5, cap=4.0, reset_after=20.0)
QUIET = 25.0  # > E + backoff cap + time to schedule and start
WATCH_HISTORY = 16  # small, so an agent that was cut off falls behind it and must relist
SNAPSHOT_EVERY = 50


@dataclass
class Result:
    seed: int
    canary: str | None
    violation: Violation | None
    trace_hash: str
    events: int
    tail: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.violation is None

    def report(self) -> str:
        head = f"seed {self.seed}" + (f" canary {self.canary}" if self.canary else "")
        if self.ok:
            return f"{head}: ok ({self.events} events, trace {self.trace_hash[:12]})"
        lines = [f"{head}: {self.violation}", "last events:"] + [f"  {t}" for t in self.tail]
        return "\n".join(lines)


class World:
    def __init__(self, scenario: Scenario, hooks: canaries.Hooks, disk: SimDisk) -> None:
        self.scenario = scenario
        self.hooks = hooks
        self.sim = Sim(scenario.seed)
        self.net = Network(self.sim)
        self.disk = disk
        self.data_dir = disk.dir
        f = FENCING
        if hooks.fence_timeout is not None:
            f = FencingConfig.model_construct(
                **{**FENCING.model_dump(), "fence_timeout": hooks.fence_timeout}
            )
        self.fencing = f
        self.scale_down_window = f.termination_grace + 1.0
        self.accepted_send: dict[str, float] = {}
        self.acked = 0
        self.incarnation = 0
        counters = {k: itertools.count(1) for k in ("uid", "pod", "epoch")}
        self._uid = lambda: f"u{next(counters['uid'])}"
        self._pod_name = lambda dep: f"{dep}-{next(counters['pod'])}"
        self._epoch = lambda: f"e{next(counters['epoch'])}"
        self.settings = AgentSettings(
            fencing=f, backoff=BACKOFF, resync_interval=1.0, watch_wait=10.0,
            request_timeout=2.0,
            heartbeat_timeout=hooks.heartbeat_timeout or TIMINGS.heartbeat_timeout,
            patches=list(hooks.agent or []),
        )  # fmt: skip
        self.nodes = {
            spec.name: SimNode(
                self.sim,
                spec.name,
                spec.capacity,
                f,
                TIMINGS.eviction_timeout,
                shims_enforce=hooks.shims_enforce,
            )
            for spec in scenario.nodes
        }
        self.cp: SimControlPlane | None = None
        self._start_cp()
        for node in self.nodes.values():
            self.sim.after(self.sim.rng.uniform(0.0, 0.5), self._booter(node))
        for entry in scenario.entries:
            self.sim.at(entry.at, self._player(entry), f"scenario {entry.describe()}")
        self.checker = Checker(self)

    # ---- parts

    def server(self) -> Server | None:
        return self.cp if self.cp is not None and self.cp.alive else None

    def _start_cp(self) -> None:
        self.incarnation += 1
        cp = SimControlPlane(
            self.sim, self.data_dir, TIMINGS, self.hooks.profile,
            uid=self._uid, pod_name=self._pod_name, on_ack=self._ack,
            on_heartbeat=self._accepted, watch_history=WATCH_HISTORY, snapshot_every=SNAPSHOT_EVERY,
        )  # fmt: skip
        if cp.store.revision < self.acked:
            raise Violation(
                "S8", f"recovered revision {cp.store.revision} < {self.acked} already acknowledged"
            )
        if self.hooks.scheduler:
            self.hooks.scheduler(cp.scheduler)
        if self.hooks.deployments:
            self.hooks.deployments(cp.deployments)
        self.cp = cp

    def _ack(self, revision: int) -> None:
        self.acked = max(self.acked, revision)

    def _accepted(self, node: str, sent_at: float) -> None:
        self.accepted_send[node] = max(self.accepted_send.get(node, -math.inf), sent_at)

    def _booter(self, node: SimNode) -> Callable[[], None]:
        return lambda: self._start_agent(node)

    def _player(self, entry: Entry) -> Callable[[], None]:
        return lambda: self._play(entry)

    def _start_agent(self, node: SimNode) -> None:
        if node.agent is None:
            SimAgent(self.sim, self.net, node, self.server, self.settings, self._epoch)

    def informers(self) -> Iterator[tuple[Informer[Any], str | None]]:
        if self.cp is not None:
            for informer in self.cp.informers:
                yield informer, None
        for name, node in sorted(self.nodes.items()):
            if node.agent is not None:
                yield node.agent.agent.pods, name

    # ---- the scenario

    def _later(self, delay: float, fn: Callable[[], None], what: str | None) -> None:
        self.sim.after(delay, fn, what)

    def _play(self, e: Entry) -> None:  # noqa: PLR0912, PLR0915 (one branch per kind)
        node = self.nodes.get(e.target)
        match e.kind:
            case "deploy" | "scale" | "undeploy":
                self._user(e, attempts=20)
            case "cp_crash":
                if self.cp is None or not self.cp.alive:
                    return
                self.cp.crash()
                cut = self.disk.crash()
                if cut:
                    self.sim.log(f"disk lost unsynced bytes: {', '.join(cut)}")
                self._later(e.duration, self._start_cp, "cp restarts from its WAL")
            case "node_crash" if node is not None:
                if node.agent is not None:
                    node.agent.kill()
                node.power_off()
                self._later(e.duration, lambda: self._start_agent(node), f"{node.name} boots")
            case "agent_crash" if node is not None and node.agent is not None:
                node.agent.kill()
                self._later(
                    e.duration, lambda: self._start_agent(node), f"{node.name} agent restarts"
                )
            case "agent_pause" if node is not None and node.agent is not None:
                actor = node.agent.actor
                actor.paused = True
                self._later(e.duration, lambda: self._resume(actor), f"{node.name} agent resumes")
            case "node_freeze" if node is not None:
                node.freeze()
                frozen = node.agent.actor if node.agent is not None else None
                if frozen is not None:
                    frozen.paused = True

                def thaw() -> None:
                    node.thaw()
                    if frozen is not None:
                        self._resume(frozen)

                self._later(e.duration, thaw, f"{node.name} thaws")
            case "partition" if node is not None:
                way = str(e.arg.get("dir", "both"))
                links = [self.net.link(node.name, CP)] if way in ("up", "both") else []
                links += [self.net.link(CP, node.name)] if way in ("down", "both") else []
                self._for(e, links, "cut", True, False)
            case "degrade" if node is not None:
                # The link slows down, then dies: slow replies right before silence.
                links = [self.net.link(node.name, CP), self.net.link(CP, node.name)]
                self._for(e, links, "spike", float(str(e.arg["spike"])), 0.0)
                cut_after = float(str(e.arg["cut_after"]))
                self._later(cut_after, self._cutter(links, cut=True), f"{node.name} link dies")
                self._later(e.duration, self._cutter(links, cut=False), None)
            case "delay" if node is not None:
                links = [self.net.link(node.name, CP), self.net.link(CP, node.name)]
                self._for(e, links, "spike", float(str(e.arg["spike"])), 0.0)
            case "drop" if node is not None:
                links = [self.net.link(node.name, CP), self.net.link(CP, node.name)]
                self._for(e, links, "drop", float(str(e.arg["p"])), 0.0)
            case "crash_storm" if node is not None:
                self._storm(node, self.sim.now + e.duration, float(str(e.arg["rate"])))
            case _:
                self.sim.log(f"skipped {e.kind} on {e.target}")

    def _for(self, e: Entry, links: list[Any], attr: str, on: object, off: object) -> None:
        for link in links:
            setattr(link, attr, on)

        def heal() -> None:
            for link in links:
                setattr(link, attr, off)

        self._later(e.duration, heal, f"heal {e.kind} {e.target}")

    def _cutter(self, links: list[Any], *, cut: bool) -> Callable[[], None]:
        def apply() -> None:
            for link in links:
                link.cut = cut

        return apply

    def _resume(self, actor: Any) -> None:
        for fn in actor.resume():
            fn()

    def _storm(self, node: SimNode, until: float, rate: float) -> None:
        if self.sim.now >= until:
            return
        procs = sorted(node.procs.values(), key=lambda p: p.pid)
        if procs:
            node.crash_process(self.sim.rng.choice(procs))
        self._later(self.sim.rng.expovariate(rate), lambda: self._storm(node, until, rate), None)

    def _user(self, e: Entry, attempts: int) -> None:
        """chronosctl, from the operator's machine: retried while chronosd is down."""
        cp = self.server()
        if cp is None:
            if attempts > 1:
                self._later(
                    1.0, lambda: self._user(e, attempts - 1), f"user retries {e.kind} {e.target}"
                )
            return
        assert isinstance(cp, SimControlPlane)
        client = cp.client
        try:
            if e.kind == "deploy":
                requests = format_resources(e.arg["requests"])  # type: ignore[arg-type]
                dep = Deployment(
                    metadata=ObjectMeta(name=e.target),
                    spec=DeploymentSpec(
                        replicas=int(str(e.arg["replicas"])),
                        template=PodTemplate.model_validate(
                            {"command": ["workload"], "resources": {"requests": requests}}
                        ),
                    ),
                )
                run_sync(client.create(dep))
            elif e.kind == "scale":
                for _ in range(5):
                    dep = run_sync(client.get(Deployment, e.target))
                    dep.spec.replicas = int(str(e.arg["replicas"]))
                    try:
                        run_sync(client.update(dep))
                        break
                    except Conflict:
                        continue
            else:
                run_sync(client.delete(Deployment, e.target))
        except NotFound:
            self.sim.log(f"user {e.kind} {e.target}: not found")
        except StoreError as exc:
            self.sim.log(f"user {e.kind} {e.target}: {type(exc).__name__}")
        cp.changed()

    # ---- running

    def run(self) -> None:
        heal = self.scenario.heal_time
        self.sim.run_until(heal + QUIET, self.checker.after_event)
        self.checker.liveness(resolve(SchedulerConfig()))  # the real profile, canary or not


def run_seed(
    seed: int,
    canary: str | None = None,
    *,
    scenario: Scenario | None = None,
    keep_trace: bool = False,
) -> Result:
    scenario = scenario or generate(seed)
    logging.disable(logging.CRITICAL)  # the trace tells the story; the agents' logs are noise
    try:
        return _run(seed, canary, scenario, keep_trace)
    finally:
        logging.disable(logging.NOTSET)


def _run(seed: int, canary: str | None, scenario: Scenario, keep_trace: bool) -> Result:
    with (
        tempfile.TemporaryDirectory(prefix="chronos-sim-") as tmp,
        canaries.active(canary) as hooks,
    ):
        world = None
        violation: Violation | None = None
        disk = SimDisk(Path(tmp), honest=hooks.honest_disk)
        try:
            with disk.installed():
                world = World(scenario, hooks, disk)
                if keep_trace:
                    world.sim.full_trace = []
                world.run()
        except Violation as v:
            violation = v
        sim = world.sim if world is not None else Sim(seed)
        if violation is not None:
            sim.log(f"VIOLATION {violation}")
        result = Result(seed, canary, violation, sim.trace_hash, sim.events, list(sim.tail))
        if keep_trace and sim.full_trace is not None:
            result.tail = sim.full_trace
        return result


def minimize(seed: int, canary: str | None, invariant: str) -> Scenario:
    """Greedy delta debugging: drop schedule entries while the same invariant still breaks."""
    scenario = generate(seed)
    i = len(scenario.entries) - 1
    while i >= 0:
        smaller = scenario.without(i)
        result = run_seed(seed, canary, scenario=smaller)
        if result.violation is not None and result.violation.invariant == invariant:
            scenario = smaller
        i -= 1
    return scenario


__all__ = ["Result", "SimError", "World", "minimize", "run_seed"]
