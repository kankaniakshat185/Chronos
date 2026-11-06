"""Chaos on real processes (P8.2, P8.3, TESTING.md section 8).

    python -m chronos.testing.chaos --seed 7 --duration 60 --nodes 3 --out chaos-report

A real `chronosd` (with its WAL) and N agents, each agent's link to the control plane
through its own `FaultProxy`. A seeded schedule of faults and user actions runs while a
live checker samples the store (over a link that isn't faulted) and the OS process
table every 200 ms. At the end everything is healed and liveness is checked.

Faults: kill -9 an agent (its workloads stay, under their shims), kill -9 a whole node,
kill -9 chronosd (restart from the WAL), SIGSTOP an agent (a GC pause), SIGSTOP a whole
node (agent + shims + workloads: a VM freeze), proxy delay / drop / blackhole / reset,
workload crash storms, and probe faults: a node's readiness probes fail for a while
(flap) or its liveness probes do (sick: the pods are restarted). Workloads request
extended resources whose names are generated per run (X-7); some have exec probes, each
deployment has its own rollout strategy, and template updates start rolling updates.

What's checked, live: S1, S2 and S11 (per resource name), S12 from the store; S3 and S4
against the process table; S7 (rolling updates): at most replicas + max_surge pods while
one runs, and, in calm windows (no fault for SETTLE s), ready pods never pushed below
replicas - max_unavailable (involuntary losses under faults aren't the controller's).
The store and the process table can't be read atomically, so S3, S4 and S7 count only if
they hold in two consecutive samples (>= 200 ms): the real-time view has that
resolution, and the simulator checks the same invariants exactly.
After the heal: L1, L2 (rollouts finished, unless stuck on pods that fit nowhere), L3
and L5.

Output: `<out>/<seed>/report.json` and `timeline.md`. Exit code 1 on any violation.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import psutil

from chronos.api import Deployment, Node, NodeCondition, Pod, PodPhase
from chronos.api.pod import TERMINAL_PHASES
from chronos.api.quantity import is_extended
from chronos.api.resources import add
from chronos.client import HttpStoreClient
from chronos.common.clock import RealClock
from chronos.core.reconcile import is_active, template_hash
from chronos.core.rollout import is_outdated
from chronos.store import Conflict, StoreError
from chronos.testing.cluster import Cluster, Timings, free_port, kill_node
from chronos.testing.faultproxy import PASS, FaultProxy, Mode

WORKLOAD = "chronos.testing.workload"
SAMPLE = 0.2
# The demo's timings: F + K + drift = 2 + 1 + 1 = 4 < E = 5 (production: 6 + 2 + 1 < 10).
# The drift margin is the budget for processes running late; the test fixture's 0.2 s is
# fine for quick integration tests, but a loaded laptop running dozens of processes blows
# through it in real time (testing-the-tests.md 4.15).
CHAOS_TIMINGS = Timings(
    heartbeat_interval=0.5, node_grace=2.0, eviction_timeout=5.0, resync_interval=0.5,
    fence_timeout=2.0, termination_grace=1.0, drift_margin=1.0,
    crash_backoff_base=0.5, crash_backoff_cap=4.0,
)  # fmt: skip
QUIET = 30.0  # after the heal: > eviction (5 s) + crash back-off cap (4 s) + starts
CONSECUTIVE = 2  # S3/S4/S7: samples in a row before it's a violation
SETTLE = 10.0  # S7 availability: a window is calm this long after the last fault healed
clock = RealClock()


@dataclass(frozen=True)
class Step:
    at: float
    kind: str
    target: str = ""
    duration: float = 0.0
    arg: dict[str, Any] = field(default_factory=dict)


@dataclass
class Report:
    seed: int
    nodes: list[str]
    duration: float
    steps: list[dict[str, Any]] = field(default_factory=list)
    violations: list[dict[str, Any]] = field(default_factory=list)
    timeline: list[tuple[float, str]] = field(default_factory=list)
    samples: int = 0
    load_max: float = 0.0  # 1-minute load average: S3/S4 are real-time bounds
    converged_after: float | None = None
    liveness: str = ""

    @property
    def ok(self) -> bool:
        return not self.violations and self.converged_after is not None


NODE_FAULTS = ["kill_agent", "kill_node", "pause_agent", "freeze_node", "crash_storm", "flap",
               "sick"]  # fmt: skip
ACTIONS = ("scale", "deploy", "update")
LINK_FAULTS = ["delay", "drop", "blackhole", "reset"]


def plan(seed: int, names: list[str], duration: float) -> tuple[list[Step], list[str]]:
    """Seeded faults (one at a time per target) and user actions; extended resource names."""
    rng = random.Random(f"chaos-{seed}")
    resources = [f"chaos{seed % 97}.dev/r{i}" for i in range(rng.choice([1, 1, 2]))]
    steps: list[Step] = []
    busy: dict[str, float] = {}
    t = 3.0
    while t < duration - 3:
        t += rng.uniform(1.0, 4.0)
        kind = rng.choice(
            [*NODE_FAULTS, *LINK_FAULTS, "kill_cp", "scale", "scale", "deploy", "update", "update"]
        )
        if kind in ACTIONS:
            action = {"replicas": rng.randint(0, 6), "version": rng.randint(2, 99)}
            steps.append(Step(round(t, 2), kind, arg=action))
            continue
        target = "cp" if kind == "kill_cp" else rng.choice(names)
        if busy.get(target, 0.0) > t:
            continue
        d = round(rng.choice([rng.uniform(0.3, 2.0), rng.uniform(2.0, 10.0)]), 2)  # E is 5 s
        arg: dict[str, Any] = {}
        if kind == "delay":
            arg = {"ms": rng.choice([50, 200, 600]), "jitter": 50}
        elif kind == "drop":
            arg = {"p": rng.choice([0.1, 0.3, 0.6])}
        steps.append(Step(round(t, 2), kind, target, d, arg))
        busy[target] = t + d + 1.0
    return steps, resources


class Chaos:
    def __init__(self, root: Path, seed: int, nodes: int = 3, duration: float = 60.0) -> None:
        self.root, self.seed, self.duration = root, seed, duration
        self.rng = random.Random(seed)
        self.names = [f"s{seed}n{i + 1}" for i in range(nodes)]
        self.timings = CHAOS_TIMINGS
        self.cluster = Cluster(root, self.timings)
        self.proxies: dict[str, FaultProxy] = {}
        self.steps, self.resources = plan(seed, self.names, duration)
        self.capacity: dict[str, dict[str, int]] = {}
        self.report = Report(seed, self.names, duration)
        self.started = 0.0
        self.frozen: dict[str, float] = {}  # node -> frozen since (math.inf: still) / resumed at
        self.deployments: list[str] = []
        self._cp_down = False
        self.api: HttpStoreClient | None = None
        # checker state
        self.bound: dict[str, str] = {}
        self.replicas_seen: dict[str, list[tuple[float, int]]] = {}
        self.strikes: Counter[str] = Counter()
        self.conditions: dict[str, str] = {}
        self.faults = 0  # active now
        self.last_fault = 0.0  # when the last one healed
        self.floor: dict[str, tuple[int, int]] = {}  # S7: uid -> (replicas, ready floor)
        self.flags = root / "probe-faults"  # flap-<node> / sick-<node>: probes fail

    # ---- helpers

    def now(self) -> float:
        return clock.monotonic() - self.started

    def note(self, text: str) -> None:
        self.report.timeline.append((round(self.now(), 3), text))

    def violation(self, invariant: str, message: str) -> None:
        self.report.violations.append(
            {"t": round(self.now(), 3), "invariant": invariant, "message": message}
        )
        self.note(f"VIOLATION {invariant}: {message}")

    def start_agent(self, name: str) -> None:
        cpu, mem = "4", "8Gi"
        extra = {r: n for r, n in self.capacity[name].items() if is_extended(r)}
        self.cluster.start_agent(name, cpu, mem, extra or None, server=self.proxies[name].url)

    # ---- the run

    async def run(self) -> Report:
        await self.cluster.start()
        for i, name in enumerate(self.names):
            proxy = FaultProxy(
                self.cluster.port, free_port(), rng=random.Random(self.seed * 100 + i)
            )
            await proxy.start()
            self.proxies[name] = proxy
            cap = {"cpu": 4000, "memory": 8 * 2**30}
            for r in self.resources:
                if i > 0 and self.rng.random() < 0.7:  # node 1 never has them: S11 exercised
                    cap[r] = self.rng.randint(1, 3)
            self.capacity[name] = cap
            self.start_agent(name)
        self.started = clock.monotonic()
        self.api = HttpStoreClient(self.cluster.url, request_timeout=1.0)
        try:
            await self._ready()
            for d in range(2):
                await self._deploy(f"d{d + 1}", self.rng.randint(1, 4))
            checker = asyncio.create_task(self._check_forever())
            await asyncio.gather(*(self._play(s) for s in self.steps), self._sleep(self.duration))
            checker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await checker
            await self._heal()
            await self._liveness()
        finally:
            await self.api.aclose()
            for proxy in self.proxies.values():
                await proxy.stop()
            leaked = await self.cluster.close()
            if leaked:
                self.violation("L5", f"process groups left after teardown: {leaked}")
        return self.report

    async def _sleep(self, seconds: float) -> None:
        await clock.sleep(seconds)

    async def _ready(self) -> None:
        assert self.api is not None
        for _ in range(150):
            with contextlib.suppress(StoreError):
                nodes, _ = await self.api.list(Node)
                if len(nodes) == len(self.names) and all(
                    n.status.condition == NodeCondition.READY for n in nodes
                ):
                    return
            await clock.sleep(0.1)
        raise RuntimeError("the cluster never became Ready")

    async def _deploy(self, name: str, replicas: int) -> None:
        assert self.api is not None
        requests: dict[str, Any] = {
            "cpu": self.rng.choice(["100m", "250m", "500m"]),
            "memory": "64Mi",
        }
        if self.resources and self.rng.random() < 0.5:
            requests[self.rng.choice(self.resources)] = 1
        template: dict[str, Any] = {
            "command": [sys.executable, "-m", WORKLOAD, "--label", f"chaos-{name}"],
            "resources": {"requests": requests},
            "env": {"v": "1"},
        }
        if self.rng.random() < 0.6:
            template["readiness_probe"] = {"exec": self._probe("flap"), "period_seconds": 1.0}
        if self.rng.random() < 0.4:
            template["liveness_probe"] = {
                "exec": self._probe("sick"), "period_seconds": 1.0, "failure_threshold": 2,
            }  # fmt: skip
        surge, unavailable = self.rng.choice([(1, 0), (0, 1), (1, 1), (2, 0)])
        dep = Deployment.model_validate({
            "metadata": {"name": name},
            "spec": {"replicas": replicas, "template": template,
                     "strategy": {"max_surge": surge, "max_unavailable": unavailable}},
        })  # fmt: skip
        with contextlib.suppress(StoreError):
            await self.api.create(dep)
            self.deployments.append(name)
            self.note(
                f"deploy {name} x{replicas} {requests} surge {surge} unavailable "
                f"{unavailable} probes {sorted(k for k in template if 'probe' in k)}"
            )

    def _probe(self, fault: str) -> list[str]:
        """An exec probe that fails while this node has the fault's flag file."""
        flag = f"{self.flags}/{fault}-$CHRONOS_NODE_NAME"
        return ["sh", "-c", f'test ! -e "{flag}"']

    # ---- faults and actions

    async def _play(self, step: Step) -> None:
        await clock.sleep(step.at)
        record = {**asdict(step), "started": round(self.now(), 3)}
        self.report.steps.append(record)
        name, d = step.target, step.duration
        self.note(f"{step.kind} {name} {d:g}s {step.arg or ''}".strip())
        fault = step.kind not in ACTIONS
        self.faults += fault
        try:
            await self._act(step)
        finally:
            self.faults -= fault
            if fault:
                self.last_fault = self.now()
        record["ended"] = round(self.now(), 3)
        self.note(f"healed {step.kind} {name}".strip())

    async def _act(self, step: Step) -> None:  # noqa: PLR0912, PLR0915 (one per kind)
        name, d = step.target, step.duration
        c = self.cluster
        match step.kind:
            case "scale" if self.deployments:
                await self._scale(self.rng.choice(self.deployments), step.arg["replicas"])
            case "deploy":
                await self._deploy(f"d{len(self.deployments) + 1}", step.arg["replicas"] or 1)
            case "update" if self.deployments:
                await self._update(self.rng.choice(self.deployments), step.arg["version"])
            case "flap" | "sick":
                self.flags.mkdir(exist_ok=True)
                flag = self.flags / f"{step.kind}-{name}"
                flag.touch()
                await clock.sleep(d)
                flag.unlink(missing_ok=True)
            case "kill_cp" if self._cp_down:
                self.note("skipped kill_cp: chronosd is still restarting")
            case "kill_cp":
                self._cp_down = True
                c.kill_server()
                await clock.sleep(d)
                began = clock.monotonic()
                try:  # a cold start (Python + FastAPI imports) can be slow on a loaded machine
                    await c.start(within=60.0)
                except AssertionError:
                    self.violation("L4", "chronosd didn't come back within 60 s of a restart")
                    raise
                self.note(f"chronosd up again after {clock.monotonic() - began:.2f}s")
                self._cp_down = False
            case "kill_agent":
                with contextlib.suppress(OSError, ValueError):
                    c.signal_agent(name, signal.SIGKILL)  # shims and workloads stay
                await _reap(c.agents.pop(name, None))
                await clock.sleep(d)
                self.start_agent(name)
            case "kill_node":
                proc = c.agents.pop(name, None)
                kill_node(c.agent_dir(name))  # the agent and every pod's process group
                await _reap(proc)
                await clock.sleep(d)
                self.start_agent(name)
            case "pause_agent":
                with contextlib.suppress(OSError, ValueError):
                    c.signal_agent(name, signal.SIGSTOP)
                await clock.sleep(d)
                with contextlib.suppress(OSError, ValueError):
                    c.signal_agent(name, signal.SIGCONT)
            case "freeze_node":
                self.frozen[name] = math.inf
                with contextlib.suppress(OSError, ValueError):
                    c.signal_agent(name, signal.SIGSTOP)
                c.signal_pods(name, signal.SIGSTOP)
                await clock.sleep(d)
                c.signal_pods(name, signal.SIGCONT)
                with contextlib.suppress(OSError, ValueError):
                    c.signal_agent(name, signal.SIGCONT)
                self.frozen[name] = self.now()
            case "crash_storm":
                end = self.now() + d
                while self.now() < end:
                    victims = workloads(lambda p: p.node == name)
                    if victims:
                        with contextlib.suppress(psutil.Error):
                            psutil.Process(self.rng.choice(victims).pid).kill()
                    await clock.sleep(self.rng.uniform(0.2, 0.8))
            case "delay" | "drop" | "blackhole" | "reset":
                self.proxies[name].set(
                    Mode(
                        step.kind,
                        step.arg.get("ms", 0),
                        step.arg.get("jitter", 0),
                        step.arg.get("p", 0),
                    )
                )
                await clock.sleep(d)
                self.proxies[name].set(PASS)
            case _:
                pass

    async def _update(self, name: str, version: int) -> None:
        """A template change: a rolling update."""
        assert self.api is not None
        for _ in range(10):
            try:
                dep = await self.api.get(Deployment, name)
                dep.spec.template.env = {"v": str(version)}
                await self.api.update(dep)
                self.note(f"update {name} -> v{version}")
                return
            except Conflict:
                continue
            except StoreError:
                return

    async def _scale(self, name: str, replicas: int) -> None:
        assert self.api is not None
        for _ in range(10):
            try:
                dep = await self.api.get(Deployment, name)
                dep.spec.replicas = replicas
                await self.api.update(dep)
                self.note(f"scale {name} -> {replicas}")
                return
            except Conflict:
                continue
            except StoreError:
                return

    async def _heal(self) -> None:
        for proxy in self.proxies.values():
            proxy.set(PASS)
        shutil.rmtree(self.flags, ignore_errors=True)
        for name in self.names:
            self.cluster.signal_pods(name, signal.SIGCONT)
            with contextlib.suppress(OSError, ValueError):
                self.cluster.signal_agent(name, signal.SIGCONT)
            proc = self.cluster.agents.get(name)
            if proc is None or proc.poll() is not None:
                self.start_agent(name)
        self.note("heal: everything restored")

    # ---- the live checker

    async def _check_forever(self) -> None:
        while True:
            await clock.sleep(SAMPLE)
            with contextlib.suppress(StoreError):  # chronosd down: the processes still are
                await self._check()

    async def _snapshot(self) -> tuple[list[Node], list[Pod], list[Deployment]] | None:
        assert self.api is not None
        try:
            nodes, _ = await self.api.list(Node)
            pods, _ = await self.api.list(Pod)
            deps, _ = await self.api.list(Deployment)
        except StoreError:
            return None
        return nodes, pods, deps

    async def _check(self) -> None:
        snap = await self._snapshot()
        procs = workloads(lambda p: p.node in self.capacity)
        self.report.samples += 1
        self.report.load_max = max(self.report.load_max, os.getloadavg()[0])
        now = self.now()
        if snap is not None:
            nodes, pods, deps = snap
            for n in nodes:
                if self.conditions.get(n.metadata.name) != n.status.condition:
                    self.conditions[n.metadata.name] = n.status.condition
                    self.note(f"node {n.metadata.name} {n.status.condition}")
            self._store_invariants(nodes, pods)
            for d in deps:
                self.replicas_seen.setdefault(d.metadata.uid, []).append((now, d.spec.replicas))
            self._s3(pods, deps, procs, now)
            self._s7(pods, deps, now)
        self._s12(procs)
        self._s4(procs, now)

    def _store_invariants(self, nodes: list[Node], pods: list[Pod]) -> None:
        caps = {n.metadata.name: n.spec.capacity for n in nodes}
        used: dict[str, dict[str, int]] = {}
        for p in pods:
            if not p.spec.node_name:
                continue
            first = self.bound.setdefault(p.metadata.uid, p.spec.node_name)
            if first != p.spec.node_name:
                self.violation("S1", f"pod {p.metadata.name} moved {first} -> {p.spec.node_name}")
            if p.status.phase not in TERMINAL_PHASES:
                used[p.spec.node_name] = add(used.get(p.spec.node_name, {}), p.requests)
        for node, total in used.items():
            for r, n in total.items():
                if n > caps.get(node, {}).get(r, 0):
                    inv = "S11" if is_extended(r) else "S2"
                    self.violation(
                        inv, f"node {node}: {r} {n} > capacity {caps.get(node, {}).get(r, 0)}"
                    )

    def _strike(self, key: str, bad: bool) -> bool:
        """True once `key` has been bad for CONSECUTIVE samples in a row."""
        if not bad:
            self.strikes.pop(key, None)
            return False
        self.strikes[key] += 1
        return self.strikes[key] == CONSECUTIVE

    def _counted(self, node: str, now: float) -> bool:
        """Running, not frozen, and not within one shim poll (+ a sample) of a thaw."""
        since = self.frozen.get(node)
        return since is None or (since != math.inf and now - since > 0.1 + SAMPLE)

    def _s3(
        self, pods: list[Pod], deps: list[Deployment], procs: list[Workload], now: float
    ) -> None:
        by_uid = {p.metadata.uid: p for p in pods}
        window = self.timings.termination_grace + 1.5  # scale-down: victims marked within it
        for d in deps:
            uid = d.metadata.uid
            allowed = max(
                (r for t, r in self.replicas_seen.get(uid, []) if now - t <= window),
                default=d.spec.replicas,
            )
            mine = [
                w for w in procs
                if w.label == f"chaos-{d.metadata.name}" and self._counted(w.node, now)
                and not _terminating(by_uid.get(w.uid))
            ]  # fmt: skip
            allowed += _surge(d, pods)  # a rollout may run replicas + max_surge
            if self._strike(f"S3/{uid}", len(mine) > allowed):
                self.violation(
                    "S3",
                    f"deployment {d.metadata.name}: {len(mine)} processes > {allowed} replicas",
                )

    def _s7(self, pods: list[Pod], deps: list[Deployment], now: float) -> None:
        """Rolling updates: surge always; availability only in calm windows, where no
        fault can take readiness away, so any loss is the controller's doing."""
        window = self.timings.termination_grace + 1.5
        calm = self.faults == 0 and now - self.last_fault > SETTLE and not self._cp_down
        for d in deps:
            uid, surge = d.metadata.uid, _surge(d, pods)
            active = [p for p in pods if p.metadata.owner == uid and is_active(p)]
            replicas = max(
                (r for t, r in self.replicas_seen.get(uid, []) if now - t <= window),
                default=d.spec.replicas,
            )
            bad = bool(surge) and len(active) > replicas + surge
            if self._strike(f"S7s/{uid}", bad):
                self.violation(
                    "S7", f"deployment {d.metadata.name}: {len(active)} active pods > "
                    f"replicas {replicas} + max_surge {surge} during a rollout",
                )  # fmt: skip
            ready = sum(p.status.ready for p in active)
            want = d.spec.replicas - d.spec.strategy.max_unavailable
            if not calm or self.floor.get(uid, (-1, 0))[0] != d.spec.replicas:
                # A new window (or a new target): from here on, ready may not fall below
                # the floor, or below what it is now if that's lower (still coming up).
                self.floor[uid] = (d.spec.replicas, min(want, ready)) if calm else (-1, 0)
                self._strike(f"S7a/{uid}", False)
                continue
            floor = self.floor[uid][1]
            if self._strike(f"S7a/{uid}", bool(surge) and ready < floor):
                self.violation(
                    "S7", f"deployment {d.metadata.name}: {ready} ready during a rollout in a "
                    f"calm window < {floor} (replicas - max_unavailable = {want})",
                )  # fmt: skip
            self.floor[uid] = (d.spec.replicas, max(floor, min(want, ready)))

    def _s4(self, procs: list[Workload], now: float) -> None:
        t = self.timings
        # F + K, plus the drift margin (the design's slack for running late), one shim
        # poll and one sample.
        bound = t.fence_timeout + t.termination_grace + t.drift_margin + 0.1 + SAMPLE
        for name in self.names:
            lease = self.cluster.agent_dir(name) / "lease"
            try:
                age = clock.monotonic() - float(lease.read_text())
            except (OSError, ValueError):
                continue
            live = [w for w in procs if w.node == name and self._counted(name, now)]
            if self._strike(f"S4/{name}", bool(live) and age > bound):
                self.violation(
                    "S4",
                    f"node {name}: {len(live)} workload(s) running {age:.2f}s after the last "
                    f"accepted heartbeat (bound {bound:.2f}s)",
                )

    def _s12(self, procs: list[Workload]) -> None:
        """From the processes themselves (`CHRONOS_ASSIGNED`): the units each live process
        really holds. Pod status lags (a fenced pod keeps its old `assigned` until it
        reports again), so checking status gave false alarms."""
        holders: dict[tuple[str, str, str], list[int]] = {}
        for w in procs:
            for r, unit in w.assigned:
                holders.setdefault((w.node, r, unit), []).append(w.pid)
        for (node, r, unit), pids in sorted(holders.items()):
            if self._strike(f"S12/{node}/{r}/{unit}", len(pids) > 1):
                self.violation("S12", f"node {node}: {r} unit {unit} held by pids {pids}")

    # ---- after the heal

    async def _liveness(self) -> None:
        assert self.api is not None
        start = clock.monotonic()
        problem = "never checked"
        while clock.monotonic() - start < QUIET:
            await clock.sleep(0.5)
            snap = await self._snapshot()
            if snap is None:
                problem = "chronosd unreachable"
                continue
            problem = self._converged(*snap, workloads(lambda p: p.node in self.capacity))
            if not problem:
                self.report.converged_after = round(clock.monotonic() - start, 2)
                self.note(f"converged {self.report.converged_after}s after the heal")
                return
        self.report.liveness = problem
        invariant = problem.split(":", 1)[0] if problem[:1] == "L" else "L4"
        self.violation(invariant, f"not converged {QUIET:g}s after the heal: {problem}")

    def _converged(  # noqa: PLR0911 (one return per finding)
        self, nodes: list[Node], pods: list[Pod], deps: list[Deployment], procs: list[Workload]
    ) -> str:
        for n in nodes:
            if n.status.condition != NodeCondition.READY:
                return f"L1: node {n.metadata.name} is {n.status.condition}"
        for d in deps:
            owned = [p for p in pods if p.metadata.owner == d.metadata.uid]
            active = [
                p
                for p in owned
                if p.status.phase not in TERMINAL_PHASES and p.metadata.deletion_timestamp is None
            ]
            if _surge(d, pods):
                # Fine only if stuck on new pods that fit nowhere (as in the simulator).
                if not any(not p.spec.node_name and p.status.reason == "Unschedulable"
                           and not is_outdated(p, template_hash(d)) for p in active):  # fmt: skip
                    return f"L2: the rollout of {d.metadata.name} didn't finish"
            elif len(active) != d.spec.replicas:
                return (
                    f"L2: {d.metadata.name} has {len(active)} active pods, wants {d.spec.replicas}"
                )
            for p in active:
                if p.spec.node_name and (p.status.phase != PodPhase.RUNNING or not p.status.ready):
                    return f"L2: pod {p.metadata.name} is {p.status.phase} ({p.status.reason})"
                if not p.spec.node_name and p.status.reason != "Unschedulable":
                    return f"L3: pod {p.metadata.name} pending without a reason"
        by_uid = {p.metadata.uid: p for p in pods}
        counts = Counter(w.uid for w in procs)
        for w in procs:
            pod = by_uid.get(w.uid)
            if pod is None or pod.status.phase in TERMINAL_PHASES or pod.spec.node_name != w.node:
                return f"L5: orphan workload pid {w.pid} on {w.node}"
            if counts[w.uid] > 1:
                return f"L5: pod {pod.metadata.name} has {counts[w.uid]} processes"
        return ""


@dataclass(frozen=True)
class Workload:
    pid: int
    uid: str
    node: str
    label: str
    assigned: tuple[tuple[str, str], ...] = ()  # (resource, unit) this process holds


def workloads(keep: Any) -> list[Workload]:
    """Live (running or stopped, not zombie) test workloads, from the process table."""
    out = []
    for proc in psutil.process_iter(["cmdline", "status"]):
        try:
            argv = proc.info["cmdline"] or []
            if WORKLOAD not in argv or "chronos.shim" in argv:
                continue
            if proc.info["status"] == psutil.STATUS_ZOMBIE:
                continue
            env = proc.environ()
            i = argv.index("--label")
            units = json.loads(env.get("CHRONOS_ASSIGNED", "{}"))
            w = Workload(
                proc.pid,
                env.get("CHRONOS_POD_UID", ""),
                env.get("CHRONOS_NODE_NAME", ""),
                argv[i + 1],
                tuple((r, u) for r, us in sorted(units.items()) for u in us),
            )
        except (psutil.Error, ValueError, IndexError):
            continue
        if keep(w):
            out.append(w)
    return out


async def _reap(proc: subprocess.Popen[bytes] | None) -> None:
    """Wait for a killed process without blocking the event loop (the checker runs on it)."""
    while proc is not None and proc.poll() is None:
        await clock.sleep(0.05)


def _surge(d: Deployment, pods: list[Pod]) -> int:
    """max_surge while `d` rolls out (some active pod has an old template), else 0."""
    current = template_hash(d)
    rolling = any(
        p.metadata.owner == d.metadata.uid and is_active(p) and is_outdated(p, current)
        for p in pods
    )
    return d.spec.strategy.max_surge if rolling else 0


def _terminating(pod: Pod | None) -> bool:
    return pod is not None and pod.metadata.deletion_timestamp is not None


def _after(seconds: float | None) -> str:
    return "never" if seconds is None else f"after {seconds} s"


def write(report: Report, out: Path) -> Path:
    d = out / str(report.seed)
    d.mkdir(parents=True, exist_ok=True)
    data = {**asdict(report), "ok": report.ok, "timeline": [list(t) for t in report.timeline]}
    (d / "report.json").write_text(json.dumps(data, indent=2))
    lines = [f"# Chaos run, seed {report.seed}", "",
             f"{len(report.nodes)} nodes, {report.duration:g} s, {report.samples} samples, "
             f"{len(report.violations)} violations, max load {report.load_max:.1f}, converged "
             f"{_after(report.converged_after)}.",
             "", "| t (s) | what |", "|---|---|"]  # fmt: skip
    lines += [f"| {t:.2f} | {what} |" for t, what in sorted(report.timeline)]
    (d / "timeline.md").write_text("\n".join(lines) + "\n")
    return d


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m chronos.testing.chaos")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--out", type=Path, default=Path("chaos-report"))
    args = parser.parse_args(argv)
    tmp = Path(tempfile.mkdtemp(prefix="chronos-chaos-"))
    try:
        report = asyncio.run(Chaos(tmp, args.seed, args.nodes, args.duration).run())
    except Exception:
        # A harness failure: keep everything for the post-mortem.
        log = tmp / "chronosd.log"
        if log.exists():
            print(log.read_text()[-4000:], file=sys.stderr)
        print(f"chaos run failed; cluster files kept in {tmp}", file=sys.stderr)
        raise
    if not report.ok:
        print((tmp / "chronosd.log").read_text()[-3000:], file=sys.stderr)
        print(f"cluster files kept in {tmp}", file=sys.stderr)
    else:
        shutil.rmtree(tmp, ignore_errors=True)
    where = write(report, args.out)
    status = "ok" if report.ok else f"{len(report.violations)} violation(s)"
    print(f"seed {report.seed}: {status}; {report.samples} samples; report in {where}")
    for v in report.violations[:10]:
        print(f"  {v['t']:.2f}s {v['invariant']}: {v['message']}")
    return 0 if report.ok else 1


if __name__ == "__main__":
    os.environ.setdefault("PYTHONUNBUFFERED", "1")
    sys.exit(main())
