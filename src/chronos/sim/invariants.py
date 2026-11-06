"""Invariants, checked against the simulated world (TESTING.md section 2).

Safety after **every** event; liveness once everything has healed and the run has
been quiet for a while. Each check states its bound from the real timings, so a
violation is a real breach of what the README promises, not a tuning artefact.

Store-based checks read the control plane's store directly (no copies) and are
skipped while it's down; process-based checks use the last store state seen.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from chronos.api import Deployment, Node, NodeCondition, Pod, PodPhase
from chronos.api.pod import TERMINAL_PHASES
from chronos.api.quantity import is_extended
from chronos.api.resources import add
from chronos.core.reconcile import is_active, template_hash
from chronos.core.rollout import is_outdated
from chronos.core.schedule import Bind, NodeInfo, Profile, pick_node
from chronos.store.kinds import NODE_INDEX, info

if TYPE_CHECKING:
    from chronos.sim.run import World

EPS = 1e-6


class Violation(Exception):
    def __init__(self, invariant: str, message: str) -> None:
        super().__init__(f"{invariant}: {message}")
        self.invariant = invariant
        self.message = message


@dataclass
class PodFacts:
    owner: str | None
    terminating: bool


class Checker:
    def __init__(self, world: World) -> None:
        self.w = world
        self.bound: dict[str, str] = {}  # pod uid -> the node it was first bound to (S1)
        self.revision = 0
        self.incarnation = -1
        self.replicas: dict[str, int] = {}  # deployment uid -> spec.replicas
        self.lowered: dict[str, tuple[int, float]] = {}  # uid -> (old replicas, allowed until)
        self.pods: dict[str, PodFacts] = {}
        self.surge: dict[str, int] = {}  # deployment uid -> max_surge, while it rolls out (S7)
        self._checked_informers: dict[int, int] = {}

    # ---- after every event

    def after_event(self) -> None:
        cp = self.w.cp
        if cp is not None and cp.alive:
            self._store(cp.store, self.w.incarnation)
            self._watches(cp.store)
        self._processes()

    def _store(self, store: object, incarnation: int) -> None:
        from chronos.store import Store  # noqa: PLC0415

        assert isinstance(store, Store)
        now = self.w.sim.now
        if incarnation == self.incarnation and store.revision < self.revision:
            raise Violation("S5", f"revision went back from {self.revision} to {store.revision}")
        self.incarnation, self.revision = incarnation, store.revision

        nodes = store.view(Node)
        used: dict[str, dict[str, int]] = {}
        pods: dict[str, PodFacts] = {}
        for pod in store.view(Pod).values():
            assert isinstance(pod, Pod)
            uid, node = pod.metadata.uid, pod.spec.node_name
            pods[uid] = PodFacts(pod.metadata.owner, pod.metadata.deletion_timestamp is not None)
            if node:
                first = self.bound.setdefault(uid, node)
                if first != node:
                    raise Violation("S1", f"pod {pod.metadata.name} moved from {first} to {node}")
                if pod.status.phase not in TERMINAL_PHASES:
                    used[node] = add(used.get(node, {}), pod.requests)
        removed = [uid for uid, f in pods.items() if f.terminating and uid in self.pods
                   and not self.pods[uid].terminating]  # fmt: skip
        self.pods = pods
        if removed:
            self._availability(store, removed)
        for name, total in used.items():
            n = nodes.get(name)
            capacity = n.spec.capacity if isinstance(n, Node) else {}
            for resource, amount in sorted(total.items()):
                if amount > capacity.get(resource, 0):
                    kind = "S11" if is_extended(resource) else "S2"
                    raise Violation(
                        kind,
                        f"node {name}: {resource} {amount} > capacity {capacity.get(resource, 0)}",
                    )

        replicas = {}
        for dep in store.view(Deployment).values():
            assert isinstance(dep, Deployment)
            uid, want = dep.metadata.uid, dep.spec.replicas
            before = self.replicas.get(uid)
            if before is not None and want < before:
                old = max(before, self.lowered.get(uid, (0, 0.0))[0])
                self.lowered[uid] = (old, now + self.w.scale_down_window)
            replicas[uid] = want
        self.replicas = replicas
        self._surge(store)

    def _surge(self, store: Any) -> None:
        """S7, surge: during a rollout at most replicas + max_surge active pods (or the
        old replicas + max_surge, within the scale-down window)."""
        now = self.w.sim.now
        by_owner: dict[str, list[Pod]] = {}
        for pod in store.view(Pod).values():
            if pod.metadata.owner and is_active(pod):
                by_owner.setdefault(pod.metadata.owner, []).append(pod)
        surge = {}
        for dep in store.view(Deployment).values():
            uid = dep.metadata.uid
            active = by_owner.get(uid, [])
            current = template_hash(dep)
            if any(is_outdated(p, current) for p in active):
                surge[uid] = dep.spec.strategy.max_surge
            if uid not in surge:
                continue  # not rolling out: S3 bounds the live processes
            allowed = dep.spec.replicas
            old, until = self.lowered.get(uid, (0, 0.0))
            if now < until:
                allowed = max(allowed, old)
            allowed += surge[uid]
            if len(active) > allowed:
                raise Violation(
                    "S7", f"deployment {dep.metadata.name}: {len(active)} active pods > "
                    f"{allowed} (replicas {dep.spec.replicas} + max_surge {surge[uid]})",
                )  # fmt: skip
        self.surge = surge

    def _availability(self, store: Any, removed: list[str]) -> None:
        """S7, availability: when the deployment controller removes a pod it saw ready
        during a rollout, its own cache must still show replicas - max_unavailable ready
        pods (among those not removed). Judged on the controller's view, because that's
        what it decides on; readiness lost behind its back is involuntary."""
        cp = self.w.cp
        if cp is None:
            return
        controller = cp.deployments
        cached = {p.metadata.uid: p for p in controller._pods.list()}
        deps = {d.metadata.uid: d for d in controller._deployments.list()}
        live = {p.metadata.uid for p in store.view(Pod).values() if is_active(p)}
        for uid in removed:
            pod = cached.get(uid)
            if pod is None or not pod.status.ready or pod.metadata.owner not in deps:
                continue
            dep = deps[pod.metadata.owner]
            owned = [p for p in cached.values()
                     if p.metadata.owner == dep.metadata.uid and is_active(p)]  # fmt: skip
            if not any(is_outdated(p, template_hash(dep)) for p in owned):
                continue  # not a rollout (a scale-down)
            ready = sum(p.status.ready for p in owned if p.metadata.uid in live)
            floor = dep.spec.replicas - dep.spec.strategy.max_unavailable
            if ready < floor:
                raise Violation(
                    "S7", f"deployment {dep.metadata.name}: removed ready pod "
                    f"{pod.metadata.name} leaving {ready} ready < replicas - max_unavailable "
                    f"= {floor} (in the controller's own cache)",
                )  # fmt: skip

    def _watches(self, store: object) -> None:
        """S6 at the consumer: an informer that says it has seen everything up to the
        store's revision must hold exactly the store's objects (for its filter)."""
        from chronos.store import Store  # noqa: PLC0415

        assert isinstance(store, Store)
        for informer, node in self.w.informers():
            if not informer.has_synced or informer.revision != store.revision:
                continue
            key = id(informer)
            if self._checked_informers.get(key) == store.revision:
                continue
            self._checked_informers[key] = store.revision
            index = info(informer.kind).indexes.get(NODE_INDEX)
            want = {
                name: obj.metadata.resource_version
                for name, obj in store.view(informer.kind).items()
                if node is None or (index is not None and index(obj) == node)
            }
            have = {name: obj.metadata.resource_version for name, obj in informer._items.items()}
            if have != want:
                missing = sorted(want.keys() - have.keys())
                extra = sorted(have.keys() - want.keys())
                stale = sorted(n for n in want.keys() & have.keys() if want[n] != have[n])
                where = f"{informer.kind.__name__} informer" + (f" on {node}" if node else "")
                raise Violation(
                    "S6",
                    f"{where} at revision {store.revision}: missing {missing}, extra {extra}, "
                    f"stale {stale}",
                )

    def _processes(self) -> None:
        w = self.w
        now = w.sim.now
        f, k, poll = w.fencing.fence_timeout, w.fencing.termination_grace, w.fencing.shim_poll
        per_deployment: Counter[str] = Counter()
        for node in w.nodes.values():
            exempt = node.frozen or now - node.resumed_at <= poll + EPS
            units: dict[tuple[str, str], int] = {}
            for proc in node.procs.values():
                for resource, ids in proc.assigned.items():
                    for unit in ids:
                        other = units.setdefault((resource, unit), proc.pid)
                        if other != proc.pid:
                            raise Violation(
                                "S12", f"node {node.name}: {resource} unit {unit} held by pids "
                                f"{other} and {proc.pid}",
                            )  # fmt: skip
                if exempt:
                    continue
                last = w.accepted_send.get(node.name, -math.inf)
                if now - last > f + poll + k + EPS:
                    raise Violation(
                        "S4", f"node {node.name}: pid {proc.pid} ({proc.pod}) still runs "
                        f"{now - last:.3f}s after the last accepted heartbeat was sent "
                        f"(bound F + poll + K = {f + poll + k:g}s)",
                    )  # fmt: skip
                facts = self.pods.get(proc.uid)
                if proc.owner is not None and not (facts is not None and facts.terminating):
                    per_deployment[proc.owner] += 1
        for uid, count in sorted(per_deployment.items()):
            if uid not in self.replicas:
                continue  # deleted: its pods are being collected (L5 checks the end state)
            allowed = self.replicas[uid]
            old, until = self.lowered.get(uid, (0, 0.0))
            if now < until:
                allowed = max(allowed, old)
            allowed += self.surge.get(uid, 0)  # a rollout may run replicas + max_surge (S7)
            if count > allowed:
                raise Violation(
                    "S3", f"deployment {uid}: {count} live processes > {allowed} replicas"
                )

    # ---- after the run, once quiet

    def liveness(self, profile: Profile) -> None:  # noqa: PLR0912 (one check after another)
        w = self.w
        cp = w.cp
        if cp is None or not cp.alive:
            raise Violation("L4", "the control plane isn't running at the end")
        store = cp.store
        nodes = {n: o for n, o in store.view(Node).items() if isinstance(o, Node)}
        for name, sim_node in sorted(w.nodes.items()):
            if (
                sim_node.agent is not None
                and name in nodes
                and nodes[name].status.condition != NodeCondition.READY
            ):
                raise Violation(
                    "L1", f"node {name} has a live agent but is {nodes[name].status.condition}"
                )
        pods = [p for p in store.view(Pod).values() if isinstance(p, Pod)]
        allocated: dict[str, dict[str, int]] = {}
        for p in pods:
            if p.spec.node_name and p.status.phase not in TERMINAL_PHASES:
                allocated[p.spec.node_name] = add(allocated.get(p.spec.node_name, {}), p.requests)
        infos = [
            NodeInfo(n.metadata.name, n.status.condition, n.spec.unschedulable,
                     dict(n.spec.capacity), allocated.get(n.metadata.name, {}))
            for n in nodes.values()
        ]  # fmt: skip
        live = {p.uid: p for node in w.nodes.values() for p in node.procs.values()}
        for dep in store.view(Deployment).values():
            assert isinstance(dep, Deployment)
            owned = [p for p in pods if p.metadata.owner == dep.metadata.uid]
            active = [
                p for p in owned
                if p.status.phase not in TERMINAL_PHASES and p.metadata.deletion_timestamp is None
            ]  # fmt: skip
            current = template_hash(dep)
            if any(is_outdated(p, current) for p in active):
                # An unfinished rollout is fine only if it's stuck on new pods that fit
                # nowhere (L3 below checks they really don't): with a full cluster and the
                # old pods holding the floor, there's no move that keeps S7 (Kubernetes
                # stalls the same way).
                if not any(not p.spec.node_name and not is_outdated(p, current) for p in active):
                    raise Violation(
                        "L2", f"deployment {dep.metadata.name}: the rollout didn't finish, and "
                        "no new pod is waiting for room",
                    )  # fmt: skip
                bound = dep.spec.replicas + dep.spec.strategy.max_surge
                if len(active) > bound:
                    raise Violation(
                        "L2", f"deployment {dep.metadata.name}: {len(active)} active pods "
                        f"> {bound} mid-rollout",
                    )  # fmt: skip
            elif len(active) != dep.spec.replicas:
                raise Violation(
                    "L2",
                    f"deployment {dep.metadata.name}: {len(active)} active pods, "
                    f"wants {dep.spec.replicas}",
                )
            for p in active:
                if not p.spec.node_name:
                    if isinstance(pick_node(p, infos, profile), Bind):
                        raise Violation(
                            "L3", f"pod {p.metadata.name} is pending but fits a Ready node"
                        )
                elif p.status.phase != PodPhase.RUNNING or not p.status.ready:
                    raise Violation(
                        "L2", f"pod {p.metadata.name} on {p.spec.node_name} is {p.status.phase} "
                        f"({p.status.reason}) after the run went quiet",
                    )  # fmt: skip
                elif p.metadata.uid not in live:
                    raise Violation("L2", f"pod {p.metadata.name} is Running with no process")
        by_uid = {p.metadata.uid: p for p in pods}
        counts = Counter(p.uid for node in w.nodes.values() for p in node.procs.values())
        for node in w.nodes.values():
            for proc in node.procs.values():
                pod = by_uid.get(proc.uid)
                if (
                    pod is None
                    or pod.spec.node_name != node.name
                    or pod.status.phase in TERMINAL_PHASES
                ):
                    raise Violation(
                        "L5", f"node {node.name}: orphan process {proc.pid} ({proc.pod})"
                    )
                if counts[proc.uid] > 1:
                    raise Violation("L5", f"pod {proc.pod} has {counts[proc.uid]} live processes")
