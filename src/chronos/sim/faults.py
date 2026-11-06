"""Seeded scenarios: a cluster, and a schedule of user actions and faults (P5.3, PX.7).

The schedule is a plain list of `Entry`s, so a failing run can be minimized by
dropping entries (`chronos.sim.run.minimize`). Every fault heals by itself
(`at + duration`), so after the last one the run can check liveness.

Resource names are generated per scenario (X-7): each run invents 0-2 extended
resources and puts them on random nodes, never on all of them, so "a node that
doesn't advertise r has 0 of it" (S11) is always exercised.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

from chronos.api.quantity import CPU, MEMORY

FAULT_WINDOW = (3.0, 90.0)  # faults and actions happen in here (virtual seconds)
GIB = 2**30
MIB = 2**20


@dataclass(frozen=True)
class NodeSpec:
    name: str
    capacity: dict[str, int]  # internal units: millicores, bytes, extended units


@dataclass(frozen=True)
class Entry:
    at: float
    kind: str  # see KINDS
    target: str = ""  # a node or deployment name
    duration: float = 0.0
    arg: dict[str, object] = field(default_factory=dict)

    def describe(self) -> str:
        extra = " ".join(f"{k}={v}" for k, v in sorted(self.arg.items()))
        return f"{self.at:.3f} {self.kind} {self.target} {self.duration:.3f}s {extra}".strip()


KINDS = (
    "deploy", "scale", "undeploy",  # user actions
    "node_crash", "agent_crash", "agent_pause", "node_freeze",  # node faults
    "partition", "delay", "drop", "degrade",  # network faults (per node link)
    "crash_storm",  # workloads crash
    "update",  # a template change (rolling update)
    "flap", "sick",  # a node's readiness / liveness probes fail for a while (P10.1)
    "cp_crash",  # the control plane crashes and restarts from its WAL
)  # fmt: skip


@dataclass(frozen=True)
class Scenario:
    seed: int
    nodes: list[NodeSpec]
    resources: list[str]  # extended resource names in this run
    entries: list[Entry]

    def without(self, index: int) -> Scenario:
        return Scenario(
            self.seed, self.nodes, self.resources, self.entries[:index] + self.entries[index + 1 :]
        )

    @property
    def heal_time(self) -> float:
        return max([FAULT_WINDOW[1], *(e.at + e.duration for e in self.entries)])


def generate(seed: int) -> Scenario:  # noqa: PLR0912 (one branch per entry kind)
    rng = random.Random(f"scenario-{seed}")
    tier2 = random.Random(f"scenario-{seed}-tier2")
    resources = [f"sim{seed % 97}.dev/r{i}" for i in range(rng.choice([0, 1, 1, 2]))]
    nodes = []
    for i in range(rng.randint(2, 6)):
        cap = {CPU: rng.choice([2000, 4000, 8000]), MEMORY: rng.choice([4, 8, 16]) * GIB}
        for name in resources:
            if i > 0 and rng.random() < 0.6:  # node 0 never has extended resources
                cap[name] = rng.randint(1, 4)
        nodes.append(NodeSpec(f"n{i + 1}", cap))
    names = [n.name for n in nodes]

    entries: list[Entry] = []
    deployments = []
    for d in range(rng.randint(1, 3)):  # at the start
        deployments.append(f"d{d + 1}")
        entries.append(_deploy(rng, tier2, f"d{d + 1}", rng.uniform(0.5, 2.0), resources))
    lo, hi = FAULT_WINDOW
    for _ in range(rng.randint(2, 10)):
        at = round(rng.uniform(lo, hi), 3)
        kind = rng.choice(
            ["scale"] * 3 + ["deploy", "undeploy"]
            + ["agent_crash", "agent_pause", "node_freeze"] * 2
            + ["node_crash", "partition", "partition", "delay", "drop", "degrade", "crash_storm"]
            + ["cp_crash"]
        )  # fmt: skip
        if kind == "deploy":
            name = f"d{len(deployments) + 1}"
            deployments.append(name)
            entries.append(_deploy(rng, tier2, name, at, resources))
        elif kind in ("scale", "undeploy"):
            entries.append(
                Entry(at, kind, rng.choice(deployments), 0.0, {"replicas": rng.randint(0, 6)})
            )
        elif kind == "cp_crash":
            down = rng.choice([rng.uniform(0.2, 2.0), rng.uniform(3.0, 10.0)])  # within F, or not
            entries.append(Entry(at, kind, "cp", round(down, 3)))
        else:
            duration = round(rng.choice([rng.uniform(0.2, 3.0), rng.uniform(3.0, 15.0)]), 3)
            arg: dict[str, object] = {}
            if kind == "partition":
                arg = {"dir": rng.choice(["both", "both", "up", "down"])}
            elif kind == "delay":
                arg = {"spike": round(rng.uniform(0.5, 2.5), 3)}
            elif kind == "degrade":
                duration = round(rng.uniform(6.0, 15.0), 3)
                arg = {"spike": round(rng.uniform(1.0, 3.0), 3),
                       "cut_after": round(rng.uniform(1.0, 4.0), 3)}  # fmt: skip
            elif kind == "drop":
                arg = {"p": round(rng.uniform(0.1, 0.3), 3)}
            elif kind == "crash_storm":
                arg = {"rate": round(rng.uniform(0.2, 2.0), 3)}
            entries.append(Entry(at, kind, rng.choice(names), duration, arg))
    # Tier 2 (P10.5) from a stream of its own, so every seed keeps the schedule it had
    # before: the seeds in tests/sim/regressions.txt still replay what found their bugs.
    for _ in range(tier2.randint(1, 4)):
        at = round(tier2.uniform(lo, hi), 3)
        kind = tier2.choice(["update", "update", "flap", "sick"])
        if kind == "update":  # a template change: a rolling update (P10.2)
            version: dict[str, object] = {"version": tier2.randint(2, 99)}
            entries.append(Entry(at, kind, tier2.choice(deployments), 0.0, version))
        else:  # a node's readiness (flap) or liveness (sick) probes fail for a while
            duration = round(tier2.choice([tier2.uniform(0.2, 3.0), tier2.uniform(3.0, 15.0)]), 3)
            entries.append(Entry(at, kind, tier2.choice(names), duration))
    entries.sort(key=lambda e: (e.at, e.kind, e.target))
    return Scenario(seed, nodes, resources, entries)


def _deploy(
    rng: random.Random, tier2: random.Random, name: str, at: float, resources: list[str]
) -> Entry:
    requests: dict[str, object] = {
        CPU: rng.choice([100, 250, 500, 1000]),
        MEMORY: rng.choice([64, 128, 256, 512]) * MIB,
    }
    if resources and rng.random() < 0.5:
        requests[rng.choice(resources)] = 1
    replicas = rng.randint(1, 5)
    surge, unavailable = tier2.choice([(1, 0), (0, 1), (1, 1), (2, 0), (2, 1)])
    return Entry(
        round(at, 3), "deploy", name, 0.0,
        {"replicas": replicas, "requests": requests, "surge": surge,
         "unavailable": unavailable, "readiness": tier2.random() < 0.6,
         "liveness": tier2.random() < 0.4},
    )  # fmt: skip
