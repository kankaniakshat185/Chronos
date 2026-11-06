"""Scheduling framework: filter and score plugins, a registry, and `pick_node` (ADR-0013, X-3).

Pure. Filters reject a node with a reason; scores rank the survivors; the highest
weighted sum wins, ties broken by node name, so placement is deterministic.

Plugins register by name (`register_filter`, `register_score`). A `SchedulerConfig`
names the enabled plugins and score weights, and `resolve()` turns it into a
`Profile`, rejecting unknown names. Built-in plugins register through the same
calls; the framework never refers to a plugin directly.

`FitsResources` iterates over resource names, with a name the node doesn't
advertise meaning capacity 0. That one rule gives the extended-resource hard
constraint (S11): a pod asking for `chronos.dev/slot` can only land on a node with
enough free slots, never anywhere else.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from pydantic import BaseModel, ConfigDict

from chronos.api import Node, NodeCondition, Pod
from chronos.api.quantity import CPU, MEMORY
from chronos.api.resources import add, insufficient, sub


@dataclass(frozen=True)
class NodeInfo:
    name: str
    condition: NodeCondition
    unschedulable: bool
    capacity: Mapping[str, int]
    allocated: Mapping[str, int] = field(default_factory=dict)

    @classmethod
    def of(cls, node: Node) -> NodeInfo:
        return cls(
            name=node.metadata.name,
            condition=node.status.condition,
            unschedulable=node.spec.unschedulable,
            capacity=dict(node.spec.capacity),
            allocated=dict(node.status.allocated),
        )

    def with_pod(self, pod: Pod) -> NodeInfo:
        """This node after the pod is bound to it."""
        return NodeInfo(
            self.name,
            self.condition,
            self.unschedulable,
            self.capacity,
            add(self.allocated, pod.requests),
        )

    @property
    def free(self) -> dict[str, int]:
        return sub(self.capacity, self.allocated)


type Filter = Callable[[Pod, NodeInfo], str | None]  # a reason if the node is rejected
type Score = Callable[[Pod, NodeInfo], int]  # 0..100, higher is better

FILTERS: dict[str, Filter] = {}
SCORES: dict[str, Score] = {}


class ConfigError(ValueError):
    pass


def register_filter(name: str, plugin: Filter) -> None:
    if name in FILTERS:
        raise ConfigError(f"filter plugin {name!r} is already registered")
    FILTERS[name] = plugin


def register_score(name: str, plugin: Score) -> None:
    if name in SCORES:
        raise ConfigError(f"score plugin {name!r} is already registered")
    SCORES[name] = plugin


class SchedulerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    filters: tuple[str, ...] = ("NodeReady", "NodeSchedulable", "FitsResources")
    scores: dict[str, int] = {"LeastAllocated": 1}  # name -> weight


@dataclass(frozen=True)
class Profile:
    filters: tuple[tuple[str, Filter], ...]
    scores: tuple[tuple[str, Score, int], ...]


def resolve(config: SchedulerConfig) -> Profile:
    unknown = [n for n in config.filters if n not in FILTERS]
    unknown += [n for n in config.scores if n not in SCORES]
    if unknown:
        raise ConfigError(f"unknown scheduler plugin(s): {', '.join(unknown)}")
    if bad := [n for n, w in config.scores.items() if w < 0]:
        raise ConfigError(f"negative score weight for: {', '.join(bad)}")
    return Profile(
        filters=tuple((n, FILTERS[n]) for n in config.filters),
        scores=tuple((n, SCORES[n], w) for n, w in sorted(config.scores.items())),
    )


@dataclass(frozen=True)
class Bind:
    node: str


@dataclass(frozen=True)
class Unschedulable:
    reasons: Mapping[str, int]  # reason -> number of nodes rejected for it
    total: int

    @property
    def message(self) -> str:
        if not self.total:
            return "0/0 nodes available: no nodes registered"
        parts = [f"{r} ({n}/{self.total} nodes)" for r, n in sorted(self.reasons.items())]
        return f"0/{self.total} nodes available: " + ", ".join(parts)


def pick_node(pod: Pod, nodes: list[NodeInfo], profile: Profile) -> Bind | Unschedulable:
    feasible: list[NodeInfo] = []
    reasons: Counter[str] = Counter()
    for node in nodes:
        reason = next((r for _, f in profile.filters if (r := f(pod, node))), None)
        if reason is None:
            feasible.append(node)
        else:
            reasons[reason] += 1
    if not feasible:
        return Unschedulable(dict(reasons), len(nodes))

    def total(node: NodeInfo) -> int:
        return sum(w * score(pod, node) for _, score, w in profile.scores)

    # Highest score wins; on a tie the smallest name (sort by name first, max is stable).
    best = max(sorted(feasible, key=lambda n: n.name), key=total)
    return Bind(best.name)


# ---- built-in plugins ----------------------------------------------------------


def node_ready(_: Pod, node: NodeInfo) -> str | None:
    return None if node.condition == NodeCondition.READY else f"node {node.condition}"


def node_schedulable(_: Pod, node: NodeInfo) -> str | None:
    return "node unschedulable" if node.unschedulable else None


def fits_resources(pod: Pod, node: NodeInfo) -> str | None:
    if missing := insufficient(pod.requests, node.free):
        return "Insufficient " + ", ".join(missing)
    return None


def least_allocated(pod: Pod, node: NodeInfo) -> int:
    """Spread: the more cpu/memory left after placing the pod, the higher the score."""
    after = add(node.allocated, pod.requests)
    scores = [
        100 * max(cap - after.get(r, 0), 0) // cap if (cap := node.capacity.get(r, 0)) else 0
        for r in (CPU, MEMORY)
    ]
    return sum(scores) // len(scores)


def most_allocated(pod: Pod, node: NodeInfo) -> int:
    """Pack: the more cpu/memory already used after placing the pod, the higher the score."""
    after = add(node.allocated, pod.requests)
    scores = [
        100 * min(after.get(r, 0), cap) // cap if (cap := node.capacity.get(r, 0)) else 0
        for r in (CPU, MEMORY)
    ]
    return sum(scores) // len(scores)


register_filter("NodeReady", node_ready)
register_filter("NodeSchedulable", node_schedulable)
register_filter("FitsResources", fits_resources)
register_score("LeastAllocated", least_allocated)
register_score("MostAllocated", most_allocated)
