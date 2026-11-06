"""The metric catalog (ADR-0024): every metric Chronos exposes is defined here.

Names and label sets are an API, golden-tested in `tests/golden/metrics.txt`, and the
Grafana dashboard may only use these. Labels are bounded sets (conditions, phases,
controller, node and resource names), never pod names or uids.

Counters and histograms are process-wide. Gauges that describe cluster state are
computed when scraped, from the store (`ControlPlaneCollector`) or the agent
(`AgentCollector`), so they can't drift from it.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Iterator, Mapping
from typing import TYPE_CHECKING

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Histogram,
    disable_created_metrics,
    generate_latest,
)
from prometheus_client.core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

if TYPE_CHECKING:
    from chronos.client.local import LocalStoreClient
    from chronos.controllers.runtime import Controller

disable_created_metrics()  # type: ignore[no-untyped-call]  # no `_created` series: clutter

# One registry per process kind, so chronosd doesn't export the agent's counters as 0s.
REGISTRY = CollectorRegistry(auto_describe=True)  # chronosd
AGENT_REGISTRY = CollectorRegistry(auto_describe=True)
_FAST = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5)

RECONCILE_SECONDS = Histogram(
    "chronos_reconcile_duration_seconds", "Time to reconcile one key.", ["controller"],
    registry=REGISTRY, buckets=_FAST,
)  # fmt: skip
SCHEDULING_SECONDS = Histogram(
    "chronos_scheduling_duration_seconds", "Time to place one pending pod (decide + bind).",
    registry=REGISTRY, buckets=_FAST,
)  # fmt: skip
BIND_CONFLICTS = Counter(
    "chronos_bind_conflicts", "Binds refused by the store (stale cache, no capacity).",
    registry=REGISTRY,
)  # fmt: skip
WAL_FSYNC_SECONDS = Histogram(
    "chronos_wal_fsync_duration_seconds", "Duration of each WAL sync to stable storage.",
    registry=REGISTRY, buckets=_FAST,
)  # fmt: skip
AGENT_FENCE_EVENTS = Counter(
    "chronos_agent_fence_events", "Times this agent entered Fenced mode.", registry=AGENT_REGISTRY
)
AGENT_HEARTBEAT_FAILURES = Counter(
    "chronos_agent_heartbeat_failures",
    "Heartbeats that weren't accepted.",
    registry=AGENT_REGISTRY,
)


class ProcessMetrics(Collector):
    """Hands on a process-wide registry's metrics to a per-scrape one."""

    def __init__(self, source: CollectorRegistry = REGISTRY) -> None:
        self._source = source

    def collect(self) -> Iterable[Metric]:
        return self._source.collect()


def render(*extra: Collector, source: CollectorRegistry = REGISTRY) -> bytes:
    """The Prometheus text exposition: `source`'s metrics plus `extra` collectors."""
    registry = CollectorRegistry(auto_describe=False)
    registry.register(ProcessMetrics(source))
    for collector in extra:
        registry.register(collector)
    return generate_latest(registry)


def _reasons(message: str) -> list[str]:
    """ "0/3 nodes available: Insufficient cpu (2/3 nodes), node NotReady (1/3 nodes)"
    -> ["Insufficient cpu", "node NotReady"]: bounded, unlike the message."""
    _, _, tail = message.partition(": ")
    return [r for r in (re.sub(r"\s*\(.*?\)\s*$", "", p).strip() for p in tail.split(", ")) if r]


class ControlPlaneCollector(Collector):
    def __init__(self, client: LocalStoreClient, controllers: Callable[[], list[Controller]]):
        self._client = client
        self._controllers = controllers

    def collect(self) -> Iterator[Metric]:
        from chronos.api import Node, NodeCondition, Pod, PodPhase  # noqa: PLC0415

        store = self._client.store
        nodes = [n for n in store.view(Node).values() if isinstance(n, Node)]
        pods = [p for p in store.view(Pod).values() if isinstance(p, Pod)]

        by_condition = GaugeMetricFamily(
            "chronos_nodes", "Nodes by condition.", labels=["condition"]
        )
        for condition in NodeCondition:
            count = sum(n.status.condition == condition for n in nodes)
            by_condition.add_metric([condition.value], count)
        yield by_condition

        by_phase = GaugeMetricFamily("chronos_pods", "Pods by phase.", labels=["phase"])
        for phase in PodPhase:
            by_phase.add_metric([phase.value], sum(p.status.phase == phase for p in pods))
        yield by_phase

        unschedulable: dict[str, int] = {}
        for p in pods:
            if not p.spec.node_name and p.status.reason == "Unschedulable":
                for reason in _reasons(p.status.message):
                    unschedulable[reason] = unschedulable.get(reason, 0) + 1
        family = GaugeMetricFamily(
            "chronos_unschedulable_pods", "Pending pods by why they don't fit.", labels=["reason"]
        )
        for reason, count in sorted(unschedulable.items()):
            family.add_metric([reason], count)
        yield family

        age = GaugeMetricFamily(
            "chronos_node_heartbeat_age_seconds", "Seconds since the node's last heartbeat.",
            labels=["node"],
        )  # fmt: skip
        allocated = GaugeMetricFamily(
            "chronos_resource_allocated", "Requests of bound, unfinished pods.",
            labels=["node", "resource"],
        )  # fmt: skip
        capacity = GaugeMetricFamily(
            "chronos_resource_capacity", "Node capacity.", labels=["node", "resource"]
        )
        for n in sorted(nodes, key=lambda n: n.metadata.name):
            name = n.metadata.name
            age.add_metric([name], self._client.leases.silence(name))
            used = store.allocated(name)
            for resource in sorted(n.spec.capacity.keys() | used.keys()):
                unit = 1000 if resource == "cpu" else 1  # base units: cores, bytes, units
                allocated.add_metric([name, resource], used.get(resource, 0) / unit)
                capacity.add_metric([name, resource], n.spec.capacity.get(resource, 0) / unit)
        yield age
        yield allocated
        yield capacity

        depth = GaugeMetricFamily(
            "chronos_workqueue_depth", "Keys waiting in each controller's queue.",
            labels=["controller"],
        )  # fmt: skip
        for controller in self._controllers():
            depth.add_metric([controller.name], len(controller.queue))
        yield depth


class AgentCollector(Collector):
    """`stats` is read on the metrics thread: the agent replaces it, never mutates it."""

    def __init__(self, stats: Callable[[], Mapping[str, int]]):
        self._stats = stats

    def collect(self) -> Iterator[Metric]:
        family = GaugeMetricFamily(
            "chronos_agent_processes", "Local workload processes by state.", labels=["state"]
        )
        stats = self._stats()
        for state in ("running", "exited"):
            family.add_metric([state], stats.get(state, 0))
        yield family


def catalog(text: str) -> list[str]:
    """`name{label,...}` lines from an exposition, for the golden test and dashboard lint."""
    seen: dict[str, set[str]] = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        head = line.split(" ")[0]
        name, _, labels = head.partition("{")
        keys = {kv.split("=")[0] for kv in labels.rstrip("}").split(",") if kv}
        name = re.sub(r"_(bucket|sum|count)$", "", name)
        keys.discard("le")
        seen.setdefault(name, set()).update(keys)
    return [f"{n}{{{','.join(sorted(k))}}}" for n, k in sorted(seen.items())]
