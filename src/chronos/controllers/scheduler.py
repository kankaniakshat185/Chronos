"""Scheduler controller (ADR-0013): informer caches, a work queue, an assume cache.

Keys are pending pod names, queued oldest first. `reconcile(pod)` runs `pick_node`
over the cached nodes and binds through the store's `bind` transaction.

What a node has allocated is summed from the pod cache, which lags the store: right
after a bind, the cache still shows the pod unbound, and the next decision would
count that capacity as free. So a successful pick is first **assumed**: its requests
are added to the node until the pod informer shows the pod bound (then the cache
counts it), the bind fails, the pod goes away, or `assume_ttl` passes. This is
kube-scheduler's assume cache. The store re-checks capacity inside `bind` anyway,
so S2/S11 hold whatever the caches say; the assume cache is what keeps a burst of
pods from losing binds to InsufficientCapacity.

Pods with no feasible node get an `Unschedulable` reason and wait: any node change,
or a pod that frees capacity, queues every pending pod again.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import UTC, datetime

from chronos.api import Node, Pod, PodPhase
from chronos.api.pod import TERMINAL_PHASES
from chronos.api.resources import add, sub
from chronos.client.local import LocalStoreClient
from chronos.common.clock import Clock
from chronos.controllers.runtime import Controller, Informers, register_controller
from chronos.core.schedule import Bind, NodeInfo, Profile, SchedulerConfig, pick_node, resolve

log = logging.getLogger(__name__)

UNSCHEDULABLE = "Unschedulable"
_EPOCH = datetime.min.replace(tzinfo=UTC)


def _age_key(pod: Pod) -> tuple[datetime, str]:
    return (pod.metadata.created_at or _EPOCH, pod.metadata.name)


def is_pending(pod: Pod) -> bool:
    return (
        not pod.spec.node_name
        and pod.status.phase == PodPhase.PENDING
        and pod.metadata.deletion_timestamp is None
    )


def _holds_capacity(pod: Pod | None) -> bool:
    return pod is not None and bool(pod.spec.node_name) and pod.status.phase not in TERMINAL_PHASES


@dataclass(frozen=True)
class Assumed:
    node: str
    requests: dict[str, int]
    expires: float


class SchedulerController(Controller):
    name = "scheduler"

    def __init__(
        self,
        client: LocalStoreClient,
        clock: Clock,
        profile: Profile,
        informers: Informers | None = None,
        *,
        resync_period: float = 30.0,
        assume_ttl: float = 30.0,
    ) -> None:
        informers = informers or Informers(client, clock)
        self._client = client
        self._profile = profile
        self._assume_ttl = assume_ttl
        self._nodes = informers.get(Node)
        self._pods = informers.get(Pod)
        self.assumed: dict[str, Assumed] = {}
        self._allocated: dict[str, dict[str, int]] = {}  # node -> bound, unfinished pods
        super().__init__(clock, [self._nodes, self._pods], resync_period=resync_period)
        self._nodes.add_handler(lambda old, new: self.resync())
        self._pods.add_handler(self._on_pod)

    def _on_pod(self, old: Pod | None, new: Pod | None) -> None:
        if _holds_capacity(old):
            assert old is not None
            node = old.spec.node_name
            self._allocated[node] = sub(self._allocated[node], old.requests)
        if _holds_capacity(new):
            assert new is not None
            node = new.spec.node_name
            self._allocated[node] = add(self._allocated.get(node, {}), new.requests)
        pod = new or old
        assert pod is not None
        if new is None or new.spec.node_name:
            self.assumed.pop(pod.metadata.name, None)  # now counted from the cache (or gone)
        if new is not None and is_pending(new):
            self.queue.add(new.metadata.name)
        if _holds_capacity(old) and not _holds_capacity(new):
            self.resync()  # capacity was freed: pending pods may fit now

    def keys(self) -> list[str]:
        return [
            p.metadata.name for p in sorted(filter(is_pending, self._pods.list()), key=_age_key)
        ]

    def node_infos(self) -> list[NodeInfo]:
        """The cached nodes, with what the cache shows allocated plus what's assumed."""
        now = self.clock.monotonic()
        for name in [n for n, a in self.assumed.items() if a.expires <= now]:
            log.warning("pod %s: assumed binding never observed; dropped", name)
            del self.assumed[name]
        extra: dict[str, dict[str, int]] = {}
        for a in self.assumed.values():
            extra[a.node] = add(extra.get(a.node, {}), a.requests)
        return [
            replace(
                NodeInfo.of(node),
                allocated=add(
                    self._allocated.get(node.metadata.name, {}),
                    extra.get(node.metadata.name, {}),
                ),
            )
            for node in self._nodes.list()
        ]

    async def reconcile(self, key: str) -> None:
        pod = self._pods.get(key)
        if pod is None or not is_pending(pod) or key in self.assumed:
            return
        decision = pick_node(pod, self.node_infos(), self._profile)
        if not isinstance(decision, Bind):
            await self._mark_unschedulable(pod, decision.message)
            return
        self.assumed[key] = Assumed(
            decision.node, dict(pod.requests), self.clock.monotonic() + self._assume_ttl
        )
        try:
            await self._client.bind(key, decision.node, pod.metadata.resource_version)
        except Exception:
            self.assumed.pop(key, None)  # e.g. Conflict on a stale pod: requeued
            raise
        log.info("pod %s: bound to %s", key, decision.node)

    async def _mark_unschedulable(self, pod: Pod, message: str) -> None:
        if (pod.status.reason, pod.status.message) == (UNSCHEDULABLE, message):
            return  # don't write the same status again
        pod.status.reason, pod.status.message = UNSCHEDULABLE, message
        await self._client.update_status(pod)
        log.info("pod %s: %s", pod.metadata.name, message)


register_controller(
    SchedulerController.name,
    lambda ctx: SchedulerController(
        ctx.client,
        ctx.clock,
        resolve(SchedulerConfig()),
        ctx.informers,
        resync_period=ctx.timings.resync_period,
    ),
)
