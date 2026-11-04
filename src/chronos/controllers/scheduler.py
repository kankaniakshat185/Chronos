"""Scheduler controller (ADR-0013; MVP: polling, no assume cache, ADR-0022).

Every tick: read the pending pods and all nodes straight from the store, take the
pods oldest first, `pick_node`, and bind. A successful bind is added to the
in-tick view of that node, so later pods in the same tick see it. Anything that
fails (Conflict, InsufficientCapacity, a node that just went away) is simply
retried next tick from fresh state. The store re-checks capacity inside `bind`,
so S2/S11 hold whatever this loop decides.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from chronos.api import Node, Pod, PodPhase
from chronos.client.local import LocalStoreClient
from chronos.common.clock import Clock
from chronos.core.schedule import Bind, NodeInfo, Profile, pick_node
from chronos.store import StoreError

log = logging.getLogger(__name__)

UNSCHEDULABLE = "Unschedulable"
_EPOCH = datetime.min.replace(tzinfo=UTC)


def _age_key(pod: Pod) -> tuple[datetime, str]:
    return (pod.metadata.created_at or _EPOCH, pod.metadata.name)


class SchedulerController:
    def __init__(
        self, client: LocalStoreClient, clock: Clock, profile: Profile, tick: float
    ) -> None:
        self._client = client
        self._clock = clock
        self._profile = profile
        self._tick = tick

    async def tick(self) -> None:
        pending, _ = await self._client.list(Pod, node_name="")
        pending = [
            p
            for p in pending
            if p.status.phase == PodPhase.PENDING and p.metadata.deletion_timestamp is None
        ]
        if not pending:
            return
        nodes = {n.metadata.name: NodeInfo.of(n) for n in (await self._client.list(Node))[0]}
        for pod in sorted(pending, key=_age_key):
            decision = pick_node(pod, list(nodes.values()), self._profile)
            if isinstance(decision, Bind):
                await self._bind(pod, decision.node, nodes)
            else:
                await self._mark_unschedulable(pod, decision.message)

    async def _bind(self, pod: Pod, node: str, nodes: dict[str, NodeInfo]) -> None:
        name = pod.metadata.name
        try:
            await self._client.bind(name, node, pod.metadata.resource_version)
        except StoreError as exc:
            log.info("pod %s: bind to %s failed, retrying next tick: %s", name, node, exc)
            return
        nodes[node] = nodes[node].with_pod(pod)
        log.info("pod %s: bound to %s", name, node)

    async def _mark_unschedulable(self, pod: Pod, message: str) -> None:
        if (pod.status.reason, pod.status.message) == (UNSCHEDULABLE, message):
            return  # don't write the same status every tick
        pod.status.reason, pod.status.message = UNSCHEDULABLE, message
        try:
            await self._client.update_status(pod)
        except StoreError as exc:
            log.info("pod %s: unschedulable status not written: %s", pod.metadata.name, exc)
            return
        log.info("pod %s: %s", pod.metadata.name, message)

    async def run(self) -> None:
        while True:
            await self.tick()
            await self._clock.sleep(self._tick)
