"""Node lifecycle controller: heartbeat silence -> node condition (ADR-0011).

Every tick it recomputes each node's condition from its lease and CAS-writes the
transitions. Going NotReady also revokes the agent's epoch, so the old agent
incarnation must re-register before it counts as alive again.

Eviction (level-triggered): every tick, any still-active pod on a NotReady node is
marked `Failed/NodeLost`. The deployment controller then counts it as gone and
creates a replacement, and its finished-pod GC removes it later. MVP fault model
is crash-stop (ADR-0022): the node's processes are already dead, so a replacement
can't overlap them. Fencing makes that true beyond crash-stop (PM-1).
"""

from __future__ import annotations

import logging

from chronos.api import Node, NodeCondition, Pod, PodPhase
from chronos.api.pod import TERMINAL_PHASES
from chronos.client.local import LocalStoreClient
from chronos.common.clock import Clock
from chronos.common.config import Timings
from chronos.core.node_health import condition
from chronos.store import StoreError

log = logging.getLogger(__name__)


class NodeLifecycleController:
    def __init__(self, client: LocalStoreClient, clock: Clock, timings: Timings) -> None:
        self._client = client
        self._clock = clock
        self._timings = timings

    async def tick(self) -> None:
        nodes, _ = await self._client.list(Node)
        for node in nodes:
            if await self._transition(node) == NodeCondition.NOT_READY:
                await self._evict(node.metadata.name)

    async def _evict(self, node: str) -> None:
        pods, _ = await self._client.list(Pod, node_name=node)
        for pod in pods:
            if pod.status.phase in TERMINAL_PHASES:
                continue
            pod.status.phase, pod.status.reason = PodPhase.FAILED, "NodeLost"
            pod.status.message = f"node {node} is NotReady"
            pod.status.ready = False
            try:
                await self._client.update_status(pod)
            except StoreError as exc:  # e.g. the agent's last status write: next tick
                log.info("pod %s: eviction deferred: %s", pod.metadata.name, exc)
                continue
            log.info("pod %s: evicted (node %s NotReady)", pod.metadata.name, node)

    async def _transition(self, node: Node) -> NodeCondition:
        """Write the node's new condition if it changed; return the condition now stored."""
        name = node.metadata.name
        if node.status.condition == NodeCondition.NOT_READY and not node.status.agent_epoch:
            return node.status.condition  # epoch revoked: NotReady until it re-registers
        silence = self._client.leases.silence(name)
        new = condition(silence, self._timings.node_grace, self._timings.eviction_timeout)
        if new == node.status.condition:
            return new
        old = node.status.condition
        node.status.condition = new
        if new == NodeCondition.NOT_READY:
            node.status.agent_epoch = ""
        try:
            await self._client.update_status(node)
        except StoreError as exc:  # e.g. a concurrent re-register; next tick retries
            log.info("node %s: transition to %s deferred: %s", name, new, exc)
            return old
        log.info("node %s: %s (silent %.2fs)", name, new, silence)
        return new

    async def run(self) -> None:
        while True:
            await self.tick()
            await self._clock.sleep(self._timings.lifecycle_tick)
