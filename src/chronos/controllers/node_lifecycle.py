"""Node lifecycle controller: heartbeat silence -> node condition (ADR-0011).

Every tick it recomputes each node's condition from its lease and CAS-writes the
transitions. Going NotReady also revokes the agent's epoch, so the old agent
incarnation must re-register before it counts as alive again. Eviction of the
node's pods is added on Day 6 (M6.3).
"""

from __future__ import annotations

import logging

from chronos.api import Node, NodeCondition
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
            name = node.metadata.name
            if node.status.condition == NodeCondition.NOT_READY and not node.status.agent_epoch:
                continue  # its epoch was revoked: NotReady until an agent re-registers
            silence = self._client.leases.silence(name)
            new = condition(silence, self._timings.node_grace, self._timings.eviction_timeout)
            if new == node.status.condition:
                continue
            node.status.condition = new
            if new == NodeCondition.NOT_READY:
                node.status.agent_epoch = ""
            try:
                await self._client.update_status(node)
            except StoreError as exc:  # e.g. a concurrent re-register; next tick retries
                log.info("node %s: transition to %s deferred: %s", name, new, exc)
                continue
            log.info("node %s: %s (silent %.2fs)", name, new, silence)

    async def run(self) -> None:
        while True:
            await self.tick()
            await self._clock.sleep(self._timings.lifecycle_tick)
