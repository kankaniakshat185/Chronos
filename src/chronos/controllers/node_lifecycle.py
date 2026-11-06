"""Node lifecycle controller: heartbeat silence -> node condition (ADR-0011).

Keys are node names. A node's condition depends on time passing without a
heartbeat, which no store event announces, so this controller resyncs every
`lifecycle_tick` (a monitor period, as in Kubernetes' node lifecycle controller)
rather than every 30 s. Going NotReady also revokes the agent's epoch, so the old
agent incarnation must re-register before it counts as alive again.

Eviction (level-triggered): any still-active pod on a NotReady node is marked
`Failed/NodeLost`. The deployment controller then counts it as gone and creates a
replacement, and its finished-pod GC removes it later. Fencing (ADR-0012) makes
sure the node's processes are gone by then. Eviction is destructive, so before it
the node's condition is confirmed with a live read: a stale cache must never evict
pods from a node that has already come back.
"""

from __future__ import annotations

import logging

from chronos.api import Node, NodeCondition, Pod, PodPhase
from chronos.api.pod import TERMINAL_PHASES
from chronos.client.local import LocalStoreClient
from chronos.common.clock import Clock
from chronos.common.config import Timings
from chronos.controllers.runtime import Controller, Informers, register_controller
from chronos.core.node_health import condition
from chronos.store import StoreError

log = logging.getLogger(__name__)

POD_NODE = "node"


def pod_node(pod: Pod) -> str | None:
    return pod.spec.node_name or None


class NodeLifecycleController(Controller):
    name = "node-lifecycle"

    def __init__(
        self,
        client: LocalStoreClient,
        clock: Clock,
        timings: Timings,
        informers: Informers | None = None,
    ) -> None:
        informers = informers or Informers(client, clock)
        self._client = client
        self._timings = timings
        self._nodes = informers.get(Node)
        self._pods = informers.get(Pod)
        self._pods.add_index(POD_NODE, pod_node)
        super().__init__(clock, [self._nodes, self._pods], resync_period=timings.lifecycle_tick)
        self._nodes.add_handler(lambda old, new: self.queue.add(_name(new or old)))

        def on_pod(old: Pod | None, new: Pod | None) -> None:
            pod = new or old
            if pod is not None and (node := pod_node(pod)) is not None:
                self.queue.add(node)  # a NotReady node may have a pod left to evict

        self._pods.add_handler(on_pod)

    def keys(self) -> list[str]:
        return self._nodes.keys()

    async def reconcile(self, key: str) -> None:
        node = self._nodes.get(key)
        if node is not None and await self._transition(node) == NodeCondition.NOT_READY:
            await self._evict(key)

    async def _evict(self, node: str) -> None:
        pods = self._pods.by_index(POD_NODE, node)
        active = [p for p in pods if p.status.phase not in TERMINAL_PHASES]
        if not active:
            return
        live = await self._client.get(Node, node)
        if live.status.condition != NodeCondition.NOT_READY:
            return
        for pod in active:
            pod.status.phase, pod.status.reason = PodPhase.FAILED, "NodeLost"
            pod.status.message = f"node {node} is NotReady"
            pod.status.ready = False
            try:
                await self._client.update_status(pod)
            except StoreError as exc:  # e.g. the agent's last status write: next pass
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
        except StoreError as exc:  # e.g. a concurrent re-register; the next pass retries
            log.info("node %s: transition to %s deferred: %s", name, new, exc)
            return old
        log.info("node %s: %s (silent %.2fs)", name, new, silence)
        return new


def _name(obj: Node | None) -> str:
    assert obj is not None
    return obj.metadata.name


register_controller(
    NodeLifecycleController.name,
    lambda ctx: NodeLifecycleController(ctx.client, ctx.clock, ctx.timings, ctx.informers),
)
