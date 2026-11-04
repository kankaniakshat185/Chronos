"""In-process client: the async façade over the synchronous store (ADR-0004, ADR-0007).

Every method runs the store call without awaiting in between, so on one event
loop the calls are serialized: the store keeps a single writer without a lock.
(A lock arrives with the WAL in PM-2, when writes start to await an fsync.)
"""

from __future__ import annotations

import builtins
from collections.abc import Mapping

from chronos.api import Deployment, Node, NodeSpec, NodeStatus, ObjectMeta, Pod
from chronos.api.resources import format_resources
from chronos.api.wire import HeartbeatReply
from chronos.store import NotFound, Store
from chronos.store.errors import EpochMismatch
from chronos.store.leases import Leases


class LocalStoreClient:
    def __init__(self, store: Store, leases: Leases) -> None:
        self.store = store
        self.leases = leases

    async def get[T: (Node, Pod, Deployment)](self, kind: type[T], name: str) -> T:
        return self.store.get(kind, name)

    async def list[T: (Node, Pod, Deployment)](
        self,
        kind: type[T],
        *,
        labels: Mapping[str, str] | None = None,
        node_name: str | None = None,
    ) -> tuple[builtins.list[T], int]:
        return self.store.list(kind, labels=labels, node_name=node_name)

    async def create[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        return self.store.create(obj)

    async def update[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        return self.store.update(obj)

    async def update_status[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        return self.store.update_status(obj)

    async def delete[T: (Node, Pod, Deployment)](
        self, kind: type[T], name: str, expected_rv: int | None = None
    ) -> T:
        gone = self.store.delete(kind, name, expected_rv)
        if kind is Node:
            self.leases.forget(name)
        return gone

    async def bind(self, pod: str, node: str, expected_rv: int) -> Pod:
        return self.store.bind(pod, node, expected_rv)

    async def register(self, node: str, agent_epoch: str, capacity: Mapping[str, int]) -> Node:
        """Create the node, or adopt it for a new agent incarnation (ADR-0011).

        The new epoch makes heartbeats from any older incarnation fail with
        EpochMismatch. The node turns Ready on the lifecycle controller's next tick.
        """
        if not agent_epoch:
            raise EpochMismatch("agent_epoch must be non-empty")
        try:
            current = self.store.get(Node, node)
        except NotFound:
            created = self.store.create(
                Node(
                    metadata=ObjectMeta(name=node),
                    spec=NodeSpec.model_validate({"capacity": format_resources(capacity)}),
                    status=NodeStatus(agent_epoch=agent_epoch),
                )
            )
            self.leases.renew(node)
            return created
        current.spec.capacity = dict(capacity)
        current = self.store.update(current)
        current.status.agent_epoch = agent_epoch
        current = self.store.update_status(current)
        self.leases.renew(node)
        return current

    async def heartbeat(self, node: str, agent_epoch: str) -> HeartbeatReply:
        current = self.store.get(Node, node)
        if not agent_epoch or agent_epoch != current.status.agent_epoch:
            raise EpochMismatch(f"Node {node!r}: agent_epoch {agent_epoch!r} is not current")
        self.leases.renew(node)
        return HeartbeatReply(condition=current.status.condition)
