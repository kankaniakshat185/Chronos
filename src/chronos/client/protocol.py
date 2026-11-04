"""The `StoreClient` protocol (ADR-0004).

Controllers in `chronosd` use `LocalStoreClient`; the agent and CLI use
`HttpStoreClient`. One contract test suite runs against both, so they can't drift.
Errors are the store's own exceptions (`chronos.store.errors`) on both sides.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from chronos.api import Deployment, Node, Pod
from chronos.api.wire import HeartbeatReply, RegisterReply
from chronos.common.config import FencingConfig


class StoreClient(Protocol):
    async def get[T: (Node, Pod, Deployment)](self, kind: type[T], name: str) -> T: ...

    async def list[T: (Node, Pod, Deployment)](
        self,
        kind: type[T],
        *,
        labels: Mapping[str, str] | None = None,
        node_name: str | None = None,
    ) -> tuple[list[T], int]: ...

    async def create[T: (Node, Pod, Deployment)](self, obj: T) -> T: ...

    async def update[T: (Node, Pod, Deployment)](self, obj: T) -> T: ...

    async def update_status[T: (Node, Pod, Deployment)](self, obj: T) -> T: ...

    async def delete[T: (Node, Pod, Deployment)](
        self, kind: type[T], name: str, expected_rv: int | None = None, *, force: bool = False
    ) -> T:
        """Bound, unfinished pods are only marked terminating unless `force` (P4.7)."""
        ...

    async def bind(self, pod: str, node: str, expected_rv: int) -> Pod: ...

    async def register(
        self,
        node: str,
        agent_epoch: str,
        capacity: Mapping[str, int],
        fencing: FencingConfig | None = None,
    ) -> RegisterReply:
        """`capacity` is in internal units: cpu in millicores, memory in bytes.

        Refused (Invalid) if `fencing` would break F + K + drift < E on this server.
        """
        ...

    async def heartbeat(self, node: str, agent_epoch: str) -> HeartbeatReply: ...
