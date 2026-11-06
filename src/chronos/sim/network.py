"""The simulated network between agents and the control plane.

Each direction of each agent's connection is a `Link` with its own delay, drop
probability, delay spikes and cut state, so partitions can be one-way. A request
and its reply are separate messages: a reply can be lost after the server acted,
which is exactly the ambiguity real clients face. Callers time out as the real
`HttpStoreClient` does and get `Unavailable`.

Objects are deep-copied on both sides, as serialization would, so no two parties
ever share a model instance.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from pydantic import BaseModel

from chronos.api import ApiObject, Pod
from chronos.api.wire import HeartbeatReply, RegisterReply
from chronos.client.protocol import WATCH_TIMEOUT, WatchBatch, WatchEvent
from chronos.common.config import FencingConfig
from chronos.sim.engine import Sim, SimFuture
from chronos.store.errors import StoreError, Unavailable

CP = "cp"


@dataclass
class Link:
    low: float = 0.0005
    high: float = 0.005
    drop: float = 0.0
    spike: float = 0.0  # extra delay, up to this much, while a spike lasts
    cut: bool = False

    def delay(self, sim: Sim) -> float:
        d = sim.rng.uniform(self.low, self.high)
        return d + sim.rng.uniform(0.0, self.spike) if self.spike else d


class Network:
    def __init__(self, sim: Sim) -> None:
        self.sim = sim
        self.links: dict[tuple[str, str], Link] = {}

    def link(self, src: str, dst: str) -> Link:
        return self.links.setdefault((src, dst), Link())

    def send(self, src: str, dst: str, deliver: Callable[[], None], what: str) -> None:
        link = self.link(src, dst)
        if link.cut:
            self.sim.log(f"net cut {src}->{dst} {what}")
            return
        if link.drop and self.sim.rng.random() < link.drop:
            self.sim.log(f"net drop {src}->{dst} {what}")
            return
        self.sim.after(link.delay(self.sim), deliver, f"net {src}->{dst} {what}")


type Respond = Callable[[Any, StoreError | None], None]


class Server(Protocol):
    def serve(  # noqa: PLR0917
        self, node: str, method: str, args: tuple[Any, ...], kwargs: dict[str, Any],
        sent_at: float, respond: Respond,
    ) -> None: ...  # fmt: skip


def copy(value: Any) -> Any:
    """Deep copy, as if serialized: models, lists, tuples, watch batches."""
    if isinstance(value, BaseModel):
        return value.model_copy(deep=True)
    if isinstance(value, list):
        return [copy(v) for v in value]
    if isinstance(value, tuple):
        return tuple(copy(v) for v in value)
    if isinstance(value, WatchBatch):
        return WatchBatch(
            [WatchEvent(e.type, copy(e.object)) for e in value.events], value.revision
        )
    return value


class SimNetClient:
    """`StoreClient` for one agent, over the simulated network."""

    def __init__(
        self,
        sim: Sim,
        net: Network,
        node: str,
        server: Callable[[], Server | None],
        *,
        request_timeout: float,
        heartbeat_timeout: float,
    ) -> None:
        self._sim, self._net, self._node, self._server = sim, net, node, server
        self._timeout = request_timeout
        self._heartbeat_timeout = heartbeat_timeout

    async def _call(
        self, method: str, *args: Any, time_limit: float | None = None, **kwargs: Any
    ) -> Any:
        future = SimFuture()
        sent_at = self._sim.now
        args, kwargs = copy(args), {k: copy(v) for k, v in kwargs.items()}
        what = f"{method}({self._node})"

        def respond(result: Any, error: StoreError | None) -> None:
            def deliver() -> None:
                if error is not None:
                    future.set_exception(error)
                else:
                    future.set_result(copy(result))

            self._net.send(CP, self._node, deliver, f"reply {what}")

        def at_server() -> None:
            server = self._server()
            if server is None:
                self._sim.log(f"cp down: {what} unanswered")
                return
            server.serve(self._node, method, args, kwargs, sent_at, respond)

        self._net.send(self._node, CP, at_server, what)
        limit = time_limit or self._timeout
        self._sim.after(
            limit, lambda: future.set_exception(Unavailable(f"{what}: no reply in {limit:g}s"))
        )
        return await future

    async def get[T: ApiObject](self, kind: type[T], name: str) -> T:
        result: T = await self._call("get", kind, name)
        return result

    async def list[T: ApiObject](
        self,
        kind: type[T],
        *,
        labels: Mapping[str, str] | None = None,
        node_name: str | None = None,
    ) -> tuple[list[T], int]:
        result: tuple[list[T], int] = await self._call(
            "list", kind, labels=labels, node_name=node_name
        )
        return result

    async def create[T: ApiObject](self, obj: T) -> T:
        result: T = await self._call("create", obj)
        return result

    async def update[T: ApiObject](self, obj: T) -> T:
        result: T = await self._call("update", obj)
        return result

    async def update_status[T: ApiObject](self, obj: T) -> T:
        result: T = await self._call("update_status", obj)
        return result

    async def delete[T: ApiObject](
        self, kind: type[T], name: str, expected_rv: int | None = None, *, force: bool = False
    ) -> T:
        result: T = await self._call("delete", kind, name, expected_rv, force=force)
        return result

    async def bind(self, pod: str, node: str, expected_rv: int) -> Pod:
        result: Pod = await self._call("bind", pod, node, expected_rv)
        return result

    async def watch[T: ApiObject](
        self,
        kind: type[T],
        since: int,
        *,
        wait: float = WATCH_TIMEOUT,
        node_name: str | None = None,
    ) -> WatchBatch[T]:
        result: WatchBatch[T] = await self._call(
            "watch", kind, since, wait=wait, node_name=node_name, time_limit=wait + 5.0
        )
        return result

    async def register(
        self,
        node: str,
        agent_epoch: str,
        capacity: Mapping[str, int],
        fencing: FencingConfig | None = None,
    ) -> RegisterReply:
        result: RegisterReply = await self._call(
            "register", node, agent_epoch, dict(capacity), fencing
        )
        return result

    async def heartbeat(self, node: str, agent_epoch: str) -> HeartbeatReply:
        result: HeartbeatReply = await self._call(
            "heartbeat", node, agent_epoch, time_limit=self._heartbeat_timeout
        )
        return result
