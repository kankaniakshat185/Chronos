"""HTTP client for the agent and the CLI (ADR-0004).

Every request has an explicit timeout. Transport failures (timeouts, refused
connections) raise `Unavailable`; error responses raise the same store exception
the server raised.
"""

from __future__ import annotations

import builtins
from collections.abc import Mapping

import httpx
from pydantic import BaseModel, ValidationError

from chronos.api import Deployment, Node, Pod
from chronos.api.kinds import PLURALS
from chronos.api.resources import format_resources
from chronos.api.wire import (
    BindingRequest,
    ErrorBody,
    HeartbeatReply,
    HeartbeatRequest,
    RegisterReply,
    RegisterRequest,
)
from chronos.common.config import FencingConfig
from chronos.store.errors import StoreError, Unavailable, from_body

PREFIX = "/api/v1"


class HttpStoreClient:
    def __init__(
        self,
        base_url: str,
        *,
        request_timeout: float = 2.0,
        heartbeat_timeout: float = 0.8,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._http = httpx.AsyncClient(base_url=base_url, transport=transport)
        self._timeout = request_timeout
        self._heartbeat_timeout = heartbeat_timeout

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> HttpStoreClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        body: BaseModel | None = None,
        params: Mapping[str, str | int] | None = None,
        time_limit: float | None = None,
    ) -> httpx.Response:
        try:
            response = await self._http.request(
                method,
                PREFIX + path,
                content=body.model_dump_json() if body is not None else None,
                headers={"content-type": "application/json"},
                params=params,
                timeout=time_limit or self._timeout,
            )
        except httpx.TransportError as exc:
            raise Unavailable(f"{method} {path}: {exc!r}") from exc
        if response.is_success:
            return response
        try:
            raise from_body(ErrorBody.model_validate_json(response.content))
        except ValidationError:
            raise StoreError(f"{method} {path}: HTTP {response.status_code}") from None

    async def get[T: (Node, Pod, Deployment)](self, kind: type[T], name: str) -> T:
        response = await self._request("GET", f"/{PLURALS[kind]}/{name}")
        return kind.model_validate_json(response.content)

    async def list[T: (Node, Pod, Deployment)](
        self,
        kind: type[T],
        *,
        labels: Mapping[str, str] | None = None,
        node_name: str | None = None,
    ) -> tuple[builtins.list[T], int]:
        params: dict[str, str | int] = {}
        if labels:
            params["labels"] = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        if node_name is not None:
            params["node"] = node_name
        data = (await self._request("GET", f"/{PLURALS[kind]}", params=params)).json()
        return [kind.model_validate(item) for item in data["items"]], int(data["revision"])

    async def create[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        response = await self._request("POST", f"/{PLURALS[type(obj)]}", body=obj)
        return type(obj).model_validate_json(response.content)

    async def update[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        path = f"/{PLURALS[type(obj)]}/{obj.metadata.name}"
        response = await self._request("PUT", path, body=obj)
        return type(obj).model_validate_json(response.content)

    async def update_status[T: (Node, Pod, Deployment)](self, obj: T) -> T:
        path = f"/{PLURALS[type(obj)]}/{obj.metadata.name}/status"
        response = await self._request("PUT", path, body=obj)
        return type(obj).model_validate_json(response.content)

    async def delete[T: (Node, Pod, Deployment)](
        self, kind: type[T], name: str, expected_rv: int | None = None, *, force: bool = False
    ) -> T:
        params: dict[str, str | int] = {"force": "true"} if force else {}
        if expected_rv is not None:
            params["resource_version"] = expected_rv
        response = await self._request("DELETE", f"/{PLURALS[kind]}/{name}", params=params)
        return kind.model_validate_json(response.content)

    async def bind(self, pod: str, node: str, expected_rv: int) -> Pod:
        body = BindingRequest(node_name=node, resource_version=expected_rv)
        response = await self._request("POST", f"/pods/{pod}/binding", body=body)
        return Pod.model_validate_json(response.content)

    async def register(
        self,
        node: str,
        agent_epoch: str,
        capacity: Mapping[str, int],
        fencing: FencingConfig | None = None,
    ) -> RegisterReply:
        f = fencing or FencingConfig()
        # Canonical strings: a bare int would be re-read as whole cores for cpu.
        body = RegisterRequest.model_validate(
            {
                "agent_epoch": agent_epoch,
                "capacity": format_resources(capacity),
                "fence_timeout": f.fence_timeout,
                "termination_grace": f.termination_grace,
                "drift_margin": f.drift_margin,
            }
        )
        response = await self._request("POST", f"/nodes/{node}/register", body=body)
        return RegisterReply.model_validate_json(response.content)

    async def heartbeat(self, node: str, agent_epoch: str) -> HeartbeatReply:
        body = HeartbeatRequest(agent_epoch=agent_epoch)
        response = await self._request(
            "POST", f"/nodes/{node}/heartbeat", body=body, time_limit=self._heartbeat_timeout
        )
        return HeartbeatReply.model_validate_json(response.content)
