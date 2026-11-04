"""The `chronosd` HTTP API (PLAN.md section 4.4, ADR-0004).

Handlers are `async def` on purpose: they run on the event loop, one at a time
between awaits, so the synchronous store keeps a single writer. A plain `def`
handler would run in a thread pool and break that.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable, Coroutine

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ValidationError

from chronos.api import Deployment, Node, Pod
from chronos.api.kinds import PLURALS
from chronos.api.wire import BindingRequest, ErrorBody, HeartbeatRequest, RegisterRequest
from chronos.client.local import LocalStoreClient
from chronos.store import Invalid, StoreError

PREFIX = "/api/v1"

type Loop = Callable[[], Coroutine[None, None, None]]


def _json(model: BaseModel, status: int = 200) -> JSONResponse:
    return JSONResponse(model.model_dump(mode="json"), status_code=status)


def _error(status: int, body: ErrorBody) -> JSONResponse:
    return JSONResponse(body.model_dump(mode="json"), status_code=status)


async def _parse[M: BaseModel](request: Request, model: type[M]) -> M:
    try:
        return model.model_validate_json(await request.body())
    except ValidationError as exc:
        raise Invalid(_describe(exc)) from None


def _describe(exc: ValidationError) -> str:
    return "; ".join(
        f"{'.'.join(str(p) for p in err['loc']) or 'body'}: {err['msg']}" for err in exc.errors()
    )


def _parse_labels(raw: str | None) -> dict[str, str] | None:
    if not raw:
        return None
    labels = {}
    for pair in raw.split(","):
        key, sep, value = pair.partition("=")
        if not sep or not key:
            raise Invalid(f"labels: expected key=value pairs, got {pair!r}")
        labels[key] = value
    return labels


def create_app(client: LocalStoreClient, loops: list[Loop] | None = None) -> FastAPI:
    """`loops` are the controllers' run() coroutines, started with the server."""

    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        tasks = [asyncio.create_task(loop()) for loop in loops or []]
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    app = FastAPI(title="chronosd", lifespan=lifespan)

    @app.exception_handler(StoreError)
    async def store_error(_: Request, exc: StoreError) -> JSONResponse:
        return _error(exc.status, exc.body())

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    _add_kind_routes(app, client, Node)
    _add_kind_routes(app, client, Pod)
    _add_kind_routes(app, client, Deployment)

    @app.post(PREFIX + "/pods/{name}/binding")
    async def bind(name: str, request: Request) -> JSONResponse:
        body = await _parse(request, BindingRequest)
        return _json(await client.bind(name, body.node_name, body.resource_version), 201)

    @app.post(PREFIX + "/nodes/{name}/register")
    async def register(name: str, request: Request) -> JSONResponse:
        body = await _parse(request, RegisterRequest)
        return _json(await client.register(name, body.agent_epoch, body.capacity))

    @app.post(PREFIX + "/nodes/{name}/heartbeat")
    async def heartbeat(name: str, request: Request) -> JSONResponse:
        body = await _parse(request, HeartbeatRequest)
        return _json(await client.heartbeat(name, body.agent_epoch))

    return app


def _add_kind_routes[T: (Node, Pod, Deployment)](
    app: FastAPI, client: LocalStoreClient, kind: type[T]
) -> None:
    base = f"{PREFIX}/{PLURALS[kind]}"

    async def parse_named(request: Request, name: str) -> T:
        obj = await _parse(request, kind)
        if obj.metadata.name != name:
            raise Invalid(f"metadata.name {obj.metadata.name!r} doesn't match the URL ({name!r})")
        return obj

    async def list_objects(labels: str | None = None, node: str | None = None) -> dict[str, object]:
        items, revision = await client.list(kind, labels=_parse_labels(labels), node_name=node)
        return {"items": [o.model_dump(mode="json") for o in items], "revision": revision}

    async def create(request: Request) -> JSONResponse:
        return _json(await client.create(await _parse(request, kind)), 201)

    async def get(name: str) -> JSONResponse:
        return _json(await client.get(kind, name))

    async def update(name: str, request: Request) -> JSONResponse:
        return _json(await client.update(await parse_named(request, name)))

    async def update_status(name: str, request: Request) -> JSONResponse:
        return _json(await client.update_status(await parse_named(request, name)))

    async def delete(name: str, resource_version: int | None = None) -> JSONResponse:
        return _json(await client.delete(kind, name, resource_version))

    app.add_api_route(base, list_objects, methods=["GET"])
    app.add_api_route(base, create, methods=["POST"])
    app.add_api_route(base + "/{name}", get, methods=["GET"])
    app.add_api_route(base + "/{name}", update, methods=["PUT"])
    app.add_api_route(base + "/{name}/status", update_status, methods=["PUT"])
    app.add_api_route(base + "/{name}", delete, methods=["DELETE"])
