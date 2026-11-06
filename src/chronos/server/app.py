"""The `chronosd` HTTP API (PLAN.md section 4.4, ADR-0004).

Every registered kind (X-6) gets the same routes, under its plural name: CRUD,
`/status`, and a long-poll watch at `/watch/<plural>?since=` (ADR-0008).

Handlers are `async def` on purpose: they run on the event loop, one at a time
between awaits, so the synchronous store keeps a single writer. A plain `def`
handler would run in a thread pool and break that.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Callable, Coroutine

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST
from prometheus_client.registry import Collector
from pydantic import BaseModel, ValidationError

from chronos.api import ApiObject
from chronos.api.wire import BindingRequest, ErrorBody, HeartbeatRequest, RegisterRequest
from chronos.client.local import LocalStoreClient
from chronos.client.protocol import WATCH_TIMEOUT
from chronos.common.config import FencingConfig
from chronos.common.metrics import render
from chronos.store import Invalid, StoreError
from chronos.store.kinds import KINDS

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


def create_app(
    client: LocalStoreClient, loops: list[Loop] | None = None, metrics: Collector | None = None
) -> FastAPI:
    """`loops` are the controllers' run() coroutines, started with the server; `metrics`
    adds cluster-state gauges to `/metrics` (ADR-0024)."""

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.closing = asyncio.Event()  # set on shutdown: open watches answer at once
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

    @app.get("/metrics")
    async def metrics_endpoint() -> Response:
        body = render(*([metrics] if metrics is not None else []))
        return Response(body, media_type=CONTENT_TYPE_LATEST)

    for kind in KINDS.values():
        _add_kind_routes(app, client, kind.model, kind.plural)

    @app.post(PREFIX + "/pods/{name}/binding")
    async def bind(name: str, request: Request) -> JSONResponse:
        body = await _parse(request, BindingRequest)
        return _json(await client.bind(name, body.node_name, body.resource_version), 201)

    @app.post(PREFIX + "/nodes/{name}/register")
    async def register(name: str, request: Request) -> JSONResponse:
        body = await _parse(request, RegisterRequest)
        fencing = FencingConfig.model_construct(  # timings checked against E in register()
            fence_timeout=body.fence_timeout,
            termination_grace=body.termination_grace,
            drift_margin=body.drift_margin,
        )
        return _json(await client.register(name, body.agent_epoch, body.capacity, fencing))

    @app.post(PREFIX + "/nodes/{name}/heartbeat")
    async def heartbeat(name: str, request: Request) -> JSONResponse:
        body = await _parse(request, HeartbeatRequest)
        return _json(await client.heartbeat(name, body.agent_epoch))

    return app


def _add_kind_routes[T: ApiObject](
    app: FastAPI, client: LocalStoreClient, kind: type[T], plural: str
) -> None:
    base = f"{PREFIX}/{plural}"

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

    async def delete(
        name: str, resource_version: int | None = None, force: bool = False
    ) -> JSONResponse:
        return _json(await client.delete(kind, name, resource_version, force=force))

    async def watch(
        request: Request,
        since: int,
        timeout: float = WATCH_TIMEOUT,  # noqa: ASYNC109 (how long to hold the poll)
        node: str | None = None,
    ) -> dict[str, object]:
        watching = asyncio.ensure_future(
            client.watch(kind, since, wait=max(timeout, 0.0), node_name=node)
        )
        closing: asyncio.Event | None = getattr(request.app.state, "closing", None)
        if closing is not None:
            stopping = asyncio.ensure_future(closing.wait())
            try:
                await asyncio.wait({watching, stopping}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                stopping.cancel()
            if not watching.done():  # shutting down: an empty batch; the client re-watches
                watching.cancel()
                return {"events": [], "revision": since}
        batch = await watching
        events = [
            {"type": e.type, "object": e.object.model_dump(mode="json")} for e in batch.events
        ]
        return {"events": events, "revision": batch.revision}

    app.add_api_route(f"{PREFIX}/watch/{plural}", watch, methods=["GET"])
    app.add_api_route(base, list_objects, methods=["GET"])
    app.add_api_route(base, create, methods=["POST"])
    app.add_api_route(base + "/{name}", get, methods=["GET"])
    app.add_api_route(base + "/{name}", update, methods=["PUT"])
    app.add_api_route(base + "/{name}/status", update_status, methods=["PUT"])
    app.add_api_route(base + "/{name}", delete, methods=["DELETE"])
