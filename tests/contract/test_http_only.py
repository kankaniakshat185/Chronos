"""HTTP-specific behaviour: status codes, body validation, transport failures."""

import httpx
import pytest

from chronos.client import HttpStoreClient, LocalStoreClient
from chronos.common.clock import FakeClock
from chronos.server.app import create_app
from chronos.store import Store
from chronos.store.errors import Unavailable
from chronos.store.leases import Leases


@pytest.fixture
def raw() -> httpx.AsyncClient:
    clock = FakeClock()
    app = create_app(LocalStoreClient(Store(clock), Leases(clock)))
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://chronosd")


POD = {"metadata": {"name": "p1"}, "spec": {"command": ["true"]}}


async def test_status_codes(raw: httpx.AsyncClient) -> None:
    async with raw:
        assert (await raw.get("/healthz")).status_code == 200
        assert (await raw.post("/api/v1/pods", json=POD)).status_code == 201
        assert (await raw.post("/api/v1/pods", json=POD)).status_code == 409
        assert (await raw.get("/api/v1/pods/nope")).status_code == 404
        stale = {**POD, "metadata": {"name": "p1", "resource_version": 9}}
        r = await raw.put("/api/v1/pods/p1", json=stale)
        assert r.status_code == 409
        assert r.json()["reason"] == "Conflict"
        assert r.json()["details"]["current"] == 1


async def test_typo_field_is_422_with_the_field_name(raw: httpx.AsyncClient) -> None:
    async with raw:
        bad = {"metadata": {"name": "p1"}, "spec": {"command": ["true"], "replicaz": 3}}
        r = await raw.post("/api/v1/pods", json=bad)
        assert r.status_code == 422
        assert r.json()["reason"] == "Invalid"
        assert "spec.replicaz" in r.json()["message"]


async def test_url_and_body_names_must_match(raw: httpx.AsyncClient) -> None:
    async with raw:
        await raw.post("/api/v1/pods", json=POD)
        r = await raw.put("/api/v1/pods/other", json={**POD, "metadata": {"name": "p1"}})
        assert r.status_code == 422


async def test_malformed_label_selector_is_422(raw: httpx.AsyncClient) -> None:
    async with raw:
        assert (await raw.get("/api/v1/pods", params={"labels": "novalue"})).status_code == 422


async def test_transport_failure_raises_unavailable() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async with HttpStoreClient("http://chronosd", transport=httpx.MockTransport(refuse)) as c:
        with pytest.raises(Unavailable):
            await c.heartbeat("n1", "e1")


async def test_unknown_workload_type_is_422(raw: httpx.AsyncClient) -> None:
    async with raw:
        pod = {"metadata": {"name": "p"}, "spec": {"command": ["x"], "workload_type": "nope"}}
        r = await raw.post("/api/v1/pods", json=pod)
        assert r.status_code == 422
        assert "unknown workload_type 'nope'" in r.json()["message"]
