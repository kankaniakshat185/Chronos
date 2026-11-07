"""The metric catalog (ADR-0024): names and label sets are an API, golden-tested.

Regenerate after a deliberate change: `UPDATE_GOLDEN=1 uv run pytest tests/unit/test_metrics.py`.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.common.clock import RealClock
from chronos.common.config import Timings
from chronos.common.metrics import AGENT_REGISTRY, AgentCollector, _reasons, catalog, render
from chronos.server.main import build
from chronos.store import Store

GOLDEN = Path(__file__).parents[1] / "golden" / "metrics.txt"


@pytest.fixture
async def chronosd(tmp_path: Path) -> AsyncIterator[httpx.AsyncClient]:
    store = Store.open(tmp_path, RealClock())
    app = build(Timings(), store)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://chronosd") as c:
            yield c
    store.close()


async def scrape(c: httpx.AsyncClient) -> str:
    """A cluster with a node and a pod that fits nowhere, so every family has a sample."""
    body = {
        "agent_epoch": "e1",
        "capacity": {"cpu": "2", "memory": "1Gi", "example.com/slot": 1},
        "fence_timeout": 6,
        "termination_grace": 2,
        "drift_margin": 1,
    }
    assert (await c.post("/api/v1/nodes/n1/register", json=body)).is_success
    huge = Pod(metadata=ObjectMeta(name="huge"), spec=PodSpec(command=["x"]))
    huge.spec.resources.requests = {"cpu": 64000}
    assert (await c.post("/api/v1/pods", content=huge.model_dump_json())).is_success
    for _ in range(200):  # up to 10 s: the controllers run on real time here
        text = (await c.get("/metrics")).text
        if "chronos_unschedulable_pods{" in text and "chronos_scheduling" in text:
            return text
        await asyncio.sleep(0.05)  # noqa: TID251 (the controllers run on real time here)
    return text


async def test_the_catalog_matches_the_golden_file(chronosd: httpx.AsyncClient) -> None:
    control = catalog(await scrape(chronosd))
    agent = catalog(render(AgentCollector(lambda: {"running": 1}), source=AGENT_REGISTRY).decode())
    got = "# chronosd\n" + "\n".join(control) + "\n# agent\n" + "\n".join(agent) + "\n"
    if os.environ.get("UPDATE_GOLDEN"):
        GOLDEN.write_text(got)
    assert got == GOLDEN.read_text()


async def test_cluster_state_is_exported_in_base_units(chronosd: httpx.AsyncClient) -> None:
    text = await scrape(chronosd)
    assert 'chronos_resource_capacity{node="n1",resource="cpu"} 2.0' in text  # cores
    assert 'chronos_resource_capacity{node="n1",resource="example.com/slot"} 1.0' in text
    assert 'chronos_pods{phase="Pending"} 1.0' in text
    assert 'chronos_unschedulable_pods{reason="Insufficient cpu"} 1.0' in text
    assert 'chronos_nodes{condition="Ready"}' in text


def test_unschedulable_reasons_are_bounded() -> None:
    message = "0/3 nodes available: Insufficient cpu (2/3 nodes), node NotReady (1/3 nodes)"
    assert _reasons(message) == ["Insufficient cpu", "node NotReady"]
    assert _reasons("0/0 nodes available: no nodes registered") == ["no nodes registered"]


def test_the_agents_metrics_server_starts_without_a_reverse_dns_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`HTTPServer.server_bind` calls `socket.getfqdn`, a reverse lookup that stalled for
    over 10 s on GitHub's macOS runners, before the agent had registered: every agent
    there looked dead. The agent's metrics server must not do that lookup."""
    import socket  # noqa: PLC0415
    import types  # noqa: PLC0415

    import httpx  # noqa: PLC0415

    from chronos.agent.main import serve_metrics  # noqa: PLC0415
    from chronos.testing.cluster import free_port  # noqa: PLC0415

    def no_lookups(name: str = "") -> str:
        raise AssertionError(f"reverse DNS lookup of {name!r}")

    monkeypatch.setattr(socket, "getfqdn", no_lookups)
    port = free_port()
    agent = types.SimpleNamespace(stats={"running": 2, "exited": 1})
    server = serve_metrics(agent, "127.0.0.1", port)  # type: ignore[arg-type]
    try:
        body = httpx.get(f"http://127.0.0.1:{port}/metrics").text
        assert 'chronos_agent_processes{state="running"} 2.0' in body
    finally:
        server.shutdown()
        server.server_close()
