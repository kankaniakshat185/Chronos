"""Metrics on real processes (ADR-0024).

`test_metrics_endpoints` needs nothing but the cluster. The `observability` tests also
need the Prometheus + Grafana stack (`make observability`) and run in the
`observability-smoke` CI job: `uv run pytest -m observability`.
"""

import json
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from chronos.api import Node, NodeCondition
from chronos.client import HttpStoreClient
from chronos.testing.cluster import Cluster, eventually
from tests.integration.helpers import deployment, wait_running

pytestmark = pytest.mark.integration
ROOT = Path(__file__).parents[2]
PROMETHEUS = "http://127.0.0.1:9090"
GRAFANA = "http://127.0.0.1:3000"
DASHBOARD = ROOT / "deploy" / "observability" / "grafana" / "dashboards" / "chronos.json"


async def ready(api: HttpStoreClient, n: int) -> None:
    async def all_ready() -> bool:
        nodes, _ = await api.list(Node)
        return len(nodes) == n and all(x.status.condition == NodeCondition.READY for x in nodes)

    await eventually(all_ready)


async def test_metrics_endpoints(cluster: Cluster, api: HttpStoreClient) -> None:
    cluster.start_agent("a")
    await ready(api, 1)
    await api.create(deployment("web", 1))
    await wait_running(api, "web", 1)
    async with httpx.AsyncClient() as http:
        control = (await http.get(f"{cluster.url}/metrics")).text
        assert 'chronos_nodes{condition="Ready"} 1.0' in control
        assert 'chronos_pods{phase="Running"} 1.0' in control
        assert 'chronos_resource_capacity{node="a",resource="cpu"} 4.0' in control

        port = cluster.metrics_ports["a"]

        async def agent_running() -> bool:
            text = (await http.get(f"http://127.0.0.1:{port}/metrics")).text
            return 'chronos_agent_processes{state="running"} 1.0' in text

        await eventually(agent_running)


# ---- with the Prometheus + Grafana stack ------------------------------------------------


@pytest.fixture
async def scraped(cluster: Cluster) -> AsyncIterator[Cluster]:
    targets = ROOT / "deploy" / "observability" / "targets" / "smoke.json"
    for name in ("a", "b", "c"):
        cluster.start_agent(name)
    cluster.write_targets(targets)
    try:
        yield cluster
    finally:
        targets.unlink(missing_ok=True)


async def promql(http: httpx.AsyncClient, query: str) -> float | None:
    data = (await http.get(f"{PROMETHEUS}/api/v1/query", params={"query": query})).json()
    result = data["data"]["result"]
    return float(result[0]["value"][1]) if result else None


@pytest.mark.observability
@pytest.mark.timeout(120)
async def test_prometheus_and_grafana_see_the_cluster(scraped: Cluster) -> None:
    async with httpx.AsyncClient(timeout=5) as http:

        async def all_up() -> bool:
            body = (await http.get(f"{PROMETHEUS}/api/v1/targets")).json()
            mine = [
                t for t in body["data"]["activeTargets"]
                if t["labels"].get("component") in ("chronosd", "agent")
            ]  # fmt: skip
            return len(mine) == 4 and all(t["health"] == "up" for t in mine)

        await eventually(all_up, within=40.0, interval=1.0)

        async def ready_nodes() -> bool:
            return await promql(http, 'chronos_nodes{condition="Ready"}') == 3

        await eventually(ready_nodes, within=20.0, interval=0.5)

        scraped.kill_node("b")

        async def not_ready_seen() -> bool:
            return await promql(http, 'chronos_nodes{condition="NotReady"}') == 1

        # Eviction after 2 s in the test timings; 1 s scrapes: well within 10 s.
        await eventually(not_ready_seen, within=10.0, interval=0.5)

        search = (await http.get(f"{GRAFANA}/api/search", params={"query": "Chronos"})).json()
        assert any(d.get("uid") == "chronos" for d in search), search

        # Every panel's query is valid PromQL, and most have data by now.
        dashboard = json.loads(DASHBOARD.read_text())
        exprs = [t["expr"] for p in dashboard["panels"] for t in p.get("targets", [])]
        empty = []
        for expr in exprs:
            body = (await http.get(f"{PROMETHEUS}/api/v1/query", params={"query": expr})).json()
            assert body["status"] == "success", (expr, body)
            if not body["data"]["result"]:
                empty.append(expr)
        assert len(empty) <= len(exprs) // 2, f"no data for: {empty}"
