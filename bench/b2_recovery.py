"""B2: from node death to replacements Running, and where the time goes (TESTING.md 9).

    uv run python bench/b2_recovery.py [--runs 20]

A real chronosd and 3 agents with the demo's timings (heartbeat 0.5 s, NotReady and
eviction after 5 s, fencing after 2 s). A deployment of 6 `sleep` pods; each run kills
-9 the node with the most pods and polls the API every 20 ms, recording when the node
goes NotReady, when its pods are Failed, when replacements exist, are bound, and are
Running. Then the node comes back for the next run.

Phases: detection (kill -> NotReady), eviction (-> pods Failed), reconcile (->
replacements created), schedule (-> bound), start (-> Running).
"""

from __future__ import annotations

import argparse
import asyncio
import tempfile
import time
from collections import Counter
from pathlib import Path

from common import save, summary

from chronos.api import Deployment, Node, NodeCondition, Pod, PodPhase
from chronos.api.pod import TERMINAL_PHASES
from chronos.client import HttpStoreClient
from chronos.testing.cluster import Cluster, Timings

DEMO = Timings(
    heartbeat_interval=0.5, node_grace=2.0, eviction_timeout=5.0, fence_timeout=2.0,
    termination_grace=1.0, drift_margin=1.0, resync_interval=1.0,
)  # fmt: skip
PHASES = ("detection", "eviction", "reconcile", "schedule", "start")
REPLICAS = 6


async def wait_ready(api: HttpStoreClient, n: int) -> None:
    while True:
        nodes, _ = await api.list(Node)
        if len(nodes) == n and all(x.status.condition == NodeCondition.READY for x in nodes):
            return
        await asyncio.sleep(0.05)


async def settled(api: HttpStoreClient) -> list[Pod]:
    while True:
        pods, _ = await api.list(Pod)
        active = [p for p in pods if p.status.phase not in TERMINAL_PHASES]
        if len(active) == REPLICAS and all(p.status.phase == PodPhase.RUNNING for p in active):
            return active
        await asyncio.sleep(0.05)


async def one_run(cluster: Cluster, api: HttpStoreClient) -> dict[str, float]:
    active = await settled(api)
    victim, _ = Counter(p.spec.node_name for p in active).most_common(1)[0]
    lost = {p.metadata.uid for p in active if p.spec.node_name == victim}
    t0 = time.perf_counter()
    cluster.kill_node(victim)
    marks: dict[str, float] = {}
    while "running" not in marks:
        await asyncio.sleep(0.02)
        now = time.perf_counter() - t0
        node = await api.get(Node, victim)
        pods, _ = await api.list(Pod)
        by_uid = {p.metadata.uid: p for p in pods}
        fresh = [p for p in pods if p.metadata.uid not in lost and p.spec.node_name != victim
                 and p.status.phase not in TERMINAL_PHASES]  # fmt: skip
        if node.status.condition == NodeCondition.NOT_READY:
            marks.setdefault("notready", now)
        if all(by_uid.get(u) is None or by_uid[u].status.phase == PodPhase.FAILED for u in lost):
            marks.setdefault("evicted", now)
        if len(fresh) >= REPLICAS:
            marks.setdefault("created", now)
            if all(p.spec.node_name for p in fresh):
                marks.setdefault("bound", now)
            if all(p.status.phase == PodPhase.RUNNING for p in fresh):
                marks.setdefault("running", now)
    cluster.start_agent(victim)
    await wait_ready(api, 3)
    order = ["notready", "evicted", "created", "bound", "running"]
    times = [0.0] + [marks.get(k, marks["running"]) for k in order]
    return {phase: max(times[i + 1] - times[i], 0.0) for i, phase in enumerate(PHASES)} | {
        "total": marks["running"]
    }


async def main(runs: int) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        cluster = Cluster(Path(tmp), DEMO)
        await cluster.start()
        for name in ("a", "b", "c"):
            cluster.start_agent(name)
        samples: dict[str, list[float]] = {k: [] for k in (*PHASES, "total")}
        try:
            async with HttpStoreClient(cluster.url) as api:
                await wait_ready(api, 3)
                await api.create(Deployment.model_validate({
                    "metadata": {"name": "b2"},
                    "spec": {"replicas": REPLICAS, "template": {
                        "command": ["sleep", "3600"],
                        "resources": {"requests": {"cpu": "100m", "memory": "16Mi"}},
                    }},
                }))  # fmt: skip
                for i in range(runs):
                    run = await one_run(cluster, api)
                    for k, v in run.items():
                        samples[k].append(v)
                    print(
                        f"run {i + 1}/{runs}: " + ", ".join(f"{k} {v:.2f}" for k, v in run.items()),
                        flush=True,
                    )
        finally:
            await cluster.close()
    save("b2", {"runs": runs, "replicas": REPLICAS, "timings": DEMO.__dict__},
         {k: {"samples": v, **summary(v)} for k, v in samples.items()})  # fmt: skip


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=20)
    asyncio.run(main(parser.parse_args().runs))
