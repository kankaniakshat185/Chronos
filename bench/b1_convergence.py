"""B1: how long from `apply` to all R replicas Running? (TESTING.md section 9)

    uv run python bench/b1_convergence.py [--runs 30] [--replicas 1,10,50,100]

A real chronosd and 3 agents (the test-fixture timings). Each run creates a deployment
of R `sleep` pods, polls the API every 10 ms until all R are Running, then deletes it
and waits until the processes are gone. "Running" is the agent's report after the
process started, so this is apply -> controller -> scheduler -> agent -> process ->
status, end to end.
"""

from __future__ import annotations

import argparse
import asyncio
import tempfile
import time
from pathlib import Path

from common import save, summary

from chronos.api import Deployment, Pod, PodPhase
from chronos.client import HttpStoreClient
from chronos.store import NotFound
from chronos.store.errors import Unavailable
from chronos.testing.cluster import Cluster

LABEL = "chronos.dev/deployment"


def deployment(replicas: int) -> Deployment:
    return Deployment.model_validate({
        "metadata": {"name": "b1"},
        "spec": {"replicas": replicas, "template": {
            "command": ["sleep", "3600"], "resources": {"requests": {"cpu": "50m", "memory": "16Mi"}},
        }},
    })  # fmt: skip


async def pods_of(api: HttpStoreClient) -> list[Pod]:
    """Patient: at R=100 chronosd shares the CPU with 100 starting processes."""
    while True:
        try:
            return (await api.list(Pod, labels={LABEL: "b1"}))[0]
        except Unavailable:
            await asyncio.sleep(0.05)


async def one_run(api: HttpStoreClient, replicas: int) -> float:
    start = time.perf_counter()
    await api.create(deployment(replicas))
    while True:
        pods = await pods_of(api)
        if sum(p.status.phase == PodPhase.RUNNING for p in pods) >= replicas:
            elapsed = time.perf_counter() - start
            break
        await asyncio.sleep(0.01)
    await api.delete(Deployment, "b1")
    while await pods_of(api):
        await asyncio.sleep(0.05)  # garbage-collected, processes stopped, pods finalized
    return elapsed


async def main(runs: int, sizes: list[int]) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        cluster = Cluster(Path(tmp))
        await cluster.start()
        for name in ("a", "b", "c"):
            cluster.start_agent(name)
        try:
            async with HttpStoreClient(cluster.url, request_timeout=10.0) as api:
                await asyncio.sleep(2)  # all Ready
                results: dict[str, object] = {}
                for r in sizes:
                    samples = []
                    for i in range(runs):
                        with_retry = 0
                        while True:
                            try:
                                samples.append(await one_run(api, r))
                                break
                            except NotFound:  # a run racing the previous GC: once more
                                with_retry += 1
                                if with_retry > 3:
                                    raise
                        print(f"R={r} run {i + 1}/{runs}: {samples[-1]:.3f}s", flush=True)
                    results[str(r)] = {"samples": samples, **summary(samples)}
        finally:
            await cluster.close()
    save("b1", {"runs": runs, "replicas": sizes, "nodes": 3, "workload": "sleep"}, results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=30)
    parser.add_argument("--replicas", default="1,10,50,100")
    args = parser.parse_args()
    asyncio.run(main(args.runs, [int(r) for r in args.replicas.split(",")]))
