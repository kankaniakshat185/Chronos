"""B4: scheduler throughput vs cluster size (TESTING.md section 9).

    uv run python bench/b4_scheduler.py [--nodes 10,100,1000] [--pods 5000]
    uv run python bench/b4_scheduler.py --smoke    # CI: 100 nodes vs the committed baseline

In-process, no real processes: a real store with N Ready nodes and P pending pods, and
the real `SchedulerController` placing them one at a time (filters, scores, assume
cache, the store's bind transaction). Its cache isn't refreshed during the run, so every
placement after the first relies on the assume cache, as it would during a burst.
Reports pods/s and the per-decision latency (one reconcile: decide + bind).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

from common import save, summary

from chronos.api import Node, NodeCondition, ObjectMeta, Pod, PodSpec
from chronos.api.resources import format_resources
from chronos.client import LocalStoreClient
from chronos.common.clock import RealClock
from chronos.controllers.scheduler import SchedulerController
from chronos.core.schedule import SchedulerConfig, resolve
from chronos.store import Store
from chronos.store.leases import Leases

BASELINE = Path(__file__).parent / "baselines" / "b4-smoke.json"


async def run(n_nodes: int, n_pods: int) -> dict[str, object]:
    clock = RealClock()
    store = Store(clock)
    for i in range(n_nodes):
        spec = {"capacity": format_resources({"cpu": 64_000, "memory": 256 * 2**30})}
        node = store.create(Node.model_validate({"metadata": {"name": f"n{i:04d}"}, "spec": spec}))
        node.status.condition = NodeCondition.READY
        store.update_status(node)
    requests = format_resources({"cpu": 100, "memory": 64 * 2**20})
    for i in range(n_pods):
        store.create(Pod(metadata=ObjectMeta(name=f"p{i:05d}"),
                         spec=PodSpec.model_validate({"command": ["x"], "resources": {"requests": requests}})))  # fmt: skip
    scheduler = SchedulerController(
        LocalStoreClient(store, Leases(clock), clock=clock), clock, resolve(SchedulerConfig())
    )
    latencies: list[float] = []
    real = scheduler.reconcile

    pods_informer = scheduler._pods

    async def timed(key: str) -> None:
        t = time.perf_counter()
        await real(key)
        latencies.append(time.perf_counter() - t)
        # The watch delivers each bind right away (in-process, no wait), as in chronosd:
        # the assume cache stays small, and the cost of catching up is in the total.
        await pods_informer.watch_once()

    scheduler.reconcile = timed  # type: ignore[method-assign]
    start = time.perf_counter()
    await scheduler.run_once()
    elapsed = time.perf_counter() - start
    bound = sum(bool(p.spec.node_name) for p in store.list(Pod)[0])
    assert bound == n_pods, f"only {bound}/{n_pods} bound"
    return {"pods_per_s": n_pods / elapsed, "seconds": elapsed,
            "decision": {"samples": latencies, **summary(latencies)}}  # fmt: skip


def smoke(tolerance: float) -> int:
    result = asyncio.run(run(100, 1000))
    rate = float(result["pods_per_s"])  # type: ignore[arg-type]
    base = json.loads(BASELINE.read_text())
    floor = base["pods_per_s"] * (1 - tolerance)
    print(f"B4 smoke: {rate:.0f} pods/s at 100 nodes (baseline {base['pods_per_s']:.0f} on "
          f"{base['machine']}, floor {floor:.0f})")  # fmt: skip
    return 0 if rate >= floor else 1


async def main(nodes: list[int], pods: int) -> None:
    results = {}
    for n in nodes:
        r = await run(n, pods)
        d = r["decision"]
        print(
            f"{n} nodes: {r['pods_per_s']:.0f} pods/s, decision p50 {d['p50'] * 1e3:.2f} ms p99 {d['p99'] * 1e3:.2f} ms",
            flush=True,
        )  # type: ignore[index]
        results[str(n)] = r
    save("b4", {"nodes": nodes, "pods": pods}, results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", default="10,100,1000")
    parser.add_argument("--pods", type=int, default=5000)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--tolerance", type=float, default=0.3)
    parser.add_argument("--write-baseline", action="store_true")
    a = parser.parse_args()
    if a.smoke:
        sys.exit(smoke(a.tolerance))
    if a.write_baseline:
        from common import machine

        r = asyncio.run(run(100, 1000))
        BASELINE.write_text(
            json.dumps({"pods_per_s": r["pods_per_s"], "machine": machine()["cpu"]}, indent=2)
            + "\n"
        )
        print(f"wrote {BASELINE}")
        sys.exit(0)
    asyncio.run(main([int(x) for x in a.nodes.split(",")], a.pods))
