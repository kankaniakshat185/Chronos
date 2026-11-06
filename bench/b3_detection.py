"""B3: detection speed vs false evictions (TESTING.md section 9): a real tradeoff curve.

    uv run python bench/b3_detection.py [--pauses 20] [--kills 3] [--evictions 1.5,3,6]

For each eviction timeout E (other timings scaled with it, keeping F + K + drift < E):

* detection latency: kill -9 the agent, time until the API shows the node NotReady;
* false-eviction rate: SIGSTOP the agent for a pause drawn from one fixed distribution
  (lognormal, median 1 s: GC pauses, a stalled VM), SIGCONT, and count the pauses during
  which the node was declared NotReady although it came back.

The same seeded pauses are used for every E, so the settings are compared on equal terms.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import signal
import tempfile
import time
from pathlib import Path

from common import save, summary

from chronos.api import Node, NodeCondition
from chronos.client import HttpStoreClient
from chronos.store import NotFound
from chronos.store.errors import Unavailable
from chronos.testing.cluster import Cluster, Timings


def timings(e: float) -> Timings:
    hb = round(e / 10, 3)
    return Timings(
        heartbeat_interval=hb, node_grace=0.4 * e, eviction_timeout=e, fence_timeout=0.5 * e,
        termination_grace=0.2 * e, drift_margin=0.1 * e, resync_interval=1.0,
    )  # fmt: skip


async def condition(api: HttpStoreClient) -> str:
    try:
        return (await api.get(Node, "n")).status.condition
    except (NotFound, Unavailable):
        return ""  # not registered yet, or chronosd busy: not Ready either way


async def until(api: HttpStoreClient, want: str) -> float:
    start = time.perf_counter()
    while await condition(api) != want:
        await asyncio.sleep(0.02)
    return time.perf_counter() - start


async def setting(e: float, pauses: list[float], kills: int) -> dict[str, object]:
    with tempfile.TemporaryDirectory() as tmp:
        cluster = Cluster(Path(tmp), timings(e))
        await cluster.start()
        cluster.start_agent("n")
        detect: list[float] = []
        false_evictions = 0
        try:
            async with HttpStoreClient(cluster.url, request_timeout=5.0) as api:
                await until(api, NodeCondition.READY)
                for d in pauses:
                    cluster.signal_agent("n", signal.SIGSTOP)
                    evicted = False
                    end = time.perf_counter() + d
                    while time.perf_counter() < end:
                        evicted |= await condition(api) == NodeCondition.NOT_READY
                        await asyncio.sleep(0.02)
                    cluster.signal_agent("n", signal.SIGCONT)
                    false_evictions += evicted
                    await until(api, NodeCondition.READY)
                    print(
                        f"E={e}: pause {d:.2f}s -> {'EVICTED' if evicted else 'kept'}", flush=True
                    )
                for _ in range(kills):
                    t0 = time.perf_counter()
                    cluster.kill_node("n")
                    await until(api, NodeCondition.NOT_READY)
                    detect.append(time.perf_counter() - t0)
                    print(f"E={e}: kill detected in {detect[-1]:.2f}s", flush=True)
                    cluster.start_agent("n")
                    await until(api, NodeCondition.READY)
        finally:
            await cluster.close()
    return {
        "detection": {"samples": detect, **summary(detect)},
        "false_evictions": false_evictions,
        "pauses": len(pauses),
        "false_eviction_rate": false_evictions / len(pauses),
    }


async def main(n_pauses: int, kills: int, evictions: list[float]) -> None:
    rng = random.Random(3)
    pauses = [round(min(rng.lognormvariate(0.0, 0.8), 10.0), 3) for _ in range(n_pauses)]
    results = {str(e): await setting(e, pauses, kills) for e in evictions}
    save("b3", {"pauses": pauses, "kills": kills, "evictions": evictions,
                "timings": {str(e): timings(e).__dict__ for e in evictions}}, results)  # fmt: skip


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pauses", type=int, default=20)
    parser.add_argument("--kills", type=int, default=3)
    parser.add_argument("--evictions", default="1.5,3,6")
    a = parser.parse_args()
    asyncio.run(main(a.pauses, a.kills, [float(x) for x in a.evictions.split(",")]))
