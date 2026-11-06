"""B5 (early): write throughput and latency per WAL sync mode, through the real client.

    uv run python bench/b5_wal_sync.py [--writers 32] [--writes 50]

N concurrent writers each create M pods through LocalStoreClient over a store with a
WAL in a temp dir. Reports writes/s and p50/p99 latency per mode. One machine, one
run: an indication, not a distribution (the full B5 is PM-6).
"""

import argparse
import asyncio
import statistics
import tempfile
import time
from pathlib import Path

from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.client import LocalStoreClient
from chronos.common.clock import RealClock
from chronos.store import Store
from chronos.store.leases import Leases


async def bench(mode: str, writers: int, writes: int, window: float) -> tuple[float, float, float]:
    with tempfile.TemporaryDirectory() as d:
        clock = RealClock()
        store = Store.open(Path(d), clock, sync=mode)  # type: ignore[arg-type]
        client = LocalStoreClient(store, Leases(clock), clock=clock, group_window=window)
        latencies: list[float] = []

        async def writer(w: int) -> None:
            for i in range(writes):
                t = time.perf_counter()
                await client.create(
                    Pod(metadata=ObjectMeta(name=f"w{w}-{i}"), spec=PodSpec(command=["x"]))
                )
                latencies.append(time.perf_counter() - t)

        start = time.perf_counter()
        await asyncio.gather(*(writer(w) for w in range(writers)))
        elapsed = time.perf_counter() - start
        store.close()
    latencies.sort()
    p99 = latencies[int(len(latencies) * 0.99) - 1]
    return writers * writes / elapsed, statistics.median(latencies) * 1000, p99 * 1000


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--writers", type=int, default=32)
    parser.add_argument("--writes", type=int, default=50)
    parser.add_argument("--window-ms", type=float, default=2.0)
    args = parser.parse_args()
    print(f"{args.writers} concurrent writers x {args.writes} creates each")
    print(f"{'mode':6} {'writes/s':>9} {'p50 ms':>8} {'p99 ms':>8}")
    for mode in ("full", "group", "none"):
        rate, p50, p99 = asyncio.run(bench(mode, args.writers, args.writes, args.window_ms / 1000))
        print(f"{mode:6} {rate:9.0f} {p50:8.2f} {p99:8.2f}")


if __name__ == "__main__":
    main()
