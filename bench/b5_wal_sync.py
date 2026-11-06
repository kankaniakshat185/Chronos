"""B5: what the WAL costs (TESTING.md section 9).

    uv run python bench/b5_wal_sync.py [--writers 1,32] [--writes 50] [--repeats 3]

N concurrent writers each create M pods through `LocalStoreClient` over a store with a
WAL in a temp dir; writes/s and per-write ack latency (raw samples) for:

* `none`: no sync at all (the ceiling; never safe);
* `full`: one durable sync per write (the default);
* `full-fsync` (macOS only): the same with plain fsync(2) instead of F_FULLFSYNC, which on
  macOS only reaches the drive's cache: the price of real durability there;
* `group-1ms`, `group-2ms`, `group-5ms`: group commit with that window.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
import time
from pathlib import Path

from common import save, summary

from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.client import LocalStoreClient
from chronos.common.clock import RealClock
from chronos.store import Store
from chronos.store import wal as wal_module
from chronos.store.leases import Leases

MODES = {"none": ("none", 0.0), "full": ("full", 0.0), "group-1ms": ("group", 0.001),
         "group-2ms": ("group", 0.002), "group-5ms": ("group", 0.005)}  # fmt: skip
if sys.platform == "darwin":
    MODES["full-fsync"] = ("full", 0.0)


async def bench(mode: str, writers: int, writes: int) -> tuple[float, list[float]]:
    sync, window = MODES[mode]
    real = wal_module.durable_sync
    if mode == "full-fsync":
        wal_module.durable_sync = os.fsync
    try:
        with tempfile.TemporaryDirectory() as d:
            clock = RealClock()
            store = Store.open(Path(d), clock, sync=sync)  # type: ignore[arg-type]
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
    finally:
        wal_module.durable_sync = real
    return writers * writes / elapsed, latencies


async def main(writer_counts: list[int], writes: int, repeats: int) -> None:
    results: dict[str, dict[str, object]] = {}
    for writers in writer_counts:
        for mode in MODES:
            rates, lat = [], []
            for _ in range(repeats):
                rate, samples = await bench(mode, writers, writes)
                rates.append(rate)
                lat += samples
            s = summary(lat)
            results[f"{mode}/{writers}"] = {
                "writes_per_s": summary(rates),
                "latency": {"samples": lat, **s},
            }
            print(f"{writers:3} writers {mode:11}: {summary(rates)['p50']:8.0f} writes/s, "
                  f"p50 {s['p50'] * 1e3:6.2f} ms, p99 {s['p99'] * 1e3:6.2f} ms", flush=True)  # fmt: skip
    save("b5", {"writers": writer_counts, "writes": writes, "repeats": repeats}, results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--writers", default="1,32")
    parser.add_argument("--writes", type=int, default=50)
    parser.add_argument("--repeats", type=int, default=3)
    a = parser.parse_args()
    asyncio.run(main([int(w) for w in a.writers.split(",")], a.writes, a.repeats))
