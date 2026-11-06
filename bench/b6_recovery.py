"""B6: recovery time vs WAL size, with and without snapshots (TESTING.md section 9).

    uv run python bench/b6_recovery.py [--sizes 10000,100000,1000000] [--repeats 3]

The log grows the way a real one does: 1 000 pods, then updates to them (label changes)
until the WAL holds N records, so the state stays small while the log gets long. Then
`Store.open` (the real recovery: newest snapshot, then every record after it) is timed.
"Without snapshots" disables them; "with" takes one every 5 000 records (the default).
Built with sync off for speed: recovery reads the same bytes either way.
"""

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

from common import save, summary

from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.common.clock import RealClock
from chronos.store import Store

PODS = 1000
NEVER = 10**12


def build(path: Path, records: int, snapshot_every: int) -> int:
    store = Store.open(path, RealClock(), sync="none", snapshot_every=snapshot_every)
    for i in range(min(PODS, records)):
        store.create(Pod(metadata=ObjectMeta(name=f"p{i:04d}"), spec=PodSpec(command=["x"])))
    i = 0
    while store.revision < records:
        pod = store.get(Pod, f"p{i % PODS:04d}")
        pod.metadata.labels = {"n": str(store.revision)}
        store.update(pod)
        i += 1
    size = sum(f.stat().st_size for f in path.iterdir())
    store.close()
    return size


def main(sizes: list[int], repeats: int) -> None:
    results: dict[str, object] = {}
    for n in sizes:
        for label, every in (("without", NEVER), ("with", 5000)):
            with tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp)
                disk = build(path, n, every)
                samples = []
                for _ in range(repeats):
                    t = time.perf_counter()
                    store = Store.open(path, RealClock(), sync="none")
                    samples.append(time.perf_counter() - t)
                    assert store.revision == n
                    store.close()
            results[f"{n}/{label}"] = {"records": n, "snapshots": label, "disk_bytes": disk,
                                        "seconds": {"samples": samples, **summary(samples)}}  # fmt: skip
            print(f"{n:>8} records {label:>7} snapshots: {summary(samples)['p50']:.3f} s "
                  f"({disk / 2**20:.1f} MiB on disk)", flush=True)  # fmt: skip
    save("b6", {"sizes": sizes, "repeats": repeats, "pods": PODS, "snapshot_every": 5000}, results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sizes", default="10000,100000,1000000")
    parser.add_argument("--repeats", type=int, default=3)
    a = parser.parse_args()
    main([int(s) for s in a.sizes.split(",")], a.repeats)
