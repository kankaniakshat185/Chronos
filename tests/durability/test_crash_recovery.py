"""S8 durability harness: crash at every crash point, for many seeds, recover, compare (ADR-0015).

Each case runs the seeded writer in a subprocess with `CHRONOS_CRASHPOINT=<point>:<n>`
(n drawn from the seed), so it dies inside one operation like a `kill -9`. The
recovered state must equal the reference state after the last finished operation
(the in-flight one was lost) or after the in-flight one (it reached the disk before
the crash): never anything else. So no acknowledged write is lost (S8), nothing
partially written is applied, and revisions continue without a gap (S5).

`DURABILITY_SEEDS` sets seeds per point: 3 by default, 50 for the exit run
(`make test-durability`).
"""

import os
import random
import subprocess
import sys
from pathlib import Path

import pytest

from chronos.common import crashpoint
from chronos.common.clock import FakeClock
from chronos.store import Store
from chronos.store.wal import SyncMode
from chronos.testing import durability

POINTS = [
    "wal.before_write",
    "wal.torn_write",
    "wal.before_fsync",
    "wal.after_fsync_before_apply",
    "segment.rotate",
    "snapshot.before_rename",
    "snapshot.after_rename_before_dirsync",
]
SEEDS = int(os.environ.get("DURABILITY_SEEDS", "3"))
CASES = [(point, seed) for point in POINTS for seed in range(SEEDS)]
MODES = ["full", "group"]


def calibrate(tmp: Path, seed: int, mode: SyncMode) -> dict[str, int]:
    """How often a crash-free run of this seed passes each crash point."""
    crashpoint.reset()
    durability.run(tmp, seed, mode=mode)
    return crashpoint.hits()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(("point", "seed"), CASES, ids=[f"{p}-{s}" for p, s in CASES])
def test_no_acknowledged_write_is_lost(
    tmp_path: Path, point: str, seed: int, mode: SyncMode
) -> None:
    total = calibrate(tmp_path / "dry", seed, mode).get(point, 0)
    assert total > 0, f"{point} is never reached with seed {seed}"
    nth = random.Random(f"{point}/{seed}").randint(1, total)
    run = subprocess.run(
        [
            sys.executable,
            "-m",
            "chronos.testing.durability",
            str(tmp_path / "wal"),
            str(seed),
            mode,
        ],
        env={**os.environ, "CHRONOS_CRASHPOINT": f"{point}:{nth}"},
        capture_output=True,
        text=True,
        timeout=60,
        check=False,  # the crash is the point
    )
    finished = len(run.stdout.splitlines())
    assert run.returncode == 137, f"expected a crash at {point}:{nth}: {run.stderr}"

    reference = durability.states(seed)
    recovered = Store.open(
        tmp_path / "wal", FakeClock(), snapshot_every=durability.SNAPSHOT_EVERY,
        segment_size=durability.SEGMENT_SIZE,
    )  # fmt: skip
    snap = recovered.snapshot()
    got = (snap.revision, snap.objects)
    # Full mode: the in-flight write is lost or made durable. Group mode: any prefix of
    # the not-yet-acknowledged batch may have reached the disk, but never less than
    # what was acknowledged, and never a gap (the log is in order).
    in_flight = 1 if mode == "full" else durability.GROUP_BATCH
    allowed = reference[finished : finished + in_flight + 1]
    assert got in allowed, (
        f"{point}:{nth} after {finished} ops: recovered revision {got[0]}, "
        f"expected one of {[r for r, _ in allowed]}"
    )
