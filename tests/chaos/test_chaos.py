"""Chaos on real processes (P8.3, P8.4). Not in the default run: `uv run pytest -m chaos`.

The smoke is what CI runs on every PR (60 s, 3 nodes). The rogue test proves the live
checker can see a violation on real processes, not just on synthetic ones: a workload
started behind Chronos's back must be reported.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

from chronos.common.clock import RealClock
from chronos.testing.chaos import WORKLOAD, Chaos, write

pytestmark = pytest.mark.chaos
clock = RealClock()
SEED = int(os.environ.get("CHAOS_SEED", "1"))
DURATION = float(os.environ.get("CHAOS_DURATION", "60"))


@pytest.mark.timeout(0)
async def test_chaos_smoke(tmp_path: Path) -> None:
    report = await Chaos(tmp_path, SEED, nodes=3, duration=DURATION).run()
    where = write(report, Path(os.environ.get("CHAOS_OUT", tmp_path / "report")))
    assert report.ok, f"{report.violations[:5]} (report in {where})"
    assert report.samples > DURATION / 0.5  # the checker really ran


@pytest.mark.timeout(0)
async def test_the_checker_catches_a_rogue_workload(tmp_path: Path) -> None:
    chaos = Chaos(tmp_path, seed=2, nodes=2, duration=12)
    chaos.steps = []  # no faults: only the rogue process can be wrong
    chaos.resources = []  # every replica fits, so one extra process is one too many
    rogue: subprocess.Popen[bytes] | None = None

    async def start_rogue() -> None:
        nonlocal rogue
        await clock.sleep(4)
        env = {**os.environ, "CHRONOS_POD_UID": "rogue", "CHRONOS_NODE_NAME": chaos.names[0]}
        rogue = subprocess.Popen(  # noqa: ASYNC220 (a one-off, outside the system under test)
            [sys.executable, "-m", WORKLOAD, "--label", "chaos-d1"], env=env,
            stdout=subprocess.DEVNULL,
        )  # fmt: skip

    try:
        report, _ = await asyncio.gather(chaos.run(), start_rogue())
    finally:
        if rogue is not None:
            rogue.kill()
            rogue.wait()
    found = {v["invariant"] for v in report.violations}
    assert "S3" in found, report.violations  # one more d1 process than replicas, live
    assert "L5" in found  # and an orphan once everything has settled
    assert not report.ok
