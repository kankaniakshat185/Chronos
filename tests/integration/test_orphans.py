"""Orphan cleanup (P2.7): an agent restart kills its predecessor's pods, and nothing else."""

import asyncio
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from chronos.agent.runtime import ProcessRuntime
from chronos.client import HttpStoreClient
from chronos.common.clock import RealClock
from chronos.common.config import FencingConfig
from chronos.testing.cluster import Cluster, alive, eventually
from tests.integration.helpers import deployment, live_processes, wait_nodes_ready, wait_running

pytestmark = pytest.mark.integration
clock = RealClock()


def spawn(*argv: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, *argv], start_new_session=True, stdout=subprocess.DEVNULL
    )


async def test_cleanup_kills_only_verified_shims(tmp_path: Path) -> None:
    runtime = ProcessRuntime(tmp_path, "n1", FencingConfig())
    pids = tmp_path / "pids"
    lease = tmp_path / "lease"
    lease.write_text(repr(clock.monotonic()))
    orphan = spawn(
        "-m", "chronos.shim", "--lease", str(lease), "--pod-uid", "uid-1",
        "--fence-timeout", "60", "--eviction-timeout", "120", "--grace", "1",
        "--", sys.executable, "-m", "chronos.testing.workload",
    )  # fmt: skip
    stranger = spawn("-c", "import time; time.sleep(30)")  # a reused PID: not ours
    try:
        await clock.sleep(0.5)
        (pids / "uid-1").write_text(str(orphan.pid))
        (pids / "uid-2").write_text(str(stranger.pid))
        (pids / "uid-3").write_text("999999")  # gone
        (pids / "uid-4").write_text("not a number")

        assert runtime.cleanup_orphans() == [orphan.pid]
        assert orphan.wait(timeout=5) == -signal.SIGKILL
        assert alive(stranger.pid), "an unrelated process must survive"
        assert list(pids.iterdir()) == []
    finally:
        for p in (orphan, stranger):
            p.kill()
            p.wait()


async def test_quick_agent_restart_kills_the_old_pods_first(
    cluster: Cluster, api: HttpStoreClient
) -> None:
    """kill -9 the agent and restart it before its shims would fence: the new agent must
    kill the old workloads before starting new ones, so R is never exceeded."""
    cluster.start_agent("a")
    await wait_nodes_ready(api, "a")
    await api.create(deployment("web", 2))
    old = {p.status.pid for p in await wait_running(api, "web", 2)}

    peak = 0
    stop = asyncio.Event()

    async def sample() -> None:
        nonlocal peak
        while not stop.is_set():
            peak = max(peak, len(live_processes("web")))
            await clock.sleep(0.02)

    sampler = asyncio.create_task(sample())
    cluster.signal_agent("a", signal.SIGKILL)
    cluster.agents.pop("a").wait(timeout=5)
    cluster.start_agent("a")  # well within the fence timeout

    async def restarted() -> set[int | None] | None:
        pids = {p.status.pid for p in await wait_running(api, "web", 2)}
        return pids if not pids & old else None

    new = await eventually(restarted)
    stop.set()
    await sampler

    assert not any(alive(g) for g in old if g is not None)
    assert peak <= 2, f"saw {peak} live replicas of a 2-replica deployment"
    assert len(new) == 2
