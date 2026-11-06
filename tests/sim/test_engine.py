"""The simulator's own machinery: futures, tasks, actors, run_sync, the disk."""

import os
from pathlib import Path

import pytest

from chronos.sim.disk import SimDisk
from chronos.sim.engine import Actor, Sim, SimError, SimFuture, Task, run_sync
from chronos.store import wal as wal_module


def test_tasks_resume_in_virtual_time_order() -> None:
    sim = Sim(1)
    order: list[str] = []
    actor = Actor("a")

    async def sleeper(name: str, d: float) -> None:
        await sim.clock.sleep(d)
        order.append(f"{name}@{sim.now:g}")

    Task(sim, actor, sleeper("slow", 2.0), "slow")
    Task(sim, actor, sleeper("fast", 1.0), "fast")
    sim.run_until(10.0, lambda: None)
    assert order == ["fast@1", "slow@2"]


def test_a_paused_actor_runs_later_and_a_dead_one_never() -> None:
    sim = Sim(1)
    ran: list[str] = []
    paused, dead = Actor("p"), Actor("d")

    async def step(name: str) -> None:
        await sim.timer(1.0)
        ran.append(f"{name}@{sim.now:g}")

    Task(sim, paused, step("p"), "p")
    Task(sim, dead, step("d"), "d")
    sim.after(0.5, lambda: setattr(paused, "paused", True))  # mid-sleep
    sim.after(0.5, dead.kill)
    sim.after(3.0, lambda: [fn() for fn in paused.resume()])
    sim.run_until(10.0, lambda: None)
    assert ran == ["p@3"]


def test_future_results_and_errors_reach_the_awaiter() -> None:
    sim = Sim(1)
    got: list[object] = []
    ok, bad = SimFuture(), SimFuture()

    async def wait() -> None:
        got.append(await ok)
        try:
            await bad
        except ValueError as exc:
            got.append(str(exc))

    Task(sim, Actor("a"), wait(), "w")
    sim.after(1.0, lambda: ok.set_result(42))
    sim.after(2.0, lambda: bad.set_exception(ValueError("nope")))
    sim.run_until(5.0, lambda: None)
    assert got == [42, "nope"]


def test_run_sync_refuses_code_that_waits() -> None:
    async def fine() -> int:
        return 7

    async def waits() -> None:
        await SimFuture()

    assert run_sync(fine()) == 7
    with pytest.raises(SimError, match="tried to wait"):
        run_sync(waits())


def test_a_task_awaiting_something_foreign_is_a_sim_error() -> None:
    import asyncio  # noqa: PLC0415

    sim = Sim(1)

    async def foreign() -> None:
        await asyncio.sleep(0)  # noqa: TID251 (exactly what the sim must refuse)

    Task(sim, Actor("a"), foreign(), "f")
    with pytest.raises(SimError, match="awaited"):
        sim.run_until(1.0, lambda: None)


def test_the_disk_keeps_only_synced_bytes(tmp_path: Path) -> None:
    disk = SimDisk(tmp_path)
    path = tmp_path / "wal-1.log"
    with disk.installed():
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
        os.write(fd, b"durable")
        wal_module.durable_sync(fd)
        os.write(fd, b"+lost")
        os.close(fd)
    assert path.read_bytes() == b"durable+lost"
    disk.crash()
    assert path.read_bytes() == b"durable"


def test_a_dishonest_disk_keeps_nothing(tmp_path: Path) -> None:
    disk = SimDisk(tmp_path, honest=False)
    path = tmp_path / "wal-1.log"
    with disk.installed():
        fd = os.open(path, os.O_WRONLY | os.O_CREAT)
        os.write(fd, b"acked")
        wal_module.durable_sync(fd)
        os.close(fd)
    disk.crash()
    assert path.read_bytes() == b""
