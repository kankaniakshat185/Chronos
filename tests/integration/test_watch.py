"""The agent is driven by its watch, not by polling (PM-3: P4.5, U3.1).

The agent's resync interval here is 30 s, so anything that happens within a few
seconds was triggered by a watch event (a pod bound or deleted) or by a local
process exiting, never by a timer.
"""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from chronos.api import ObjectMeta, Pod, PodPhase, PodSpec, RestartPolicy
from chronos.client import HttpStoreClient
from chronos.common.clock import RealClock
from chronos.store import NotFound
from chronos.testing.cluster import Cluster, Timings, alive, eventually
from tests.integration.test_agent import ready_nodes, workload

pytestmark = pytest.mark.integration
clock = RealClock()
NO_RESYNC = Timings(resync_interval=30.0)
FAST = 3.0  # well under the resync interval


@pytest.fixture
async def quiet(tmp_path: Path) -> AsyncIterator[Cluster]:
    c = Cluster(tmp_path, NO_RESYNC)
    await c.start()
    try:
        yield c
    finally:
        leaked = await c.close()
        print(c.logs())
        assert leaked == [], f"pod process groups leaked: {leaked}"


async def test_agent_reacts_to_watch_events_and_exits_without_polling(
    quiet: Cluster,
) -> None:
    quiet.start_agent("a")
    async with HttpStoreClient(quiet.url) as api:
        await ready_nodes(api, "a")
        await clock.sleep(0.5)  # the agent's first sync has run; the next is 30 s away

        spec = PodSpec(command=workload("--label", "w"), restart_policy=RestartPolicy.NEVER)
        await api.create(Pod(metadata=ObjectMeta(name="w"), spec=spec))

        async def running() -> Pod | None:
            p = await api.get(Pod, "w")
            return p if p.status.phase == PodPhase.RUNNING else None

        pod = await eventually(running, within=FAST)  # the bind arrived by watch
        assert pod.status.pid is not None
        await api.delete(Pod, "w")

        async def gone() -> bool:
            try:
                await api.get(Pod, "w")
            except NotFound:
                return True
            return False

        await eventually(gone, within=FAST)  # stopped and finalized, by watch
        assert pod.status.pid is not None and not alive(pod.status.pid)

        spec = PodSpec(
            command=workload("--crash-after", "0.2", "--exit-code", "3"),
            restart_policy=RestartPolicy.NEVER,
        )
        await api.create(Pod(metadata=ObjectMeta(name="c"), spec=spec))

        async def failed() -> bool:
            return (await api.get(Pod, "c")).status.phase == PodPhase.FAILED

        await eventually(failed, within=FAST)  # the exit woke the sync: no timer did
        await api.delete(Pod, "c")
