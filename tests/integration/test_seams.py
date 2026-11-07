"""Proof of modularity (PX.9), agent side: the test-only workload type and provider
(`tests/extensions.py`) run a real process through the unchanged runtime. (The
simulator doesn't run commands, so this is where `prepare` and `env` are proven.)"""

from pathlib import Path

import pytest

from chronos.agent.providers import Providers
from chronos.agent.runtime import ProcessRuntime
from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.common.clock import RealClock
from chronos.common.config import FencingConfig
from chronos.testing.cluster import eventually
from tests.extensions import WIDGET, WIDGET_TASK

pytestmark = pytest.mark.integration
clock = RealClock()


async def test_fake_type_and_provider_through_the_unchanged_runtime(tmp_path: Path) -> None:
    providers = Providers({WIDGET: 3})
    runtime = ProcessRuntime(tmp_path, "n1", FencingConfig(), providers=providers)
    runtime.eviction_timeout = 60.0
    runtime.write_lease(clock.monotonic())
    spec = PodSpec.model_validate(
        {
            "command": ["ignored"],
            "workload_type": WIDGET_TASK,
            "resources": {"requests": {WIDGET: 2}},
        }
    )
    await runtime.start(Pod(metadata=ObjectMeta(name="p", uid="u1"), spec=spec))
    assert runtime.snapshot()["u1"].assigned == {WIDGET: ["w0", "w1"]}

    async def exited() -> bool:
        return runtime.snapshot()["u1"].exit_code == 0

    await eventually(exited)
    assert (tmp_path / "pods/u1/stdout.log").read_text().strip() == "w0,w1 yes"
    assert providers.allocate("u2", {WIDGET: 3})[0] == {WIDGET: ["w2", "w0", "w1"]}  # released
