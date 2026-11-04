"""X-4/X-5 from test code only: a fake workload type and a fake provider run a real
process through the unchanged runtime (the agent-side half of PX.9)."""

import sys
from pathlib import Path

import pytest

from chronos.agent.providers import Providers, register_provider
from chronos.agent.runtime import ProcessRuntime
from chronos.api import ObjectMeta, Pod, PodSpec
from chronos.api.workload_types import Generic, register_workload_type
from chronos.common.clock import RealClock
from chronos.common.config import FencingConfig
from chronos.testing.cluster import eventually

pytestmark = pytest.mark.integration
WIDGET = "example.com/widget"
clock = RealClock()


class PrintsItsEnvironment(Generic):
    """A test-only workload type: ignores `command` and reports what it was given."""

    def prepare(self, pod: Pod) -> tuple[list[str], dict[str, str]]:
        script = "import os; print(os.environ['WIDGETS'], os.environ['FROM_TYPE'])"
        return [sys.executable, "-c", script], {"FROM_TYPE": "yes"}


class WidgetProvider:
    """A test-only provider: widgets are named, and the workload learns which it got."""

    def __init__(self, resource: str, capacity: int) -> None:
        self.free = [f"w{i}" for i in range(capacity)]
        self.held: dict[str, list[str]] = {}

    def allocate(self, uid: str, n: int) -> list[str]:
        self.held[uid], self.free = self.free[:n], self.free[n:]
        return self.held[uid]

    def release(self, uid: str) -> None:
        self.free += self.held.pop(uid, [])

    def env(self, units: list[str]) -> dict[str, str]:
        return {"WIDGETS": ",".join(units)}


register_workload_type("test.prints-env", PrintsItsEnvironment())
register_provider(WIDGET, WidgetProvider)


async def test_fake_type_and_provider_through_the_unchanged_runtime(tmp_path: Path) -> None:
    providers = Providers({WIDGET: 3})
    runtime = ProcessRuntime(tmp_path, "n1", FencingConfig(), providers=providers)
    runtime.eviction_timeout = 60.0
    runtime.write_lease(clock.monotonic())
    spec = PodSpec.model_validate(
        {
            "command": ["ignored"],
            "workload_type": "test.prints-env",
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
