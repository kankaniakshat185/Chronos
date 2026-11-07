"""Test-only extensions for the proof of modularity (PX.9, X-9).

A new extended resource (`example.com/widget`), a score plugin (X-3), a workload type
(X-4) and a resource provider (X-5), all defined and registered here, in test code.
Nothing in `src/` knows about any of them. Importing this module registers them once
(the registries reject a second registration of a name), so every PX.9 test shares it:

* `tests/property/test_modularity_widget.py`: scheduling and bind properties;
* `tests/integration/test_seams.py`: a real process through the unchanged runtime;
* `tests/sim/test_seam_proof.py`: simulator runs, with every invariant checked.
"""

from __future__ import annotations

import sys

from chronos.agent.providers import ProviderError, register_provider
from chronos.api import Pod
from chronos.api.pod import PodTemplate
from chronos.api.workload_types import register_workload_type
from chronos.core.schedule import NodeInfo, register_score

WIDGET = "example.com/widget"
WIDGET_TASK = "test.widget-task"
FEWEST_WIDGETS_LEFT = "test.FewestWidgetsLeft"


def fewest_widgets_left(_: Pod, node: NodeInfo) -> int:
    """Bin-pack widgets: prefer the node with the fewest free widgets (a test-only policy)."""
    return 100 - min(node.free.get(WIDGET, 0), 100)


class WidgetTask:
    """A test-only workload type: needs at least one widget, ignores `command`, and
    reports which widgets it got and that the type prepared it."""

    def validate(self, template: PodTemplate) -> None:
        if template.resources.requests.get(WIDGET, 0) < 1:
            raise ValueError(f"{WIDGET_TASK} needs at least one {WIDGET}")

    def prepare(self, pod: Pod) -> tuple[list[str], dict[str, str]]:
        script = "import os; print(os.environ['WIDGETS'], os.environ['FROM_TYPE'])"
        return [sys.executable, "-c", script], {"FROM_TYPE": "yes"}


class WidgetProvider:
    """A test-only provider: widgets are named "w0".."wN-1", handed out in order of
    release, and the workload learns which ones it got."""

    def __init__(self, resource: str, capacity: int) -> None:
        self.resource = resource
        self.free = [f"w{i}" for i in range(capacity)]
        self.held: dict[str, list[str]] = {}

    def allocate(self, uid: str, n: int) -> list[str]:
        if uid in self.held:
            raise ProviderError(f"{self.resource}: pod {uid} already holds widgets")
        if n > len(self.free):
            raise ProviderError(f"{self.resource}: {n} requested, {len(self.free)} free")
        self.held[uid], self.free = self.free[:n], self.free[n:]
        return self.held[uid]

    def release(self, uid: str) -> None:
        self.free += self.held.pop(uid, [])

    def env(self, units: list[str]) -> dict[str, str]:
        return {"WIDGETS": ",".join(units)}


register_score(FEWEST_WIDGETS_LEFT, fewest_widgets_left)
register_workload_type(WIDGET_TASK, WidgetTask())
register_provider(WIDGET, WidgetProvider)
