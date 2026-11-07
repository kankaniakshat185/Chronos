"""Proof of modularity (PX.9, X-9), scheduling and bind: a new resource and a new score
plugin, from tests only.

`example.com/widget` and the `test.FewestWidgetsLeft` plugin exist only in test code
(`tests/extensions.py`). Nothing in `src/` was changed for them: the scheduler and the
store's bind treat the widget like any extended resource, and the plugin plugs in
through the same registry the built-ins use.
"""

import asyncio

from hypothesis import given
from hypothesis import strategies as st

from chronos.api import Node, NodeCondition, Pod
from chronos.core.schedule import SchedulerConfig, resolve
from tests.extensions import FEWEST_WIDGETS_LEFT, WIDGET
from tests.property.test_schedule_props import build, within_capacity

PROFILE = resolve(SchedulerConfig(scores={"LeastAllocated": 1, FEWEST_WIDGETS_LEFT: 10}))


@st.composite
def widget_clusters(
    draw: st.DrawFn,
) -> tuple[list[tuple[dict[str, int], NodeCondition, bool]], list[dict[str, int]]]:
    nodes = [({"cpu": 4000, "memory": 2**32}, NodeCondition.READY, False)]  # no widgets
    for _ in range(draw(st.integers(1, 3))):
        cap = {"cpu": 4000, "memory": 2**32, WIDGET: draw(st.integers(0, 4))}
        nodes.append((cap, NodeCondition.READY, False))
    pods = draw(st.lists(st.dictionaries(st.just(WIDGET), st.integers(0, 3)), max_size=8))
    return nodes, pods


@given(widget_clusters())
def test_widgets_schedule_and_bind_like_any_extended_resource(
    cluster: tuple[list[tuple[dict[str, int], NodeCondition, bool]], list[dict[str, int]]],
) -> None:
    store, controller = build(cluster, PROFILE)
    asyncio.run(controller.run_once())
    nodes = {n.metadata.name: n for n in store.list(Node)[0]}
    for name, node in nodes.items():
        assert within_capacity(store.allocated(name), node.spec.capacity)
    for pod in store.list(Pod)[0]:
        if pod.requests.get(WIDGET, 0) > 0 and pod.spec.node_name:
            assert pod.spec.node_name != "n0", "the widget-less node can't host widget pods"


def test_the_test_plugin_changes_placement() -> None:
    cluster = (
        [
            ({"cpu": 4000, "memory": 2**32, WIDGET: 4}, NodeCondition.READY, False),
            ({"cpu": 4000, "memory": 2**32, WIDGET: 1}, NodeCondition.READY, False),
        ],
        [{WIDGET: 1}],
    )
    default, c1 = build(cluster)
    plugged, c2 = build(cluster, PROFILE)
    asyncio.run(c1.run_once())
    asyncio.run(c2.run_once())
    assert default.get(Pod, "p00").spec.node_name == "n0"  # name tie-break
    assert plugged.get(Pod, "p00").spec.node_name == "n1"  # fewest widgets left
