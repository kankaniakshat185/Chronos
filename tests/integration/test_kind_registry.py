"""X-6 (PX.6): a kind and a controller defined only in this file.

`Widget` and `WidgetController` are not in `src/`. Registering them is all it takes
for the widget to get the API routes (CRUD, status, watch), the store's write rules,
durability through the WAL, manifest parsing, and a reconcile loop on the shared
controller runtime, exactly as Node, Pod and Deployment do.
"""

from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Literal

import httpx
import pytest
from pydantic import BaseModel, ConfigDict, Field

from chronos.api import ApiObject, ObjectMeta
from chronos.cli.manifest import load
from chronos.client import HttpStoreClient
from chronos.common.clock import FakeClock, RealClock
from chronos.common.config import Timings
from chronos.controllers.runtime import (
    CONTROLLERS,
    Controller,
    ControllerContext,
    register_controller,
)
from chronos.server.main import build
from chronos.store import Invalid, Store
from chronos.store.kinds import KINDS, KindInfo, register_kind
from chronos.testing.cluster import eventually


class WidgetSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    size: int = Field(ge=0)


class WidgetStatus(BaseModel):
    model_config = ConfigDict(extra="forbid")
    observed_size: int = 0


class Widget(ApiObject):
    kind: Literal["Widget"] = "Widget"
    metadata: ObjectMeta
    spec: WidgetSpec
    status: WidgetStatus = WidgetStatus()


def no_huge_widgets(obj: ApiObject) -> None:
    assert isinstance(obj, Widget)
    if obj.spec.size > 100:
        raise Invalid(f"Widget {obj.metadata.name!r}: size {obj.spec.size} is over 100")


class WidgetController(Controller):
    """Copies spec.size into status.observed_size: the smallest real reconcile loop."""

    name = "test.widget"

    def __init__(self, ctx: ControllerContext) -> None:
        self._client = ctx.client
        self._widgets = ctx.informers.get(Widget)
        super().__init__(ctx.clock, [self._widgets], resync_period=30.0)
        self._widgets.add_handler(lambda old, new: self.queue.add(_name(new or old)))

    def keys(self) -> list[str]:
        return self._widgets.keys()

    async def reconcile(self, key: str) -> None:
        widget = self._widgets.get(key)
        if widget is not None and widget.status.observed_size != widget.spec.size:
            widget.status.observed_size = widget.spec.size
            await self._client.update_status(widget)


def _name(obj: Widget | None) -> str:
    assert obj is not None
    return obj.metadata.name


def widget(name: str, size: int) -> Widget:
    return Widget(metadata=ObjectMeta(name=name), spec=WidgetSpec(size=size))


@pytest.fixture
def registered() -> Iterator[None]:
    register_kind(KindInfo(Widget, "widgets", ("widget",), validate_create=no_huge_widgets))
    register_controller(WidgetController.name, WidgetController)
    try:
        yield
    finally:
        del KINDS["Widget"], CONTROLLERS[WidgetController.name]


@pytest.fixture
async def api(registered: None, tmp_path: Path) -> AsyncIterator[HttpStoreClient]:
    """The real chronosd app, with every registered controller running, in-process."""
    store = Store.open(tmp_path, RealClock())
    app = build(Timings(), store)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with HttpStoreClient("http://chronosd", transport=transport) as client:
            yield client
    store.close()


async def test_a_registered_kind_gets_the_api_and_a_reconcile_loop(
    api: HttpStoreClient,
) -> None:
    created = await api.create(widget("w1", 3))
    assert created.metadata.uid and created.metadata.resource_version > 0
    with pytest.raises(Invalid, match="over 100"):
        await api.create(widget("huge", 101))  # its store validator ran

    async def observed(size: int) -> bool:
        return (await api.get(Widget, "w1")).status.observed_size == size

    await eventually(lambda: observed(3), within=5.0)  # the controller reconciled it
    current = await api.get(Widget, "w1")
    current.spec.size = 5
    await api.update(current)
    await eventually(lambda: observed(5), within=5.0)

    items, revision = await api.list(Widget)
    assert [w.metadata.name for w in items] == ["w1"]
    batch = await api.watch(Widget, 0, wait=0)
    assert [e.type for e in batch.events][:2] == ["ADDED", "MODIFIED"]
    assert batch.revision >= revision
    await api.delete(Widget, "w1")
    assert (await api.list(Widget))[0] == []


def test_a_registered_kind_is_durable_and_parses_from_manifests(
    registered: None, tmp_path: Path
) -> None:
    store = Store.open(tmp_path / "data", FakeClock())
    store.create(widget("w1", 7))
    store.close()
    again = Store.open(tmp_path / "data", FakeClock())
    assert again.get(Widget, "w1").spec.size == 7  # through the WAL and recovery
    again.close()

    manifest = tmp_path / "w.yaml"
    manifest.write_text("kind: Widget\nmetadata: {name: w2}\nspec: {size: 2}\n")
    [parsed] = load(manifest)
    assert isinstance(parsed, Widget) and parsed.spec.size == 2
