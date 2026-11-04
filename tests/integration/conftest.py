"""A real cluster per test; teardown fails the test if any pod process leaked."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from chronos.client import HttpStoreClient
from chronos.testing.cluster import Cluster


@pytest.fixture
async def cluster(tmp_path: Path) -> AsyncIterator[Cluster]:
    c = Cluster(tmp_path)
    await c.start()
    try:
        yield c
    finally:
        leaked = await c.close()
        print(c.logs())  # shown by pytest only when the test fails
        assert leaked == [], f"pod process groups leaked: {leaked}"


@pytest.fixture
async def api(cluster: Cluster) -> AsyncIterator[HttpStoreClient]:
    async with HttpStoreClient(cluster.url) as client:
        yield client
