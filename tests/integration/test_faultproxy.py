"""The fault proxy's modes, against a tiny echo server (P8.1)."""

import asyncio
import contextlib
import random
from collections.abc import AsyncIterator

import pytest

from chronos.common.clock import RealClock
from chronos.testing.cluster import free_port
from chronos.testing.faultproxy import FaultProxy, Mode, parse_mode

clock = RealClock()


@pytest.fixture
async def proxy() -> AsyncIterator[FaultProxy]:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(ConnectionError):
            while data := await reader.read(1024):
                writer.write(data)
                await writer.drain()
        writer.close()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    target = server.sockets[0].getsockname()[1]
    p = FaultProxy(target, free_port(), rng=random.Random(1))
    await p.start()
    yield p
    await p.stop()
    server.close()


async def roundtrip(p: FaultProxy, payload: bytes = b"ping", within: float = 1.0) -> bytes:
    """What came back; b"" if the connection was closed or reset (the proxy sends RST)."""
    reader, writer = await asyncio.open_connection("127.0.0.1", p.port)
    try:
        writer.write(payload)
        await writer.drain()
        return await asyncio.wait_for(reader.read(1024), within)
    except ConnectionError:
        return b""
    finally:
        writer.close()


async def test_pass_forwards(proxy: FaultProxy) -> None:
    assert await roundtrip(proxy) == b"ping"


async def test_delay_adds_latency_both_ways(proxy: FaultProxy) -> None:
    proxy.set(Mode("delay", ms=100))
    start = clock.monotonic()
    assert await roundtrip(proxy) == b"ping"
    assert clock.monotonic() - start >= 0.19  # 100 ms each way


async def test_blackhole_answers_nothing_until_healed(proxy: FaultProxy) -> None:
    proxy.set(Mode("blackhole"))
    with pytest.raises(TimeoutError):
        await roundtrip(proxy, within=0.3)
    proxy.set(Mode())
    assert await roundtrip(proxy) == b"ping"


async def test_reset_refuses_and_drops(proxy: FaultProxy) -> None:
    reader, writer = await asyncio.open_connection("127.0.0.1", proxy.port)
    proxy.set(Mode("reset"))
    with contextlib.suppress(ConnectionError):
        writer.write(b"x")
        await writer.drain()
    with contextlib.suppress(ConnectionError):  # reset under us: empty read or RST
        assert await asyncio.wait_for(reader.read(10), 1.0) == b""
    writer.close()
    assert await roundtrip(proxy) == b""  # new connections are closed at once


async def test_drop_closes_some_connections(proxy: FaultProxy) -> None:
    proxy.set(Mode("drop", p=0.5))
    results = [await roundtrip(proxy) for _ in range(20)]
    assert b"" in results and b"ping" in results


def test_admin_query_parsing() -> None:
    assert parse_mode("set=delay&ms=300&jitter=50") == Mode("delay", 300, 50, 0)
    assert parse_mode("set=drop&p=0.2").describe() == "drop p=0.2"
