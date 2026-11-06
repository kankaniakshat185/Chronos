"""A TCP proxy that can misbehave, one per agent <-> control-plane link (P8.1).

Modes, switchable at runtime:

* `pass`: forward both ways.
* `delay(ms, jitter)`: forward every chunk after ms +- jitter (order kept).
* `drop(p)`: every chunk closes its connection with probability p (a flaky link:
  requests fail mid-way, clients reconnect).
* `blackhole`: keep accepting and reading, forward nothing, answer nothing. The
  nastiest partition: the client only learns from its own timeouts.
* `reset`: close every connection now and refuse new ones.

Used in-process by the chaos runner. Standalone, with an HTTP admin port:

    python -m chronos.testing.faultproxy --listen 7071 --target 7070 --admin 7171
    curl 'localhost:7171/mode?set=delay&ms=300&jitter=100'
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import socket
import struct
from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit

from chronos.common.clock import Clock, RealClock

CHUNK = 65536


@dataclass(frozen=True)
class Mode:
    name: str = "pass"  # pass | delay | drop | blackhole | reset
    ms: float = 0.0
    jitter: float = 0.0
    p: float = 0.0

    def describe(self) -> str:
        match self.name:
            case "delay":
                return f"delay {self.ms:g}±{self.jitter:g} ms"
            case "drop":
                return f"drop p={self.p:g}"
            case _:
                return self.name


PASS = Mode()


class FaultProxy:
    def __init__(
        self,
        target_port: int,
        listen_port: int,
        *,
        target_host: str = "127.0.0.1",
        rng: random.Random | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.target = (target_host, target_port)
        self.port = listen_port
        self.mode = PASS
        self._rng = rng or random.Random()
        self._clock = clock or RealClock()
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.port)

    async def stop(self) -> None:
        self._close_all()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for task in list(self._tasks):
            task.cancel()

    def set(self, mode: Mode) -> None:
        self.mode = mode
        if mode.name == "reset":
            self._close_all()

    def _close_all(self) -> None:
        for writer in list(self._writers):
            _abort(writer)
        self._writers.clear()

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.mode.name == "reset":
            _abort(writer)
            return
        self._writers.add(writer)
        try:
            up_reader, up_writer = await asyncio.open_connection(*self.target)
        except OSError:
            _abort(writer)
            return
        self._writers.add(up_writer)
        task = asyncio.current_task()
        if task is not None:
            self._tasks.add(task)
        try:
            await asyncio.gather(
                self._pipe(reader, up_writer, writer), self._pipe(up_reader, writer, up_writer)
            )
        finally:
            for w in (writer, up_writer):
                self._writers.discard(w)
                with contextlib.suppress(Exception):
                    w.close()
            if task is not None:
                self._tasks.discard(task)

    async def _pipe(
        self, src: asyncio.StreamReader, dst: asyncio.StreamWriter, other: asyncio.StreamWriter
    ) -> None:
        with contextlib.suppress(OSError, asyncio.CancelledError, ConnectionError):
            while data := await src.read(CHUNK):
                mode = self.mode
                if mode.name == "blackhole":
                    continue  # swallow it: nothing reaches the other side
                if mode.name == "drop" and self._rng.random() < mode.p:
                    _abort(dst)
                    _abort(other)
                    return
                if mode.name == "delay":
                    wait = mode.ms + self._rng.uniform(-mode.jitter, mode.jitter)
                    await self._clock.sleep(max(wait, 0.0) / 1000)
                if mode.name == "reset":
                    return
                dst.write(data)
                await dst.drain()
        with contextlib.suppress(Exception):
            dst.close()


def _abort(writer: asyncio.StreamWriter) -> None:
    """Close with RST, not FIN: what a reset link looks like to the peer."""
    sock = writer.get_extra_info("socket")
    if sock is not None:
        with contextlib.suppress(OSError):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    with contextlib.suppress(Exception):
        writer.close()


def parse_mode(query: str) -> Mode:
    q = {k: v[0] for k, v in parse_qs(query).items()}
    return Mode(
        q.get("set", "pass"),
        float(q.get("ms", 0)),
        float(q.get("jitter", 0)),
        float(q.get("p", 0)),
    )


async def _admin(proxy: FaultProxy, port: int) -> asyncio.Server:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        line = (await reader.readline()).decode()
        parts = line.split()
        url = urlsplit(parts[1] if len(parts) > 1 else "/")
        if url.path == "/mode" and url.query:
            proxy.set(parse_mode(url.query))
        body = proxy.mode.describe().encode() + b"\n"
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n" % len(body) + body)
        await writer.drain()
        writer.close()

    return await asyncio.start_server(handle, "127.0.0.1", port)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="python -m chronos.testing.faultproxy")
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--target", type=int, required=True)
    parser.add_argument("--admin", type=int, required=True)
    args = parser.parse_args(argv)

    async def run() -> None:
        proxy = FaultProxy(args.target, args.listen)
        await proxy.start()
        await _admin(proxy, args.admin)
        print(f"proxy :{args.listen} -> :{args.target}, admin on :{args.admin}", flush=True)
        await asyncio.Event().wait()

    asyncio.run(run())


if __name__ == "__main__":
    main()
