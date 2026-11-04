"""A minimal TCP forwarder that can be cut, to partition one agent from the control plane.

Just enough for a partition test; the full fault proxy (delay, drop, one-way) is PM-6.
"""

import asyncio
import contextlib

from chronos.testing.cluster import free_port


class Partition:
    def __init__(self, target_port: int) -> None:
        self.target_port = target_port
        self.port = free_port()
        self._server: asyncio.Server | None = None
        self._links: set[asyncio.StreamWriter] = set()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def heal(self) -> None:
        self._server = await asyncio.start_server(self._forward, "127.0.0.1", self.port)

    async def cut(self) -> None:
        """Refuse new connections and drop the open ones."""
        # Drop the links first: on 3.12, Server.wait_closed() waits for every open
        # connection, so the other order deadlocks.
        for writer in list(self._links):
            writer.close()
        self._links.clear()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def _forward(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self.target_port)
        except OSError:
            writer.close()
            return
        self._links |= {writer, up_writer}

        async def pipe(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
            with contextlib.suppress(OSError, asyncio.CancelledError):
                while data := await src.read(65536):
                    dst.write(data)
                    await dst.drain()
            dst.close()

        await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))
        self._links -= {writer, up_writer}
