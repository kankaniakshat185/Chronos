"""Running probes against local pods (P10.1); the bookkeeping is `core.probes`.

`Prober` is what the agent calls; `RealProber` runs the three kinds against the pod's
process on this machine (workloads share the host network). The simulator substitutes
its own prober.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from typing import Protocol

import httpx

from chronos.api import Pod
from chronos.api.pod import Probe


class Prober(Protocol):
    async def check(self, pod: Pod, probe: Probe) -> bool: ...


class RealProber:
    async def check(self, pod: Pod, probe: Probe) -> bool:
        try:
            async with asyncio.timeout(probe.timeout_seconds):
                if probe.exec is not None:
                    return await self._exec(pod, probe.exec)
                if probe.http_get is not None:
                    return await self._http(probe.http_get.port, probe.http_get.path)
                assert probe.tcp_port is not None
                return await self._tcp(probe.tcp_port)
        except (TimeoutError, OSError, httpx.HTTPError):
            return False

    async def _exec(self, pod: Pod, command: list[str]) -> bool:
        env = {**os.environ, **pod.spec.env, "CHRONOS_POD_NAME": pod.metadata.name,
               "CHRONOS_NODE_NAME": pod.spec.node_name or ""}  # fmt: skip
        proc = await asyncio.create_subprocess_exec(
            *command, env=env, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
        )
        try:
            return await proc.wait() == 0
        finally:
            if proc.returncode is None:  # timed out: don't leave it behind
                with contextlib.suppress(ProcessLookupError):
                    proc.kill()
                await proc.wait()

    async def _http(self, port: int, path: str) -> bool:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://127.0.0.1:{port}{path}")
            return 200 <= response.status_code < 400

    async def _tcp(self, port: int) -> bool:
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.close()
        with contextlib.suppress(OSError):
            await writer.wait_closed()
        return True
