"""Runs pods as real OS processes (ADR-0005; MVP: no shim yet, ADR-0022).

Each pod's command runs in its own session, so its pid is also its process-group
id and a stop reaches the whole group. Files under the agent's data dir:

    agent.pid               the agent's own pid
    pids/<pod uid>          the pod's process-group id, removed once reaped
    pods/<pod uid>/stdout.log, stderr.log
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from collections.abc import Coroutine
from pathlib import Path

from chronos.api import Pod
from chronos.core.agent_sync import LocalProc

log = logging.getLogger(__name__)

# Exit code for a command that couldn't be started (as in a shell).
EXIT_NOT_STARTABLE = 127


class ProcessRuntime:
    def __init__(self, data_dir: Path, node_name: str, termination_grace: float) -> None:
        self._dir = data_dir
        self._node = node_name
        self._grace = termination_grace
        self._procs: dict[str, LocalProc] = {}
        self._handles: dict[str, asyncio.subprocess.Process] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._stopping: set[str] = set()
        (data_dir / "pids").mkdir(parents=True, exist_ok=True)
        (data_dir / "pods").mkdir(parents=True, exist_ok=True)
        (data_dir / "agent.pid").write_text(str(os.getpid()))

    def snapshot(self) -> dict[str, LocalProc]:
        return dict(self._procs)

    async def start(self, pod: Pod) -> None:
        uid, name = pod.metadata.uid, pod.metadata.name
        logs = self._dir / "pods" / uid
        logs.mkdir(parents=True, exist_ok=True)
        env = {
            **os.environ,
            **pod.spec.env,
            "CHRONOS_POD_UID": uid,
            "CHRONOS_POD_NAME": name,
            "CHRONOS_NODE_NAME": self._node,
        }
        try:
            with (logs / "stdout.log").open("ab") as out, (logs / "stderr.log").open("ab") as err:
                proc = await asyncio.create_subprocess_exec(
                    *pod.spec.command, env=env, stdout=out, stderr=err, start_new_session=True
                )
        except OSError as exc:  # missing binary, permission denied, ...
            log.warning("pod %s: can't start %s: %s", name, pod.spec.command, exc)
            self._procs[uid] = LocalProc(uid, name, pid=0, exit_code=EXIT_NOT_STARTABLE)
            return
        self._pidfile(uid).write_text(str(proc.pid))
        self._handles[uid] = proc
        self._procs[uid] = LocalProc(uid, name, pid=proc.pid)
        self._spawn(self._wait(uid, proc))
        log.info("pod %s: started pid %d", name, proc.pid)

    def stop(self, uid: str) -> None:
        """SIGTERM the group, SIGKILL it after the grace period. Returns at once."""
        if uid in self._handles and uid not in self._stopping:
            self._stopping.add(uid)
            self._spawn(self._stop(uid, self._handles[uid]))

    def forget(self, uid: str) -> None:
        self._procs.pop(uid, None)

    async def stop_all(self) -> None:
        for uid in list(self._handles):
            self.stop(uid)
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _stop(self, uid: str, proc: asyncio.subprocess.Process) -> None:
        _killpg(proc.pid, signal.SIGTERM)
        try:
            async with asyncio.timeout(self._grace):
                await proc.wait()
        except TimeoutError:
            log.info("pod %s: still running after %.1fs grace, SIGKILL", uid, self._grace)
            _killpg(proc.pid, signal.SIGKILL)
        # Leftover group members (the leader may have exited first) get no grace.
        _killpg(proc.pid, signal.SIGKILL)
        self._stopping.discard(uid)

    async def _wait(self, uid: str, proc: asyncio.subprocess.Process) -> None:
        code = await proc.wait()
        self._pidfile(uid).unlink(missing_ok=True)
        self._handles.pop(uid, None)
        if (current := self._procs.get(uid)) is not None:
            self._procs[uid] = LocalProc(uid, current.pod_name, current.pid, exit_code=code)
        log.info("pod %s: pid %d exited with %d", uid, proc.pid, code)

    def _pidfile(self, uid: str) -> Path:
        return self._dir / "pids" / uid

    def _spawn(self, coro: Coroutine[object, object, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


def _killpg(pgid: int, sig: signal.Signals) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pgid, sig)
