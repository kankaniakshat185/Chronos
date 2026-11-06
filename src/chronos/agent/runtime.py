"""Runs pods as real OS processes, each under `chronos-shim` (ADR-0005, ADR-0012).

The shim starts in its own session, so its pid is the pod's process-group id, and
the workload is its child in the same group: a stop reaches the whole group, and the
shim fences the workload by itself if the agent's lease goes stale. Files under the
agent's data dir:

    agent.pid               the agent's own pid
    lease                   monotonic send time of the last accepted heartbeat
    pids/<pod uid>          the pod's process-group id (the shim's pid), removed once reaped
    pods/<pod uid>/stdout.log, stderr.log
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import sys
from collections.abc import Callable, Coroutine
from dataclasses import replace
from pathlib import Path

import psutil

from chronos.agent.providers import Providers
from chronos.api import Pod
from chronos.api.workload_types import workload_type
from chronos.common.clock import Clock, RealClock
from chronos.common.config import FencingConfig
from chronos.core.agent_sync import LocalProc
from chronos.core.fencing import FENCED_EXIT_CODE

log = logging.getLogger(__name__)

# Exit code for a command that couldn't be started (as in a shell).
EXIT_NOT_STARTABLE = 127


class ProcessRuntime:
    def __init__(
        self,
        data_dir: Path,
        node_name: str,
        fencing: FencingConfig,
        clock: Clock | None = None,
        providers: Providers | None = None,
    ) -> None:
        self._clock = clock or RealClock()
        self._providers = providers or Providers({})
        self._dir = data_dir
        self._node = node_name
        self._fencing = fencing
        self._grace = fencing.termination_grace
        self.eviction_timeout: float | None = None  # from the server, set at registration
        # Called when a local process starts or exits: what the node should report
        # changed, with no store event to say so (the agent's sync listens).
        self.on_change: Callable[[], None] | None = None
        self.lease = data_dir / "lease"
        self._procs: dict[str, LocalProc] = {}
        self._handles: dict[str, asyncio.subprocess.Process] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._stopping: set[str] = set()
        self._fenced: set[str] = set()
        (data_dir / "pids").mkdir(parents=True, exist_ok=True)
        (data_dir / "pods").mkdir(parents=True, exist_ok=True)
        (data_dir / "agent.pid").write_text(str(os.getpid()))

    def cleanup_orphans(self) -> list[int]:
        """Kill pod process groups left by an earlier agent on this data dir (P2.7).

        Run before registering. A pidfile is trusted only if its process is still a
        `chronos.shim` for that pod uid: after a reboot or PID reuse, the number may
        belong to an unrelated process, which must not be touched.
        """
        killed = []
        for pidfile in sorted((self._dir / "pids").glob("*")):
            uid = pidfile.name
            try:
                pgid = int(pidfile.read_text())
                argv = psutil.Process(pgid).cmdline()
            except (ValueError, psutil.Error):
                argv = []  # unreadable, or the process is gone
            if "chronos.shim" in argv and uid in argv:
                _killpg(pgid, signal.SIGKILL)
                killed.append(pgid)
                log.warning("orphan cleanup: killed pod %s (process group %d)", uid, pgid)
            pidfile.unlink(missing_ok=True)
        return killed

    def write_lease(self, last_ok_send: float) -> None:
        """Atomically (write + rename): a shim never reads a half-written lease."""
        tmp = self.lease.with_suffix(".tmp")
        tmp.write_text(repr(last_ok_send))
        os.replace(tmp, self.lease)

    def snapshot(self) -> dict[str, LocalProc]:
        return dict(self._procs)

    async def start(self, pod: Pod, *, restarts: int = 0, crashes: int = 0) -> None:
        """Start the pod under its shim. Raises ProviderError (nothing started) if the
        node can't supply its extended-resource units."""
        uid, name = pod.metadata.uid, pod.metadata.name
        logs = self._dir / "pods" / uid
        logs.mkdir(parents=True, exist_ok=True)
        command, type_env = workload_type(pod.spec.workload_type).prepare(pod)  # X-4
        assigned, unit_env = self._providers.allocate(uid, pod.requests)  # X-5
        env = {
            **os.environ,
            **pod.spec.env,
            **type_env,
            **unit_env,
            "CHRONOS_POD_UID": uid,
            "CHRONOS_POD_NAME": name,
            "CHRONOS_NODE_NAME": self._node,
            # The units this process holds, e.g. {"chronos.dev/slot": ["1"]} (X-5): for the
            # workload, and the ground truth the chaos checker verifies S12 against.
            "CHRONOS_ASSIGNED": json.dumps(assigned, sort_keys=True),
        }
        if self.eviction_timeout is None:
            raise RuntimeError("start() before registration: eviction timeout unknown")
        f = self._fencing
        argv = [
            sys.executable, "-m", "chronos.shim",
            "--lease", str(self.lease), "--pod-uid", uid,
            "--fence-timeout", str(f.fence_timeout),
            "--eviction-timeout", str(self.eviction_timeout),
            "--grace", str(f.termination_grace), "--poll", str(f.shim_poll),
            "--", *command,
        ]  # fmt: skip
        try:
            with (logs / "stdout.log").open("ab") as out, (logs / "stderr.log").open("ab") as err:
                proc = await asyncio.create_subprocess_exec(
                    *argv, env=env, stdout=out, stderr=err, start_new_session=True
                )
        except OSError as exc:  # missing binary, permission denied, ...
            log.warning("pod %s: can't start %s: %s", name, pod.spec.command, exc)
            self._providers.release(uid)
            now = self._clock.monotonic()
            self._procs[uid] = LocalProc(
                uid, name, 0, EXIT_NOT_STARTABLE, now, now, restarts, crashes
            )
            self._changed()
            return
        self._pidfile(uid).write_text(str(proc.pid))
        self._handles[uid] = proc
        self._procs[uid] = LocalProc(
            uid, name, proc.pid, started_at=self._clock.monotonic(), restarts=restarts,
            crashes=crashes, assigned=assigned,
        )  # fmt: skip
        self._spawn(self._wait(uid, proc))
        log.info("pod %s: started pid %d", name, proc.pid)
        self._changed()

    def stop(self, uid: str, *, fenced: bool = False) -> None:
        """SIGTERM the group, SIGKILL it after the grace period. Returns at once.

        `fenced`: record the exit as FENCED_EXIT_CODE (not a crash), so the pod is
        started again, not failed, once the agent leaves Fenced mode.
        """
        if fenced:
            self._fenced.add(uid)
        if uid in self._handles and uid not in self._stopping:
            self._stopping.add(uid)
            self._spawn(self._stop(uid, self._handles[uid]))

    def kill_now(self, uid: str) -> None:
        """SIGKILL the group at once (fenced): replacements may already be running."""
        self._fenced.add(uid)
        if (proc := self._handles.get(uid)) is not None:
            _killpg(proc.pid, signal.SIGKILL)

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
        self._providers.release(uid)  # S12: units are free once the process is gone
        if uid in self._fenced:
            self._fenced.discard(uid)
            code = FENCED_EXIT_CODE
        self._pidfile(uid).unlink(missing_ok=True)
        self._handles.pop(uid, None)
        current = self._procs.get(uid)
        if current is not None and current.pid == proc.pid:  # not already restarted
            self._procs[uid] = replace(current, exit_code=code, exited_at=self._clock.monotonic())
        log.info("pod %s: pid %d exited with %d", uid, proc.pid, code)
        self._changed()

    def _changed(self) -> None:
        if self.on_change is not None:
            self.on_change()

    def _pidfile(self, uid: str) -> Path:
        return self._dir / "pids" / uid

    def _spawn(self, coro: Coroutine[object, object, None]) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)


def _killpg(pgid: int, sig: signal.Signals) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pgid, sig)
