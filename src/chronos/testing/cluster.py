"""A real Chronos cluster for integration tests: `chronosd` + agents as subprocesses (P2.9).

Everything runs on free localhost ports under one temp directory. `kill_node()`
is a crash-stop of a whole node, like a machine losing power: it SIGKILLs the
agent and every pod process group it started (from the agent's pidfiles). The
demo's `scripts/kill-node.sh` calls the same function.
"""

from __future__ import annotations

import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import httpx
import psutil

from chronos.common.clock import RealClock

_clock = RealClock()


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def eventually[T](
    probe: Callable[[], Awaitable[T]], *, within: float = 10.0, interval: float = 0.05
) -> T:
    """Poll `probe` until it returns a truthy value; fail with the last value seen."""
    deadline = _clock.monotonic() + within
    while True:
        value = await probe()
        if value:
            return value
        if _clock.monotonic() > deadline:
            raise AssertionError(f"not true after {within}s; last value: {value!r}")
        await _clock.sleep(interval)


def alive(pgid: int) -> bool:
    """Does any non-zombie process of this process group still exist?

    A process that has exited but not been reaped (a zombie: e.g. a shim whose parent
    agent is SIGSTOPped) runs nothing, so it doesn't count. Linux reports such a group
    to `killpg(pgid, 0)` as existing; macOS doesn't, so check explicitly.
    """
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    for proc in psutil.process_iter(["status"]):
        try:
            if os.getpgid(proc.pid) == pgid and proc.info["status"] != psutil.STATUS_ZOMBIE:
                return True
        except (ProcessLookupError, psutil.Error):
            continue
    return False


def kill_node(data_dir: Path) -> None:
    """SIGKILL the agent whose data dir this is, and all of its pods' process groups."""
    agent_pid = data_dir / "agent.pid"
    if agent_pid.exists():
        with contextlib.suppress(ProcessLookupError):
            os.kill(int(agent_pid.read_text()), signal.SIGKILL)
    for pidfile in (data_dir / "pids").glob("*"):
        with contextlib.suppress(ProcessLookupError, ValueError):
            os.killpg(int(pidfile.read_text()), signal.SIGKILL)


@dataclass(frozen=True)
class Timings:
    """Fast, but still safe: F + K + drift = 1.0 + 0.5 + 0.2 = 1.7 < E = 2.0 (ADR-0012).
    F is 5 heartbeats, so a busy CI runner stalling the agent briefly doesn't fence."""

    heartbeat_interval: float = 0.2
    node_grace: float = 0.8
    eviction_timeout: float = 2.0
    resync_interval: float = 0.1
    fence_timeout: float = 1.0
    termination_grace: float = 0.5
    drift_margin: float = 0.2
    crash_backoff_base: float = 0.2
    crash_backoff_cap: float = 1.6


class Cluster:
    def __init__(self, root: Path, timings: Timings | None = None) -> None:
        self.root = root
        self.timings = timings or Timings()
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self._server: subprocess.Popen[bytes] | None = None
        self.agents: dict[str, subprocess.Popen[bytes]] = {}
        self.bind = os.environ.get("CHRONOS_BIND", "127.0.0.1")  # 0.0.0.0: scrape from Docker
        self.metrics_ports: dict[str, int] = {}

    def _spawn(self, log_name: str, args: list[str]) -> subprocess.Popen[bytes]:
        with (self.root / f"{log_name}.log").open("ab") as log:  # the child keeps its own fd
            return subprocess.Popen(
                [sys.executable, "-m", *args],
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )

    async def start(self) -> None:
        t = self.timings
        self._server = self._spawn(
            "chronosd",
            [
                "chronos.server.main", "run", "--host", self.bind, "--port", str(self.port),
                "--data-dir", str(self.root / "server"),
                "--heartbeat-interval", str(t.heartbeat_interval),
                "--node-grace", str(t.node_grace),
                "--eviction-timeout", str(t.eviction_timeout),
            ],
        )  # fmt: skip

        async def healthy() -> bool:
            try:
                async with httpx.AsyncClient() as c:
                    return (await c.get(f"{self.url}/healthz", timeout=0.5)).is_success
            except httpx.TransportError:
                return False

        await eventually(healthy, within=15.0)

    def kill_server(self) -> None:
        """SIGKILL chronosd, like a crash: no shutdown, no flushing."""
        if self._server is not None:
            self._server.kill()
            self._server.wait(timeout=5)
            self._server = None

    async def restart_server(self) -> None:
        """Crash chronosd and start a new one on the same port and data dir."""
        self.kill_server()
        await self.start()

    def write_targets(self, path: Path, host: str = "host.docker.internal") -> None:
        """Prometheus file service discovery for this cluster (ADR-0024)."""
        agents = [f"{host}:{p}" for _, p in sorted(self.metrics_ports.items())]
        path.write_text(
            json.dumps(
                [
                    {"targets": [f"{host}:{self.port}"], "labels": {"component": "chronosd"}},
                    {"targets": agents, "labels": {"component": "agent"}},
                ]
            )
        )

    def agent_dir(self, name: str) -> Path:
        return self.root / "agents" / name

    def start_agent(
        self,
        name: str,
        cpu: str = "4",
        mem: str = "8Gi",
        resources: dict[str, int] | None = None,
        server: str | None = None,  # e.g. through a proxy that a test can cut
    ) -> None:
        t = self.timings
        args = [
            "chronos.agent.main", "run", "--name", name, "--cpu", cpu, "--mem", mem,
            "--server", server or self.url, "--data-dir", str(self.agent_dir(name)),
            "--heartbeat-interval", str(t.heartbeat_interval),
            "--resync-interval", str(t.resync_interval),
            "--fence-timeout", str(t.fence_timeout),
            "--termination-grace", str(t.termination_grace),
            "--drift-margin", str(t.drift_margin),
            "--crash-backoff-base", str(t.crash_backoff_base),
            "--crash-backoff-cap", str(t.crash_backoff_cap),
        ]  # fmt: skip
        for res, units in (resources or {}).items():
            args += ["--resource", f"{res}={units}"]
        port = self.metrics_ports.setdefault(name, free_port())
        args += ["--metrics-port", str(port), "--metrics-host", self.bind]
        self.agents[name] = self._spawn(f"agent-{name}", args)

    def signal_agent(self, name: str, sig: signal.Signals) -> None:
        os.kill(int((self.agent_dir(name) / "agent.pid").read_text()), sig)

    def signal_pods(self, name: str, sig: signal.Signals) -> None:
        """Send `sig` to every pod process group (shim + workload) of this node."""
        for pidfile in (self.agent_dir(name) / "pids").glob("*"):
            with contextlib.suppress(ProcessLookupError, ValueError):
                os.killpg(int(pidfile.read_text()), sig)

    def kill_node(self, name: str) -> None:
        kill_node(self.agent_dir(name))
        if (proc := self.agents.pop(name, None)) is not None:
            proc.wait(timeout=5)

    def stop_agent(self, name: str) -> None:
        """Graceful: SIGTERM, so the agent stops its workloads first (SIGKILL if it hangs)."""
        proc = self.agents.pop(name)
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

    def pod_pgids(self) -> list[int]:
        return [
            int(f.read_text())
            for f in (self.root / "agents").glob("*/pids/*")
            if f.read_text().strip().isdigit()
        ]

    async def close(self) -> list[int]:
        """Stop everything; return the process groups of pods still alive (should be [])."""
        for name in list(self.agents):  # a test may have left some of them SIGSTOPped
            with contextlib.suppress(OSError, ValueError):
                self.signal_agent(name, signal.SIGCONT)
            self.signal_pods(name, signal.SIGCONT)
        for name in list(self.agents):
            self.stop_agent(name)
        if self._server is not None:
            self._server.terminate()
            try:
                self._server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._server.kill()  # fail the test, but don't leave it running
                self._server.wait(timeout=5)
                raise AssertionError("chronosd didn't stop within 10 s of SIGTERM") from None

        async def none_alive() -> bool:
            return not any(alive(g) for g in self.pod_pgids())

        with contextlib.suppress(AssertionError):
            await eventually(none_alive, within=3.0)
        leaked = [g for g in self.pod_pgids() if alive(g)]
        for pgid in leaked:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)
        return leaked

    def logs(self) -> str:
        return "\n".join(f"==> {p.name}\n{p.read_text()}" for p in sorted(self.root.glob("*.log")))


def main(argv: list[str] | None = None) -> int:
    """`python -m chronos.testing.cluster kill-node DATA_DIR`: what scripts/kill-node.sh runs."""
    import argparse  # noqa: PLC0415

    parser = argparse.ArgumentParser(prog="chronos.testing.cluster")
    sub = parser.add_subparsers(dest="command", required=True)
    kill = sub.add_parser("kill-node", help="crash-stop a node: SIGKILL its agent and pods")
    kill.add_argument("data_dir", type=Path)
    args = parser.parse_args(argv)
    if not (args.data_dir / "agent.pid").exists():
        print(f"no agent.pid in {args.data_dir}", file=sys.stderr)
        return 1
    kill_node(args.data_dir)
    print(f"killed agent and pods of {args.data_dir.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
