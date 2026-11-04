"""Helpers shared by the integration tests."""

import secrets
import subprocess
import sys

from chronos.api import Deployment, Node, NodeCondition, Pod, PodPhase
from chronos.client import HttpStoreClient
from chronos.store import Conflict
from chronos.testing.cluster import eventually

# Per test session, so live_processes() never counts another cluster's workloads
# (e.g. a demo cluster running on the same machine, or another xdist worker).
SESSION = secrets.token_hex(3)


def label_of(name: str) -> str:
    return f"{name}.{SESSION}"


def workload_command(label: str) -> list[str]:
    return [sys.executable, "-m", "chronos.testing.workload", "--label", label]


def deployment(
    name: str, replicas: int, requests: dict[str, str | int] | None = None
) -> Deployment:
    return Deployment.model_validate(
        {
            "metadata": {"name": name},
            "spec": {
                "replicas": replicas,
                "template": {
                    "command": workload_command(label_of(name)),
                    "resources": {"requests": requests or {"cpu": "100m", "memory": "64Mi"}},
                },
            },
        }
    )


def live_processes(name: str) -> set[int]:
    """PIDs of live (non-zombie) workload processes of deployment `name` in this session."""
    label = label_of(name)
    # -A means "all processes" on both Linux and macOS (Linux's -a skips session leaders,
    # and every pod process is one).
    out = subprocess.run(
        ["ps", "-A", "-o", "pid=,stat=,command="], capture_output=True, text=True, check=True
    ).stdout
    pids = set()
    for line in out.splitlines():
        pid, stat, *argv = line.split()
        if stat.startswith("Z") or "chronos.testing.workload" not in argv:
            continue
        i = argv.index("--label") if "--label" in argv else -1
        if 0 <= i < len(argv) - 1 and argv[i + 1] == label:
            pids.add(int(pid))
    return pids


async def wait_nodes_ready(api: HttpStoreClient, *names: str) -> None:
    async def ready() -> bool:
        nodes = {n.metadata.name: n for n in (await api.list(Node))[0]}
        return all(n in nodes and nodes[n].status.condition == NodeCondition.READY for n in names)

    await eventually(ready)


async def owned_pods(api: HttpStoreClient, name: str) -> list[Pod]:
    pods, _ = await api.list(Pod, labels={"chronos.dev/deployment": name})
    return pods


async def wait_running(api: HttpStoreClient, name: str, n: int, within: float = 10.0) -> list[Pod]:
    """Exactly `n` Running pods (store) backed by exactly those live PIDs (OS)."""

    async def settled() -> list[Pod] | None:
        pods = await owned_pods(api, name)
        running = [p for p in pods if p.status.phase == PodPhase.RUNNING]
        active = [p for p in pods if p.status.phase not in (PodPhase.FAILED, PodPhase.SUCCEEDED)]
        pids = {p.status.pid for p in running}
        if len(running) == n == len(active) and pids == live_processes(name):
            return running
        return None

    return await eventually(settled, within=within)


async def scale(api: HttpStoreClient, name: str, replicas: int) -> None:
    """Read-modify-write with retry: the controller updates status concurrently."""
    for _ in range(20):
        d = await api.get(Deployment, name)
        d.spec.replicas = replicas
        try:
            await api.update(d)
            return
        except Conflict:
            continue
    raise AssertionError(f"scale {name}: kept conflicting")
