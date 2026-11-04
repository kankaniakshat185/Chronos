"""chronosctl against the real API app in-process (no sockets), with a fake clock.

Golden files live in tests/golden/; regenerate with UPDATE_GOLDEN=1 after an
intended output change, and review the diff.
"""

import os
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
from typer.testing import CliRunner

from chronos.api import Deployment, Node, NodeCondition, Pod, PodPhase
from chronos.cli import main as cli
from chronos.client import HttpStoreClient, LocalStoreClient
from chronos.common.clock import FakeClock
from chronos.server.app import create_app
from chronos.store import Store
from chronos.store.leases import Leases

GOLDEN = Path(__file__).parent.parent / "golden"
SLOT = "chronos.dev/slot"
runner = CliRunner()


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store(clock: FakeClock, monkeypatch: pytest.MonkeyPatch) -> Iterator[Store]:
    store = Store(clock, uid_factory=iter(f"uid-{i}" for i in range(1000)).__next__)
    app = create_app(LocalStoreClient(store, Leases(clock)))
    monkeypatch.setattr(
        cli, "connect", lambda url: HttpStoreClient(url, transport=httpx.ASGITransport(app=app))
    )
    monkeypatch.setattr(cli, "clock", clock)
    yield store


def chronosctl(*args: str) -> tuple[int, str]:
    result = runner.invoke(cli.app, list(args))
    return result.exit_code, result.output


def golden(name: str, actual: str) -> None:
    path = GOLDEN / name
    if os.environ.get("UPDATE_GOLDEN"):
        path.write_text(actual)
    assert actual == path.read_text()


def demo_cluster(store: Store, clock: FakeClock) -> None:
    """The demo's state right after step 5 (A.7)."""
    for name, extra in (("plain-1", {}), ("slot-a", {SLOT: 1}), ("slot-b", {SLOT: 1})):
        node = Node.model_validate(
            {
                "metadata": {"name": name},
                "spec": {"capacity": {"cpu": "4", "memory": "8Gi", **extra}},
            }
        )
        node = store.create(node)
        node.status.condition = NodeCondition.READY
        store.update_status(node)
    clock.advance(90)
    placements = [("web-k2x9p", "plain-1", 4101), ("slot-demo-b7c2m", "slot-a", 4102),
                  ("slot-demo-q8n4z", "slot-b", 4103), ("slot-demo-t5v3w", "", None)]  # fmt: skip
    for name, node_name, pid in placements:
        requests = {"cpu": "500m", "memory": "128Mi"} | ({SLOT: 1} if "slot" in name else {})
        pod = store.create(
            Pod.model_validate(
                {
                    "metadata": {"name": name},
                    "spec": {"command": ["x"], "resources": {"requests": requests}},
                }
            )
        )
        if node_name:
            pod = store.bind(name, node_name, pod.metadata.resource_version)
            pod.status.phase, pod.status.pid = PodPhase.RUNNING, pid
        else:
            pod.status.reason = "Unschedulable"
            pod.status.message = "0/3 nodes available: Insufficient chronos.dev/slot (3/3 nodes)"
        store.update_status(pod)
        clock.advance(5)
    dep = store.create(
        Deployment.model_validate(
            {"metadata": {"name": "web"}, "spec": {"replicas": 3, "template": {"command": ["x"]}}}
        )
    )
    dep.status.replicas, dep.status.ready_replicas = 3, 2
    store.update_status(dep)
    clock.advance(3 * 3600)


def test_get_nodes(store: Store, clock: FakeClock) -> None:
    demo_cluster(store, clock)
    code, out = chronosctl("get", "nodes")
    assert code == 0
    golden("get_nodes.txt", out)


def test_get_pods(store: Store, clock: FakeClock) -> None:
    demo_cluster(store, clock)
    golden("get_pods.txt", chronosctl("get", "pods")[1])
    golden("get_pods_wide.txt", chronosctl("get", "pods", "-o", "wide")[1])


def test_get_deployments(store: Store, clock: FakeClock) -> None:
    demo_cluster(store, clock)
    golden("get_deployments.txt", chronosctl("get", "deployments")[1])


WEB = """\
kind: Deployment
metadata: {{name: web}}
spec:
  replicas: {replicas}
  template:
    command: [python, -m, chronos.testing.workload]
    resources: {{requests: {{cpu: 250m, memory: 64Mi}}}}
"""


def test_apply_create_update_no_op(store: Store, tmp_path: Path) -> None:
    manifest = tmp_path / "web.yaml"
    manifest.write_text(WEB.format(replicas=3))
    assert chronosctl("apply", "-f", str(manifest)) == (0, "deployment/web created\n")
    assert chronosctl("apply", "-f", str(manifest)) == (0, "deployment/web unchanged\n")
    manifest.write_text(WEB.format(replicas=5))
    assert chronosctl("apply", "-f", str(manifest)) == (0, "deployment/web configured\n")
    dep = store.get(Deployment, "web")
    assert (dep.spec.replicas, dep.metadata.generation) == (5, 2)
    assert dep.spec.template.resources.requests == {"cpu": 250, "memory": 64 * 2**20}


def test_apply_retries_when_a_controller_wrote_in_between(store: Store, tmp_path: Path) -> None:
    manifest = tmp_path / "web.yaml"
    manifest.write_text(WEB.format(replicas=3))
    chronosctl("apply", "-f", str(manifest))
    dep = store.get(Deployment, "web")
    dep.status.replicas = 3
    store.update_status(dep)  # bumps resource_version, as the controller does
    manifest.write_text(WEB.format(replicas=4))
    assert chronosctl("apply", "-f", str(manifest)) == (0, "deployment/web configured\n")


def test_apply_multiple_documents(store: Store, tmp_path: Path) -> None:
    manifest = tmp_path / "two.yaml"
    manifest.write_text(
        WEB.format(replicas=1) + "---\n" + WEB.format(replicas=1).replace("web", "api")
    )
    code, out = chronosctl("apply", "-f", str(manifest))
    assert (code, out) == (0, "deployment/web created\ndeployment/api created\n")


def test_typo_field_names_the_field(store: Store, tmp_path: Path) -> None:
    manifest = tmp_path / "typo.yaml"
    manifest.write_text(WEB.format(replicas=3).replace("replicas:", "replicaz:"))
    code, out = chronosctl("apply", "-f", str(manifest))
    assert code == 1
    assert "spec.replicaz: Extra inputs are not permitted" in out
    assert "spec.replicas: Field required" in out
    assert store.list(Deployment)[0] == []


def test_unknown_kind_in_manifest(store: Store, tmp_path: Path) -> None:
    manifest = tmp_path / "job.yaml"
    manifest.write_text("kind: Job\nmetadata: {name: j}\n")
    code, out = chronosctl("apply", "-f", str(manifest))
    assert code == 1 and "kind must be one of" in out


def test_server_side_rejection_is_reported(store: Store, tmp_path: Path) -> None:
    manifest = tmp_path / "pod.yaml"
    manifest.write_text("kind: Pod\nmetadata: {name: p}\nspec: {command: [x], node_name: n1}\n")
    code, out = chronosctl("apply", "-f", str(manifest))
    assert code == 1 and "use bind" in out


def test_scale_and_delete(store: Store, tmp_path: Path) -> None:
    manifest = tmp_path / "web.yaml"
    manifest.write_text(WEB.format(replicas=3))
    chronosctl("apply", "-f", str(manifest))
    assert chronosctl("scale", "deployment", "web", "--replicas", "5") == (
        0,
        "deployment/web scaled to 5\n",
    )
    assert store.get(Deployment, "web").spec.replicas == 5
    assert chronosctl("delete", "deploy", "web") == (0, "deployment/web deleted\n")
    assert store.list(Deployment)[0] == []


def test_delete_missing_is_an_error(store: Store) -> None:
    code, out = chronosctl("delete", "pod", "nope")
    assert code == 1 and "Pod 'nope' not found" in out


def test_unknown_kind_argument(store: Store) -> None:
    code, out = chronosctl("delete", "jobs", "x")
    assert code == 2 and "unknown kind" in out


def test_unreachable_server(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    monkeypatch.setattr(
        cli, "connect", lambda url: HttpStoreClient(url, transport=httpx.MockTransport(refuse))
    )
    code, out = chronosctl("--server", "http://127.0.0.1:1", "get", "nodes")
    assert code == 1
    assert "can't reach chronosd at http://127.0.0.1:1" in out


def test_get_with_nothing_there(store: Store) -> None:
    assert chronosctl("get", "pods") == (0, "NAME   STATUS   AGE\n")
    assert chronosctl("get", "nodes")[0] == 0
    assert chronosctl("get", "deployments")[0] == 0


@pytest.mark.parametrize("example", sorted((GOLDEN.parent.parent / "examples").glob("*.yaml")))
def test_examples_are_valid_manifests(example: Path) -> None:
    from chronos.cli.manifest import load  # noqa: PLC0415

    assert load(example)
