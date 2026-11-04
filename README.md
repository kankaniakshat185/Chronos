# Chronos

A small container orchestrator, in the spirit of Kubernetes, built from scratch in Python to
learn and demonstrate how orchestration works: declarative desired state, a versioned store
with compare-and-swap, a filter/score scheduler, node heartbeats and failure detection, and
level-triggered reconciliation. Workloads are real OS processes.

> You declare desired state in YAML. Chronos places each replica on a node that fits its CPU,
> memory and extended-resource requests (an extended resource such as `chronos.dev/slot` is an
> integer, non-shareable, **hard** constraint), runs it as a real OS process, and when a node
> dies it detects the failure and reschedules the work. It never places a workload on a node
> that doesn't advertise enough free units of a requested extended resource, even when that
> means the workload waits. Every one of those statements is backed by a property-based or
> real-process test.

**Status: `v0.1-mvp` + PM-1 (self-fencing).** The core loop works end to end, and workloads
are fenced when their node loses contact with the control plane, so a paused, crashed or
partitioned node can't cause duplicate replicas. The
[limitations](#limitations-what-chronos-does-not-do-yet) are listed below, each with the block
that fixes it, plus the [design limitations](#design-limitations-deliberate) that stay.

## Quickstart

Needs [uv](https://docs.astral.sh/uv/) and macOS or Linux.

```bash
uv sync                      # Python 3.12 + dependencies from uv.lock
make check                   # ruff, mypy --strict, ~300 tests (unit, property, real-process)
make demo-cluster            # chronosd on :7070 + agents plain-1, slot-a, slot-b
```

In a second terminal:

```bash
uv run chronosctl get nodes
uv run chronosctl apply -f examples/web.yaml
uv run chronosctl get pods -o wide
```

## Demo

`plain-1` has no slots; `slot-a` and `slot-b` advertise `chronos.dev/slot=1` each. Demo
timings: heartbeat 0.5 s, Unknown after 2 s of silence, NotReady (and eviction) after 5 s.

| Step | Run | What happens |
|------|-----|--------------|
| 1 | `chronosctl get nodes` | 3 nodes `Ready`; the slot column reads `—`, `0/1`, `0/1` |
| 2 | `chronosctl apply -f examples/web.yaml` | 3 `web` pods go Pending → Bound → Running, spread over the nodes, each with a real PID |
| 3 | `ps -A -o pid,pgid,command \| awk '$2 == <pid>'` | the pod's process group: its `chronos-shim` and the real workload under it |
| 4 | `chronosctl scale deployment web --replicas 5` | 2 more pods converge |
| 5 | `chronosctl apply -f examples/slot-demo.yaml` | 2 slot pods run on `slot-a` and `slot-b`; the 3rd stays Pending: `0/3 nodes available: Insufficient chronos.dev/slot (3/3 nodes)` |
| 6 | `chronosctl scale deployment slot-demo --replicas 2` | the Pending pod is the one removed |
| 7 | `scripts/kill-node.sh slot-a` | `slot-a` → Unknown → NotReady; its pods → `Failed/NodeLost`; `web` is replaced elsewhere; the slot pod's replacement **waits** instead of landing on `plain-1` |
| 8 | `scripts/start-agent.sh slot-a --resource chronos.dev/slot=1` | `slot-a` is Ready again and the waiting pod takes its slot |

The same scenario runs as an automated test,
[`test_demo_scenario.py`](tests/integration/test_demo_scenario.py), and unattended against a
fresh copy of the repo with `scripts/fresh-clone-check.sh`.

## Architecture (as built in `v0.1-mvp`)

```mermaid
flowchart LR
    subgraph CP["chronosd (one process)"]
        API["API server (FastAPI)"]
        STORE[("Versioned store<br/>in memory (no WAL yet)")]
        DC["Deployment controller"]
        SCH["Scheduler"]
        NLC["Node lifecycle controller<br/>(+ eviction)"]
        API <--> STORE
        DC <-->|"poll 250 ms / CAS"| STORE
        SCH <-->|"poll 250 ms / bind"| STORE
        NLC <-->|"poll 250 ms / CAS"| STORE
    end
    CLI["chronosctl"] -->|HTTP| API
    subgraph N1["chronos-agent (node)"]
        A1["heartbeat · fence watch · sync"] --> R1["process runtime"]
        A1 -.->|"writes"| L1[/"lease file<br/>(send time of last accepted heartbeat)"/]
        R1 --> S1a["chronos-shim"] --> W1a(("workload"))
        R1 --> S1b["chronos-shim"] --> W1b(("workload"))
        L1 -.->|"read every 100 ms"| S1a
        L1 -.->|"read every 100 ms"| S1b
    end
    A1 -->|"heartbeat; poll pods?node=self every 500 ms; status"| API
```

- **One store holds the truth.** Every write bumps a global revision and is a compare-and-swap
  on the object's `resource_version`, so a stale writer gets `409 Conflict` instead of
  silently overwriting ([ADR-0007](docs/adr/0007-versioned-single-writer-store.md)).
- **Functional core, imperative shell.** Scheduling, reconciliation, node health and agent
  sync are pure functions in `src/chronos/core/` with no I/O or clock access; the loops around
  them are thin ([ADR-0006](docs/adr/0006-functional-core-imperative-shell.md)). That is what
  makes the property tests below possible.
- **Level-triggered.** Every tick recomputes the full diff from authoritative state, so
  nothing depends on having seen a particular event.
- **Agents pull.** The control plane never connects to an agent.
- **Self-fencing.** Every workload runs under a small `chronos-shim` that reads the agent's
  lease file. Once no heartbeat has been accepted for `fence_timeout` F (measured from the
  heartbeat's *send* time), the agent and every shim stop their workloads within the grace K,
  even if the agent itself is dead. The control plane only starts replacements after
  `eviction_timeout` E, and both sides refuse timings unless F + K + drift < E, so old and new
  never overlap ([ADR-0012](docs/adr/0012-agent-self-fencing.md)).

**Remaining MVP shortcuts** ([ADR-0022](docs/adr/0022-week-1-mvp-scope.md)): controllers poll
the store instead of watching it, and state is in memory only. Each has a scheduled fix,
listed under Limitations.

## What is guaranteed, and how it's tested

| Invariant | Status | Evidence |
|-----------|--------|----------|
| **S1** a pod is bound to at most one node, once | ✅ | [store unit tests](tests/unit/test_store.py), [stateful model test](tests/property/test_store_model.py) |
| **S2** for every resource name, Σ requests ≤ capacity on every node | ✅ | [scheduler properties](tests/property/test_schedule_props.py), [bind property](tests/property/test_store_bind_props.py), [integration](tests/integration/test_scheduling.py) |
| **S3** at most R live processes per deployment | ✅ under agent pause, agent crash and network partition | integration, sampling live process groups every 20 ms: [paused agent, killed agent, partitioned node](tests/integration/test_fencing.py), [quick agent restart](tests/integration/test_orphans.py), [whole-node loss](tests/integration/test_deployments.py). A **whole-node freeze** has a bounded window of one shim poll ([test](tests/integration/test_fencing.py); design limitation 2). |
| **S4** a cut-off node's workloads stop within F + K | ✅ | [paused agent, partitioned node](tests/integration/test_fencing.py); [shim on real processes](tests/integration/test_shim.py); [fence boundaries](tests/unit/test_fencing.py); [send-time rule](tests/unit/test_agent_fencing.py) |
| **S5** revisions strictly increase; a stale compare-and-swap never succeeds | ✅ | [stateful model test](tests/property/test_store_model.py) |
| **S11** extended resources are a hard constraint, never a soft preference | ✅ | [scheduler properties](tests/property/test_schedule_props.py), [bind property](tests/property/test_store_bind_props.py), [integration](tests/integration/test_scheduling.py), [demo scenario](tests/integration/test_demo_scenario.py) |
| **L1** a dead node is detected within the configured windows | ✅ | [boundary table](tests/unit/test_node_health.py), [lifecycle on a fake clock](tests/unit/test_node_lifecycle.py), [integration](tests/integration/test_agent.py) |
| **L2** deployments converge to their replica count | ✅ | [reconcile properties](tests/property/test_reconcile_props.py), [integration](tests/integration/test_deployments.py) |
| **L3** a pending pod that fits somewhere gets placed | ✅ | [scheduler properties](tests/property/test_schedule_props.py), [demo scenario, step 8](tests/integration/test_demo_scenario.py) |
| **L5** no processes left behind after delete or teardown | ✅ | [integration](tests/integration/test_deployments.py); every integration test fails if a pod process outlives its cluster ([fixture](tests/integration/conftest.py)) |
| **S12** no unit of an extended resource is held by two live pods | ✅ | [provider property](tests/property/test_providers_props.py), [integration](tests/integration/test_scheduling.py) |
| **S6, S7, S8, L4** | ❌ not yet | watch, rollouts, durability: see Limitations |

**Modularity:** a resource that exists only in test code, `example.com/widget`, plus a
test-only scheduler plugin, pass the scheduling and bind properties with no production-code
changes ([`test_modularity_widget.py`](tests/property/test_modularity_widget.py)); a test-only
workload type and resource provider run a real process through the unchanged agent runtime
([`test_seams.py`](tests/integration/test_seams.py)).

**How the tests are layered:** unit tests, Hypothesis property and stateful tests (100 examples
locally, 1 000 in CI, 20 000 with `make test-nightly`), and integration tests that run a real
`chronosd` and agents as subprocesses. CI runs everything on Ubuntu and macOS.

## Limitations: what Chronos does not do yet

> **What this does NOT do yet.** Each item names the planned fix. PM-1 closed the items
> about fencing, crash backoff and one-phase deletion.

1. **No durability.** All cluster state is in memory. Restarting `chronosd` loses every Deployment, Pod and Node. Agents re-register, find no desired state, and stop their workloads. → WAL + snapshots, PM-2 ([ADR-0015](docs/adr/0015-wal-and-snapshots.md)).
3. **No chaos testing or deterministic simulation.** Fault testing is limited to scripted integration tests (whole-node kill, agent pause and crash, a cut partition, a whole-node freeze). There are no randomized fault schedules, network partitions, delays or message loss, and no canary bugs proving test strength. → PM-4 (simulator), PM-6 (chaos harness).
4. **No GPU/CUDA support: planned, Stage 2 (deferred).** Nothing detects or uses GPUs, and no CUDA runs. Stage 1 provides the seams GPU support plugs into: extended resources (X-1/X-2), workload types (X-4) and resource providers (X-5), proven in-process by PX.9 ([`test_modularity_widget.py`](tests/property/test_modularity_widget.py)).
5. **Polling, not watch.** Controllers poll the store every 250 ms and agents poll every 500 ms; this is O(cluster) per tick. It's correct (level-triggered) but not scalable, and not measured beyond 3 nodes. → watch + informers + work queue + expectations, PM-3.
7. **No resource isolation or enforcement.** CPU, memory and extended-resource requests are used for *placement accounting only*; a process can use more than it requested. → memory-limit enforcement PM-9a (P10.3); CPU stays advisory (ADR-0014).
8. **Single machine, single control plane.** Nodes are processes on one host; there's no control-plane HA. → Backlog (P11.x, ADR-0018); not scheduled.
9. **No benchmarks.** Timings visible in the demo are a single run on a laptop, not measured distributions. → B1–B6, PM-6.
10. **No jobs / run-to-completion workloads: planned, Stage 2 (deferred)** (ADR-0020). Only long-running Deployments. The kind registry (X-6, PM-3) makes Jobs purely additive later.
12. **Scheduling policy is basic.** `LeastAllocated` only. → `MostAllocated`, PM-5. Resource-aware stranding avoidance for GPUs (`GpuAvoidance`) is **planned, Stage 2 (deferred)**.
13. **No auth, TLS or multi-tenancy.** It's built for a trusted localhost. (Non-goal.)
14. **Extensions are in-process only.** There's no plugin loader; new resources, workload types and providers are registered by in-tree code. (Conscious limit, ADR-0023; a loader may be added at the start of Stage 2 if needed.)
15. **No metrics or dashboards.** There's no `/metrics` endpoint, Prometheus or Grafana yet; observe the cluster with `chronosctl get`. → PM-5 (P7.4, P7.6, [ADR-0024](docs/adr/0024-prometheus-grafana-observability.md)).

## Design limitations (deliberate)

These stay true at the end of Stage 1; they are trade-offs, not missing work.

1. **A control-plane outage longer than `fence_timeout` stops every workload (fencing Option A).** Agents can't tell a dead control plane from a partition, so they fence. This trades availability for safety (no duplicates). Real Kubernetes keeps pods running while the API server is down; Chronos deliberately doesn't. When the control plane returns, startup grace prevents evictions and agents restart their still-bound pods (L4). A smarter fencing rule (e.g. peer checks) was rejected as too complex; the real fix is control-plane HA, which is in the Backlog (P11.x, ADR-0018). *Today the return path isn't there yet: state is in memory, so a restarted control plane has forgotten everything (limitation 1); startup grace and this test arrive with PM-2.*
2. **"No duplicate replicas" (S3/S4) is scoped to agent pause, agent crash and network partition.** For a **whole-node freeze** (a VM pause that stops the shims too), there's a bounded window of **at most one shim poll (100 ms)** after the node resumes, during which a stale workload may run alongside its replacement before the shim SIGKILLs it. Kubernetes has the same limit. The README must not claim "no duplicates under any pause".
3. **Fencing assumes the node's monotonic clock keeps advancing across a freeze.** That holds for Chronos's single-host setup (all "nodes" share the host clock). On real VMs it depends on the hypervisor's clock handling; if the guest clock stalled, the window in #2 could extend up to `fence_timeout`.

## Roadmap

The block names used above, in build order:

| Block | What it adds |
|-------|--------------|
| **PM-1** ✅ | Self-fencing (agent + per-pod shim), restart with backoff, orphan cleanup, two-phase deletion, workload-type and resource-provider seams |
| **PM-2** | Durability: write-ahead log with CRC framing, snapshots, crash-point-tested recovery |
| **PM-3** | Watch + informers + work queue + expectations, replacing polling |
| **PM-4** | Deterministic simulation with canary bugs |
| **PM-5** | Full CLI, structured logs, events, `/metrics`, Prometheus + Grafana |
| **PM-6** | Chaos harness (partitions, pauses, delays) and benchmarks |
| **PM-9** | Probes, rolling updates, memory limits, drain; final write-up |
| Stage 2 | GPU-aware scheduling and CUDA workloads, on the seams above (deferred) |

## Layout

```
src/chronos/
  api/          Pydantic models, the resource map and quantity parsing
  core/         pure logic: schedule, reconcile, node_health, agent_sync
  store/        versioned store, validation, errors, heartbeat leases
  server/       FastAPI app and the chronosd entry point
  controllers/  deployment, scheduler, node lifecycle (polling loops)
  client/       StoreClient protocol, local and HTTP clients
  agent/        chronos-agent, the process runtime, resource providers
  shim/         chronos-shim: per-pod wrapper that enforces fencing
  cli/          chronosctl
  testing/      test workload, real-cluster fixture, kill_node()
docs/adr/       architecture decision records
docs/devlog/    what was built each day, and what broke
```
