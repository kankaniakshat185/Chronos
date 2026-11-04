# Architecture Decision Records

Process: see [../DOCUMENTATION.md §3](../DOCUMENTATION.md#3-adr-process). Template: [template.md](template.md).

Every ADR follows the same eight sections: **What · Why · Alternatives considered · How · Consequences · Verification · Revisit when · References**.

**Stage 2 (GPU/CUDA)** ADRs 0019–0021 are **Deferred – Stage 2**: kept unchanged and not scheduled until Stage 1 exits and hardware is lined up. ADR-0018 is in the Backlog.

All ADRs start as **Proposed** (drafted during planning, 2026-10-21). They become **Accepted** in the PR that implements them.

| # | Title | Status | Phase |
|---|-------|--------|-------|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted | P0 |
| [0002](0002-python-as-implementation-language.md) | Python 3.12 as the implementation language | Accepted | P0 |
| [0003](0003-tooling.md) | Tooling: uv, ruff, mypy strict, pytest + Hypothesis | Proposed | P0 |
| [0004](0004-http-json-transport.md) | HTTP/JSON (FastAPI) transport; gRPC deferred | Proposed | P1 |
| [0005](0005-workloads-are-os-processes.md) | Workloads are OS processes supervised by node agents | Proposed | P2 |
| [0006](0006-functional-core-imperative-shell.md) | Functional core, imperative shell, injected clock | Accepted | P0 |
| [0007](0007-versioned-single-writer-store.md) | Versioned single-writer store with optimistic concurrency | Proposed | P1 |
| [0008](0008-watch-via-long-polling.md) | Watch via long-polling on resource versions | Proposed | P1 |
| [0009](0009-level-triggered-reconciliation.md) | Level-triggered reconciliation with informers, work queue, resync | Proposed | P4 |
| [0010](0010-pull-based-agents.md) | Pull-based node agents | Proposed | P2 |
| [0011](0011-heartbeat-leases-failure-detection.md) | Heartbeat leases and a node condition state machine | Proposed | P2 |
| [0012](0012-agent-self-fencing.md) | Self-fencing (agent + per-pod shim) to bound duplicates during partitions | Proposed | P4 |
| [0013](0013-filter-score-scheduler.md) | Filter/score scheduler with atomic binding | Proposed (amended 2026-11-04) | P3, MVP |
| [0014](0014-resource-model.md) | Resource model: an integer resource map (millicores, bytes, extended units), request-based | Proposed (amended 2026-11-04) | P0/P3, MVP |
| [0015](0015-wal-and-snapshots.md) | Persistence via WAL with CRC framing and snapshots | Proposed | P6 |
| [0016](0016-deterministic-simulation-testing.md) | Deterministic simulation testing | Proposed | P5 |
| [0017](0017-manifest-format.md) | Own YAML manifest schema validated by Pydantic | Proposed (amended 2026-11-04) | P1 |
| [0018](0018-control-plane-high-availability.md) | Control-plane high availability | Deferred (Backlog, not scheduled) | Backlog |
| [0019](0019-gpu-extended-resource.md) | GPUs as an integer, non-shareable, hard-constrained resource with slot assignment | Deferred – Stage 2 | Stage 2 (P9) |
| [0020](0020-run-to-completion-jobs.md) | Run-to-completion `Job` kind | Deferred – Stage 2 | Stage 2 (P9) |
| [0021](0021-cuda-workload-toolchain.md) | CUDA workloads via numba-cuda, with a simulate mode | Deferred – Stage 2 | Stage 2 (P9) |
| [0022](0022-week-1-mvp-scope.md) | MVP scope (formerly "Week-1 MVP"): temporary shortcuts and their pay-down plan | Proposed (revised 2026-11-04) | Stage 1, Part A |
| [0023](0023-extension-seams.md) | Extension seams: resource map, extended resources and small registries | Proposed | Stage 1 |
| [0024](0024-prometheus-grafana-observability.md) | Prometheus + Grafana for metrics and dashboards (required) | Proposed | Stage 1, PM-5 |
