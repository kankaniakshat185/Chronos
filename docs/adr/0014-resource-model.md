# ADR-0014: Resource model: an integer resource map (millicores, bytes, extended units), request-based

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Accepted | 2026-10-21 (amended 2026-11-04) | P0 (types), P3 (accounting), P10 (limits); X-1/X-2 in the MVP | — | — |

> **Amended 2026-11-04 (still Proposed, so amended in place):** generalized for the Stage 1 extension seams, [ADR-0023](0023-extension-seams.md) X-1/X-2: resources are a map, and any name other than `cpu`/`memory` is an extended resource.

> **Accepted at Stage 1 exit (2026-11-09).** Memory limits are enforced as described (`ProcessRuntime.enforce_memory`, every 0.5 s, OOMKilled). The warning when `limits.cpu` is set was **not built**; the README's limitations section is the only notice that CPU limits aren't enforced.

## 1. What
Resources are a **map `resource name → non-negative int`**. CPU is the entry `cpu` in integer **millicores**, memory is `memory` in integer **bytes**, and **any other name is an extended resource** counted in whole units: integer-only, non-fractional, requests == limits, never overcommitted, and a node that doesn't advertise it has capacity 0. There are no floats anywhere in accounting, and no resource name is hard-coded beyond `cpu` and `memory`. The scheduler reserves **requests** against a node's **declared** capacity. **Memory limits** (Tier 2) are enforced by RSS polling and OOM-kill; **CPU limits are not enforced**, and that is documented.

## 2. Why
### Problem
Scheduling is arithmetic: if `0.1 + 0.2 != 0.3` creeps into capacity checks, S2 tests flake and edge-case pods are accepted or rejected wrongly. Separately, workloads are unisolated processes (ADR-0005), so claiming "limits" without saying how they're enforced would be dishonest.
### Forces & constraints
- Exact sums over hundreds of pods.
- Familiar notation in manifests (`250m`, `64Mi`).
- Several agents share one host, so capacity can't come from the host.
- No cgroups (out of scope), and it must work on macOS.
- Later resources (e.g. GPUs in Stage 2, deferred) must plug in **without rewriting** the model, the scheduler or the store (ADR-0023).

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: integer millicores/bytes; requests reserved; RSS-polled memory limits** | Exact; familiar; honest | Memory enforcement reactive; CPU advisory | ✅ Chosen |
| A. Float CPU/GB | Simple | Rounding drift; flaky equality | ❌ |
| B. `setrlimit(RLIMIT_AS)` for memory | Kernel-enforced | Not enforced on macOS; limits *virtual* memory on Linux (breaks many runtimes) | ❌ |
| C. cgroups v2 | Real enforcement (CPU + mem) | Linux-only, privileged; it's the skipped container-runtime scope | ❌ |
| D. Capacity auto-detected from the host | No flags | Wrong when N agents share one host | ❌ |
| E. Fixed fields (`cpu`, `memory`, then `gpu` (Stage 2, deferred), …) | Simple types | Every new resource rewrites models, fit, bind and tests | ❌ (was the pre-2026-11-04 shape) |
| F. Require `vendor/name` for extended resources (K8s rule) | K8s-faithful | Not needed for correctness; bare names keep Stage 2 specs (`gpu`) unchanged | ❌ (bare names allowed; `gpu` ≡ `nvidia.com/gpu`) |

**A** fails S2 property tests at boundaries. **B** doesn't do what its name suggests. **C** crosses the non-goal line. **D** would let each of 5 local agents claim the whole machine. **E** makes every new resource a rewrite. **F** adds a rule that buys nothing here.

## 4. How
### Components & files
- `src/chronos/api/quantity.py`: parse/format.
- `src/chronos/api/resources.py`: `Resources = Mapping[str, int]` helpers (`add`, `sub`, `fits_in`, all over the union of names, missing = 0) and the extended-resource rule.
- `src/chronos/agent/runtime.py`: `enforce_memory`, the RSS monitor (Tier 2).
### Mechanism
**Parsing:**
```
cpu:    "2" → 2000   "0.5" → 500   "250m" → 250             (decimal → Fraction → exact int; reject sub-millicore)
memory: "64Mi" → 67_108_864   "1G" → 1_000_000_000   "512Ki", "1Gi", plain int bytes
extended (any other name, e.g. chronos.dev/slot): "2" → 2, 2 → 2;  "0.5", "500m", "-1" → rejected
names: ([a-z0-9.-]+/)?[a-z0-9-]+   (bare names allowed)
```
`Fraction` is used during parsing so `"0.1"` is exact; results must be whole millicores/bytes, or the value is rejected.
**Accounting:** `node.allocated[r] = Σ pod.requests[r]` over active pods bound to the node (the store's secondary index); `fits = allocated[r] + assumed[r] + req[r] ≤ capacity[r]` for every requested name `r`. **Extended resources:** if `limits[r]` is set it must equal `requests[r]`; they are never scored by `LeastAllocated` (cpu/memory only).
**Memory limits (Tier 2):** every 500 ms the agent sums RSS over the shim's process tree (`psutil.Process(pgid).children(recursive=True)`); if `> limits.memory` → `killpg(SIGKILL)`, `status.reason = "OOMKilled"`, then the restart policy applies.
### Configuration
| Param | Default |
|-------|---------|
| agent `--cpu`, `--mem` | required |
| agent `--resource <name>=<qty>` | repeatable; declares extended-resource capacity |
| `limits.poll_interval` | 500 ms |
| pod without requests | treated as `0/0` (best effort); the examples always set requests |
### Edge cases & failure handling
- **Limit < request:** validation error at apply time.
- **Extended `limits ≠ requests`, fractional or negative extended quantity:** validation error.
- **Request for an extended resource no node advertises:** valid; the pod stays Unschedulable (`Insufficient <r>`).
- **RSS read races with process exit:** `NoSuchProcess` is ignored.
- **Short memory spikes:** may slip through the 500 ms window. Documented as reactive enforcement.

## 5. Consequences
**Positive:** exact arithmetic; K8s-like quantities; an honest enforcement story.
**Negative / accepted costs:** memory enforcement is reactive; CPU is advisory only; best-effort pods can overcommit the real host (but not the declared capacity).
**Risks & mitigations:** users assume CPU limits work → the README limitations section plus a warning when `limits.cpu` is set.

## 6. Verification
- Property: `parse(format(q)) == q`; `Σ` matches a big-int reference; `fits_in` is consistent with component-wise comparison.
- S2 tests use these types throughout, over random resource-name sets.
- **S11:** extended-resource parsing and placement properties (PLAN.md Day 1/5).
- Integration (Tier 2): a `--leak-mem` workload is OOMKilled within ~1 s of crossing its limit; restart_count increments.

## 7. Revisit when
A Linux-only `CgroupRuntime` is ever added (then enforce CPU and memory in the kernel), or a resource needs fractional sharing (out of scope).

## 8. Related & references
ADR-0005, ADR-0013, ADR-0023; ADR-0019 (Stage 2, deferred: GPU as an instance of the extended-resource rule); Kubernetes "Resource Management for Pods and Containers" (quantities, requests vs limits).
