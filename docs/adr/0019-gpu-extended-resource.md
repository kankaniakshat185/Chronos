# ADR-0019: GPUs as an integer, non-shareable, hard-constrained resource with per-node slot assignment

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Deferred – Stage 2 (after Stage 1 exit; hardware lined up) | 2026-10-23 | Stage 2 (P9) | — (extends ADR-0014, ADR-0013) | — |

> **Deferred to Stage 2 (GPU/CUDA)** on 2026-11-04: the plan was split into Stage 1 (Orchestration) and Stage 2 (GPU/CUDA). This decision is kept unchanged and is not scheduled. It plugs into the Stage 1 seams in [ADR-0023](0023-extension-seams.md); see [PHASE2-EXTENSION-POINTS.md](../PHASE2-EXTENSION-POINTS.md).

## 1. What
`Resources` gains a third dimension, **`gpu: int`**, which is **non-fractional and non-shareable**. Nodes declare `capacity.gpu` (0 for CPU-only nodes). The scheduler treats GPU fit as a **hard filter**, with **no fallback** to nodes without free GPUs. On the node, the agent assigns **specific GPU slots** to each pod and exposes them via `CUDA_VISIBLE_DEVICES`.

## 2. Why
### Problem
Real clusters are heterogeneous: GPUs are scarce, expensive and only on some nodes. Without GPU awareness, three things go wrong:
1. A CUDA job lands on a CPU-only node and fails (or, worse, a "helpful" fallback places it there when GPU nodes are full).
2. Two jobs both get "the" GPU on a 1-GPU node and contend for it or OOM it.
3. CPU-only work fills up GPU nodes' CPU/memory, so GPU jobs can't be placed even though GPUs sit idle ("stranding").
### Forces & constraints
- Must match the Kubernetes device-plugin model closely enough to discuss in interviews: extended resources are integers, can't be overcommitted, and requests must equal limits.
- Must be testable without a GPU (CI, macOS) and validated with a real GPU at the end.
- Must reuse the existing resource accounting (ADR-0014), so "used" can never drift from reality.
- Scope guardrails: no MIG, time-slicing, fractional GPUs or driver simulation.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: `gpu: int` field + hard filter + agent slot assignment** | Matches K8s semantics; tiny change; exclusivity enforced on both scheduler and node | GPUs are counted, not health-checked | ✅ Chosen |
| A. GPU as a score/preference | Pods still run when GPUs are full | Silently runs CUDA jobs on CPU nodes, which is wrong by definition | ❌ |
| B. Generic extended-resource map (`{"nvidia.com/gpu": 1, ...}`) | Future-proof | YAGNI: one extra resource doesn't justify a generic map + validation layer | ⏸ Revisit if a 2nd device type appears |
| C. Fractional/shared GPUs (millis like CPU) | Higher utilisation | Needs MPS/MIG/time-slicing to mean anything; explicitly out of scope | ❌ |
| D. Count-only, no per-node slot assignment | Less agent code | Scheduler counts can't stop two local pods using device 0; exclusivity unenforced on the node | ❌ |
| E. Stored `gpu_used` counter on the Node | Matches the spec's field list literally | A second source of truth that can drift from the bound pods | ❌ (exposed as a *derived* `allocated.gpu`) |

**A** is the trap the spec warns about: "fallback" placement turns a scheduling failure into a runtime failure. **B** is the right shape eventually, but over-engineered for one device type. **C** is enterprise complexity outside the exercise. **D** leaves the node-level half of "one task per GPU" unenforced; K8s solves this with device IDs from the device plugin, and slots are the equivalent here. **E**: a stored counter would need updating on every bind, eviction and deletion, and any missed path silently corrupts it. Deriving it from bound pods (as for CPU/memory) makes drift impossible.

## 4. How
### Components & files
- `src/chronos/api/resources.py`: `Resources(cpu: Millicores, memory: Bytes, gpu: int = 0)`.
- `src/chronos/api/quantity.py`: `parse_gpu`, which accepts non-negative integers only.
- `src/chronos/core/schedule.py`: `FitsResources` (now 3-D), new `GpuAvoidance` score plugin.
- `src/chronos/store/store.py`: bind re-check includes gpu.
- `src/chronos/agent/gpu_slots.py`: slot allocator.
### Mechanism
**Spec mapping** (from the extension spec):
| Spec field | Chronos |
|------------|---------|
| `cpu_capacity / memory_capacity / gpu_capacity` | `node.spec.capacity.{cpu, memory, gpu}` |
| `cpu_used / memory_used / gpu_used` | `node.status.allocated.*` = **derived** Σ requests of active pods bound to the node |
| `gpu_request` | `template.resources.requests.gpu` (default 0) |

**Filter** (your spec's pseudocode, as a plugin):
```python
def fits_resources(pod: Pod, n: NodeInfo) -> str | None:
    free = n.capacity - n.allocated - n.assumed          # component-wise Resources
    req = pod.requests
    if req.cpu    > free.cpu:    return "Insufficient cpu"
    if req.memory > free.memory: return "Insufficient memory"
    if req.gpu    > free.gpu:    return "Insufficient gpu"   # capacity.gpu == 0 ⇒ always rejects gpu ≥ 1
    return None
```
There is no relaxation path: if every node is filtered out, the result is `Unschedulable`, full stop.

**GpuAvoidance score** (preference, for `requests.gpu == 0` only):
```python
score = 0 if node.capacity.gpu > 0 else 100      # weight configurable; 0 disables
```
**Validation:** `requests.gpu` integer ≥ 0; `"0.5"`, `"500m"` → error "gpu must be a whole number"; if `limits.gpu` is set it must equal `requests.gpu`.

**Agent slot assignment:**
```python
class GpuSlots:                           # slots 0..capacity-1 → physical ids (default identity)
    def allocate(self, pod_uid, n) -> list[int]: ...   # lowest free slots; raises NoFreeSlot (S10 breach)
    def release(self, pod_uid) -> None: ...            # on shim exit (any reason)
    def rebuild(self, running: dict[uid, list[int]]): ...  # after agent restart (normally empty: orphans killed)
```
On `StartProcess`: `slots = allocate(uid, req.gpu)`; env `CUDA_VISIBLE_DEVICES=",".join(phys[s] for s in slots)`, `CHRONOS_GPU_SLOTS=...`; `status.gpu_devices = slots`. A pod with `gpu == 0` gets `CUDA_VISIBLE_DEVICES=""` so it **can't** grab a GPU by accident.
### Configuration
| Param | Default | Notes |
|-------|---------|-------|
| agent `--gpu` | 0 | logical slot count |
| agent `--gpu-devices` | `0..gpu-1` | physical index per slot; may repeat (one physical GPU shared by several logical slots in local demos; documented) |
| `scheduler.gpu_avoidance_weight` | 1 | 0 disables |
### Edge cases & failure handling
- **Node with GPUs goes NotReady:** standard eviction (ADR-0011); its GPU pods are replaced and filtered onto other GPU nodes only.
- **Agent restarts:** orphan cleanup kills old shims, so the slot table starts empty and is rebuilt from the pods it then starts.
- **Slot allocation fails despite the scheduler's check** (should be impossible): pod reported `Failed/GpuSlotUnavailable`, loud log, S10 checker flags it.
- **`capacity.gpu` lowered while pods are bound:** existing pods keep running; new GPU pods are filtered against the new capacity.

## 5. Consequences
**Positive:** GPU placement is correct by construction (S9) and enforced twice (filter + bind); node-level exclusivity (S10); a heterogeneous cluster makes fragmentation measurable (B7); a near-zero-cost extension, which itself shows the resource model was designed well.
**Negative / accepted costs:** GPUs are counted, not monitored (no health checks or ECC errors); logical slots mapped to one physical GPU share it physically in local demos.
**Risks & mitigations:** a demo misread as "real isolation" → GPU.md and the README say clearly that exclusivity is at the scheduling/slot level.

## 6. Verification
- **S9:** Hypothesis property over random heterogeneous clusters; store bind rejects GPU overcommit; canary `gpu_soft_constraint` detected by the sim; integration + real-GPU "no eligible node → Unschedulable" tests.
- **S10:** slot allocator unit/property tests; canary `agent_reuse_gpu_slot`; integration checks `status.gpu_devices` disjointness per node.
- **B7:** stranding with vs without `GpuAvoidance`.

## 7. Revisit when
A second device type is needed (move to an extended-resource map, alternative B), or GPU sharing becomes a goal (that would be a new ADR on MPS/time-slicing, out of current scope).

## 8. Related & references
ADR-0013, ADR-0014, ADR-0020, ADR-0021; Kubernetes "Schedule GPUs" and the device plugin framework (extended resources, integer, no overcommit); NVIDIA k8s-device-plugin.
