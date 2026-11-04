# ADR-0013: Filter/score scheduler with atomic binding

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P3 | — | — |

> **Amended 2026-11-04 (still Proposed, so amended in place):** generalized for the Stage 1 extension seams, [ADR-0023](0023-extension-seams.md) X-1/X-2/X-3: fit and bind iterate over resource names, and plugins register by name.

## 1. What
A single-threaded scheduler places one pending pod at a time: **filter** plugins remove infeasible nodes, **score** plugins rank the rest, and ties break deterministically by node name. It keeps an **assumed-allocation** overlay for its own in-flight decisions, and it binds through a **store transaction that re-checks capacity for every resource name**, so overcommit is impossible even with a stale cache. Filter and score plugins register by name in a **registry**; config selects and weights them, so a new plugin needs no framework change (X-3).

## 2. Why
### Problem
Pending pods need a node that can fit them, chosen by a sensible policy. The scheduler decides from a cache that lags the store. Concrete failure: node-1 has room for one more 1-CPU pod; the scheduler places pod A there, and before the watch reflects A's binding it places pod B there too, which overcommits the node (S2 violated).
### Forces & constraints
- S2 must hold *always*, not just usually.
- Decisions must be deterministic for sim reproducibility.
- Policy should be swappable (spread vs pack) and extensible (affinity: Backlog P12.1; GPU-aware scoring: Stage 2, deferred) **without changing the framework**.
- Throughput target: ≤ 1 000 nodes, thousands of pods.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: filter/score plugins + assume + transactional bind** | Correct under staleness; deterministic; extensible; K8s vocabulary | One pod at a time | ✅ Chosen |
| A. First-fit / round-robin | Trivial | No policy story; nothing to compare | ❌ (fine as a P3 spike) |
| B. Optimal batch placement (ILP / solver) | Better packing | Latency; dependency; solves a different problem | ❌ |
| C. Optimistic parallel schedulers (Omega) | Throughput | Unneeded at this scale; more conflict handling | ⏸ Write-up note |
| D. Trust the cache, no bind re-check | Simpler store | One stale-cache bug → overcommit | ❌ |

**A** works but leaves nothing to discuss or measure. **B** optimizes globally and fits batch systems, not online placement. **C** is enabled by the CAS bind but isn't needed yet. **D** makes S2 depend on the cache being correct; checking capacity in the store costs one extra sum and removes that dependency (defence in depth).

## 4. How
### Components & files
- `src/chronos/core/schedule.py`: plugin interfaces, the plugin registry, `pick_node`.
- `src/chronos/controllers/scheduler.py`: queue, assume cache, bind loop.
- `src/chronos/store/store.py`: `bind` transaction.
### Mechanism
**Pure core:**
```python
class Filter(Protocol):  def __call__(self, pod: Pod, node: NodeInfo) -> str | None: ...   # reason if rejected
class Score(Protocol):   def __call__(self, pod: Pod, node: NodeInfo) -> int: ...          # 0..100

def pick_node(pod, nodes: list[NodeInfo], filters, scores: list[tuple[Score, int]]) -> Placement:
    feasible, reasons = [], Counter()
    for n in nodes:
        r = next((f(pod, n) for f in filters if f(pod, n)), None)
        (reasons.update([r]) if r else feasible.append(n))
    if not feasible: return Unschedulable(reasons)            # e.g. {"Insufficient cpu": 3}
    return Bind(max(feasible, key=lambda n: (sum(w * s(pod, n) for s, w in scores), invert(n.name))).name)
```
`NodeInfo` = node + `allocated` (from bound non-terminal pods) + `assumed`, each a map `resource name → int` (ADR-0014).

**Plugin registry (X-3):**
```python
FILTERS: dict[str, Filter] = {}; SCORES: dict[str, Score] = {}
def register_filter(name: str, f: Filter) -> None: ...   # duplicate name → error
def register_score(name: str, s: Score) -> None: ...
# config: scheduler.filters = [names]; scheduler.scores = {name: weight}; unknown name → config error
```
Built-in plugins register through the same calls; the framework never imports a plugin by name.
- Filters: `NodeReady`, `NodeSchedulable`, `FitsResources`: for **every resource name r** in the request, `allocated[r] + assumed[r] + req[r] ≤ capacity[r]`, where a name the node doesn't advertise has capacity 0. That makes the extended-resource hard constraint (S11) a consequence of the same check. Reason: `Insufficient <r>`.
- Scores: `LeastAllocated = 100 · (1 − used/cap)` averaged over CPU and memory; `MostAllocated = 100 · used/cap`.

**Shell loop:**
```python
while pod := await pending.get():
    p = core.pick_node(pod, cache.node_infos(), ...)
    if isinstance(p, Unschedulable): mark_unschedulable(pod, p.reasons); continue
    assume(pod, p.node)
    try:   await client.bind(pod.name, p.node, pod.resource_version)
    except (Conflict, InsufficientCapacity, NotFound):
           forget(pod); pending.add_rate_limited(pod.name)
# informer sees the binding → forget(pod) from assumed (now counted in `allocated`)
```
**Store bind transaction:**
```python
def bind(pod_name, node_name, expected_rv):
    pod = get(Pod, pod_name);  check rv == expected_rv;  check pod.spec.node_name == ""
    node = get(Node, node_name); check node.condition == READY and not node.spec.unschedulable
    used = sum_maps(p.requests for p in pods_on(node_name) if p.is_active())
    for r, n in pod.requests.items():
        if used.get(r, 0) + n > node.capacity.get(r, 0): raise InsufficientCapacity(r)
    pod.spec.node_name = node_name; pod.status.phase = BOUND; commit
```
`pods_on(node)` uses a secondary index maintained by the store.
**Unschedulable set:** pods with no feasible node are parked; any Node ADDED/MODIFIED event, or a pod deletion that frees capacity, moves them back to `pending`, and so does a 30 s backoff.
### Configuration
| Param | Default |
|-------|---------|
| `scheduler.policy` | `spread` (LeastAllocated) \| `pack` (MostAllocated) |
| `scheduler.filters` / `scheduler.scores` | registered plugin names / name → weight (X-3) |
| `scheduler.unschedulable_retry` | 30 s |
| `scheduler.assume_ttl` | 30 s (safety net if the bind result is never observed) |
### Edge cases & failure handling
- **Node becomes NotReady after assume, before bind:** the store's Ready check rejects → requeue.
- **Pod deleted while pending:** bind → NotFound → drop.
- **Scheduler restart:** the assume cache is empty, the informer relists, and pending pods are re-enqueued. Nothing is lost.

## 5. Consequences
**Positive:** S2 is enforced in two places; deterministic placements; extensible; maps onto the kube-scheduler framework.
**Negative / accepted costs:** one decision at a time; greedy placement can fragment (shown in BENCHMARKS with an example).
**Risks & mitigations:** an O(nodes) scan per pod → B4 measures it; optimise (pre-sorted candidate lists) only if needed.

## 6. Verification
- **S2:** property test over random clusters/pod sequences and random resource-name sets (≥ 10 k examples); the store bind rejects overcommit even with an adversarial cache.
- **S11:** pods requesting an extended resource never bind to a node without enough free units; infeasible → Unschedulable; canary `extended_resource_soft_constraint` detected by the sim.
- **Registry (X-3):** a plugin registered from test code changes placement with no framework change (PX.9).
- **Determinism:** same input → same placement (property).
- **L3:** a single pod that fits somewhere is never Unschedulable.
- Canary `skip_bind_capacity_check` + an artificially stale cache → S2 violation found by the sim.
- **B4:** throughput and p99 decision latency vs node count (10 → 1 000).

## 7. Revisit when
B4 shows scheduling throughput limits B1 convergence, or Backlog P12.1 (affinity/taints) needs pre-filtering.

## 8. Related & references
ADR-0007, ADR-0014, ADR-0023; Kubernetes scheduling framework (filter/score/bind, assume cache); Schwarzkopf et al., "Omega" (EuroSys 2013); Verma et al., "Borg" (EuroSys 2015).
