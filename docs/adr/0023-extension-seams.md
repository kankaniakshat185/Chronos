# ADR-0023: Extension seams: resource map, extended resources and small registries

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Accepted | 2026-11-04 | Stage 1 (MVP, PM-1, PM-3, PM-4, PM-6, PM-9b) | — | — |

> **Accepted at Stage 1 exit (2026-11-09).** X-1–X-8 exist as described and X-9 is green (PM-9b): test-only extensions pass the scheduling and bind properties, run a real process, and hold every invariant in the simulator. Writing the proof found one real bug, now fixed: the deployment controller garbage-collected pods owned by any other kind, which would have broken Stage 2's Jobs.

## 1. What
Stage 1 has eight small **extension seams** (X-1–X-8), each a plain protocol plus a module-level registry: a resource map, the extended-resource rule, and registries for scheduler plugins, workload types, resource providers, and kinds/controllers. Together with generic invariants and an optional-dependency guard, they let Stage 2 (GPU/CUDA, deferred) be added as **new code behind existing extension points, with no rewrites of Stage 1 code**. An **in-process proof-of-modularity test** (X-9) shows it. There is **no plugin loader**: X-10 was dropped as a conscious limit.

## 2. Why
### Problem
The original plan wove GPU into Stage 1: `Resources(cpu, memory, gpu)`, a GPU-specific filter, GPU flags, GPU demo steps. Removing GPU from Stage 1 without seams would mean that adding it later touches the models, scheduler, store bind, agent supervisor, controller runtime and test generators: a rewrite. Concrete failure: Stage 2 adds `gpu` as a fourth fixed field, forgets one of the three places capacity is summed, and S2 silently stops covering GPUs.
### Forces & constraints
- Stage 1 must contain **no GPU code** and no `gpu` literal; GPU is Stage 2 (deferred).
- **Very scoped:** seams must not grow into a plugin framework; each should be a few dozen lines.
- The proof must be a test, not a claim.
- Kubernetes already has the right model for this: extended resources and the device-plugin split between scheduler counting and node-level assignment.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: resource map + extended-resource rule + 5 small registries + in-process proof** | GPU (Stage 2, deferred) and others become instantiations; no rewrites; tiny | Some indirection | ✅ Chosen |
| A. No seams; refactor when Stage 2 starts | Zero Stage 1 cost | Stage 2 becomes a rewrite across 6+ modules; regressions in Stage 1 invariants | ❌ |
| B. Full plugin framework (entry points, dynamic loading, versioned plugin API) | Out-of-tree extensions | Far beyond scope; more code than the features it serves | ❌ |
| C. Seams + plugin loader + integration-level proof (X-10) | Proves out-of-process extension | Loader + subprocess plumbing for a need that doesn't exist yet | ❌ (dropped; revisit at Stage 2 entry) |
| D. Keep GPU in Stage 1 as before | No new seams | Contradicts the Stage 1/Stage 2 split | ❌ |

**A** postpones the cost and multiplies it. **B** is over-engineering for one future extension. **C** was proposed and then deliberately dropped: an integration-level proof needs a loader so test code can reach separate `chronosd`/agent processes, and that's production code with no Stage 1 user. **D** is the structure this ADR replaces.

## 4. How
### Components & files
| Seam | Files | Lands in |
|------|-------|----------|
| X-1 resource map | `api/resources.py`, `core/schedule.py` (`FitsResources`), `store/store.py` (bind) | MVP |
| X-2 extended-resource rule | `api/resources.py`, `api/quantity.py` | MVP |
| X-3 scheduler plugin registry | `core/schedule.py` | MVP |
| X-4 WorkloadType registry | `agent/workload_types.py` | PM-1 |
| X-5 resource-provider hook | `agent/providers.py` | PM-1 |
| X-6 kind + controller registry | `api/kinds.py`, `controllers/runtime.py` | PM-3 |
| X-7 generic invariants | `testing/invariants.py`, `sim/faults.py` | PM-4 / PM-6 |
| X-8 optional-dependency guard | `tests/unit/test_architecture.py` | PM-1 |
| X-9 proof of modularity | `tests/extensions.py` (the test-only extensions); `tests/property/test_modularity_widget.py`, `tests/integration/test_seams.py`, `tests/sim/test_seam_proof.py` | early in MVP; final in PM-9b |
### Mechanism
Every registry has the same shape, and nothing more:
```python
_REGISTRY: dict[str, T] = {}
def register(name: str, item: T) -> None:      # duplicate name → ValueError
    if name in _REGISTRY: raise ValueError(name)
    _REGISTRY[name] = item
def get(name: str) -> T: ...                    # unknown name → clear validation/config error
```
Built-ins register through the same call; framework code never imports a specific plugin.

**Contracts:**
```python
Resources = Mapping[str, int]                    # X-1: "cpu" millicores, "memory" bytes, else whole units
def is_extended(name: str) -> bool: return name not in ("cpu", "memory")        # X-2

class WorkloadType(Protocol):                    # X-4: keyed by template.workload_type ("generic")
    def validate(self, template: PodTemplate) -> None: ...
    def prepare(self, pod: Pod) -> tuple[list[str], dict[str, str]]: ...       # argv, extra env

class ResourceProvider(Protocol):                # X-5: keyed by resource name; default = counting provider
    def allocate(self, pod: Pod, n: int) -> list[str]: ...    # on StartProcess; unit ids → pod.status.assigned
    def release(self, pod: Pod) -> None: ...                  # on shim exit
    def env(self, units: list[str]) -> dict[str, str]: ...    # e.g. a GPU provider sets CUDA_VISIBLE_DEVICES (Stage 2)

register_kind(name, model, validators, indexes)  # X-6: used by API routes, store validation, manifest parsing
register_controller(name, factory)               # X-6: used by the controller runtime
```
X-4 is distinct from ADR-0005's `Runtime` protocol: `Runtime` is the execution backend (process); `WorkloadType` prepares what to run.

**Invariants:** S2 is stated per resource name. **S11** (extended-resource hard constraint) and **S12** (provider unit exclusivity) are generic. Stage 2's S9/S10 are their instantiations for `gpu` (≡ `nvidia.com/gpu`; bare names are allowed).

**Proof (X-9), in-process only:** test code registers `example.com/widget` capacity on random nodes, a fake workload type and a fake provider, then runs the scheduling properties, the store-bind properties and (final version) a sim run. Its diff must touch only `tests/`.

**Conscious limit (X-10 dropped):** there's no `CHRONOS_PLUGINS` loader, and no integration-level proof with separate processes. At **Stage 2 entry**, choose one of:
- (a) one import line per entry point (`chronosd`, `chronos-agent`) that registers the (Stage 2, deferred) GPU modules; or
- (b) add the loader then.

Either way, framework and core code don't change.
### Configuration
`scheduler.filters`, `scheduler.scores` (X-3). Agent `--resource <name>=<qty>` (repeatable) declares extended capacity; the counting provider handles any advertised extended resource without a specific provider.
### Edge cases & failure handling
- Duplicate registration → `ValueError` at import time (fail fast).
- Unknown workload type / kind / plugin name → validation (422) or config error at startup.
- A provider raising in `allocate` → the pod is reported `Failed(ProviderError)`; no process starts.
- Agent restart → providers rebuild their state from the pods they restart (orphans were already killed).

### Implementation status (2026-11-05)
Done: X-1, X-2, X-3 (MVP), X-4, X-5, X-8 (PM-1), early X-9. Deviations from the sketch above: the WorkloadType registry is `chronos/api/workload_types.py`, not `agent/`, because templates are validated wherever they're parsed (API 422, `chronosctl`, agent); providers take the pod **uid** (`allocate(uid, n)`, `release(uid)`) because release happens on process exit, when only the uid is at hand; providers are registered as **factories** `(resource, capacity) -> provider`, because a provider needs the node's capacity, which isn't known at import time. Remaining: X-7 (PM-4/PM-6), final X-9 (PM-9b).

**X-6 done (PM-3, 2026-11-06).** `chronos/store/kinds.py` holds the kind registry (`register_kind(KindInfo(model, plural, aliases, validators, indexes))`), not `api/kinds.py`: validators raise the store's `Invalid`, and the store package can't be imported from `api/` without a cycle. Every kind subclasses `api.meta.ApiObject` (kind, metadata, spec, status), so the store, routes, both clients, manifest loading and the CLI are generic over `T: ApiObject`. Indexes are named key functions; the store keeps them and `list(node_name=)` and the watch `node=` filter use the `node` index. The controller registry (`register_controller(name, factory)`) is in `controllers/runtime.py`; `chronosd` builds every registered controller on one shared set of informers. Kind-specific store logic that is about *binding* (Pod node assignment, Node capacity vs allocated) stays in the store: it belongs to the bind transaction, not to a kind plug-in. The Event kind isn't registered yet: it arrives with P7.3 (PM-5). Verified by `tests/integration/test_kind_registry.py`: a `Widget` kind and controller defined only in test code get CRUD, status, watch, a store validator, WAL durability, manifest parsing and a reconcile loop.

## 5. Consequences
**Positive:** Stage 2 becomes a checklist (PHASE2-EXTENSION-POINTS.md); GPU tests become instantiations of existing invariants; the seams are small enough to explain in a minute.
**Negative / accepted costs:** extra seam work spread across Stage 1; one more level of indirection in four places; no out-of-tree extensions.
**Risks & mitigations:** seam over-engineering → the registry shape above is the whole design, and anything more needs a new ADR; seams that aren't real → X-9 fails.

## 6. Verification
- X-1: S2 property over random resource-name sets; lint test that `src/` has no `gpu` literal (GPU is Stage 2, deferred).
- X-2: parsing properties; **S11** at property, sim, integration and chaos layers; canary `extended_resource_soft_constraint`.
- X-3: a test-registered plugin changes placement without framework change.
- X-4: unknown type → 422; a fake type runs through the unchanged runtime.
- X-5: **S12** property and integration; canary `provider_reuse_unit`.
- X-6: a fake kind gets CRUD + reconcile without framework change.
- X-7: sim/chaos runs with random extended resources.
- X-8: AST import test.
- **X-9:** in-process proof green with a tests-only diff (a Stage 1 exit criterion).

## 7. Revisit when
At Stage 2 entry (the loader decision above); or if any seam needs more than the registry shape (write a new ADR first).

## 8. Related & references
ADR-0005, 0006, 0009, 0013, 0014, 0017, 0019–0021 (Stage 2, deferred), 0022; PLAN.md §4.7 and Appendix A Phase X; PHASE2-EXTENSION-POINTS.md; Kubernetes "Extended resources" and the device-plugin framework.
