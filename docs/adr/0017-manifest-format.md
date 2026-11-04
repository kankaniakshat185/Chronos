# ADR-0017: Own YAML manifest schema validated by Pydantic

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P1 | — | — |

> **Amended 2026-11-04 (still Proposed, so amended in place):** generalized for the Stage 1 extension seams, [ADR-0023](0023-extension-seams.md) X-1/X-2/X-4/X-6: arbitrary resource names, a `workload_type` field, and kind dispatch through the kind registry.

## 1. What
Users declare desired state in a small, **Kubernetes-flavoured but not Kubernetes-compatible** YAML format, parsed with `yaml.safe_load` and validated by the **same Pydantic models the API uses**, with unknown fields rejected. `chronosctl apply` creates or CAS-replaces the full spec.

## 2. Why
### Problem
There has to be a way to say "3 replicas of X with these resources". Copying the Kubernetes API means a huge surface (apiVersion dispatch, strategic merge patch, dozens of fields) that teaches nothing about the mechanisms Chronos is about, and invites "why doesn't X work?" comparisons. A sloppy schema, on the other hand, lets typos silently become defaults (`replica: 3` → default 1 replica).
### Forces & constraints
- Familiar to anyone who knows K8s.
- One schema source (no drift between CLI, API, storage).
- Typos must fail loudly.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: own small YAML schema, Pydantic, `extra="forbid"`** | Familiar, small, one source, loud errors | Not K8s-compatible | ✅ Chosen |
| A. K8s-compatible YAML | Ecosystem familiarity | Huge surface; non-goal | ❌ |
| B. JSON only | No YAML parser quirks | Unpleasant to hand-write | ❌ |
| C. TOML | Strict, simple | Awkward nesting for commands/probes; unconventional here | ❌ |
| D. Server-side merge/patch on apply | Preserves fields set by others | Complex; spec is single-writer (users) anyway | ❌ |

**A** is a non-goal (PLAN.md §1). **B/C** lose on ergonomics and convention. **D** solves a problem Chronos doesn't have, because only users write `spec`, and status is separate (ADR-0007).

## 4. How
### Components & files
- `src/chronos/api/deployment.py`: `DeploymentSpec`, `PodTemplate`, `Probe`, `Strategy`.
- `src/chronos/cli/apply.py`: load → validate → create-or-update.
- `examples/*.yaml`.
### Mechanism
**Schema example:**
```yaml
kind: Deployment
name: web
labels: {app: web}
spec:
  replicas: 3
  template:
    workload_type: generic             # default; must be registered (X-4)
    command: ["python", "-m", "http.server", "${PORT}"]
    env: {GREETING: hello}
    resources:
      requests: {cpu: 250m, memory: 64Mi}           # plus any extended resource, e.g. chronos.dev/slot: 1
      limits:   {memory: 128Mi}
    restart_policy: Always            # Always | OnFailure | Never
    readiness_probe: {http: {path: /, port_env: PORT}, period_s: 1}     # Tier 2
    liveness_probe:  {exec: ["true"], period_s: 5, failure_threshold: 3} # Tier 2
  strategy: {max_surge: 1, max_unavailable: 0}                           # Tier 2
```
**Apply algorithm:**
```python
for doc in yaml.safe_load_all(f):
    obj = parse_obj(doc)                          # Pydantic, extra="forbid" → error with field path
    try:   client.create(obj)
    except AlreadyExists:
        retry_on_conflict(lambda: client.update(replace_spec(client.get(obj.kind, obj.name), obj)))
```
**`${PORT}`:** if the template references `${PORT}`, the agent allocates a free port per pod, sets `PORT` in env and substitutes it into `command`. That lets multiple HTTP workloads share a host.
**Validation rules:** extended-resource rules from ADR-0014 (integer-only, `limits == requests`); `workload_type` must be registered (X-4) and is validated by its handler; `kind` must be registered (X-6); `replicas ≥ 0`; `command` non-empty; `limits.memory ≥ requests.memory`; probe periods > 0; `max_surge + max_unavailable > 0`; names are DNS-label-like (`[a-z0-9-]{1,63}`).
### Configuration
None.
### Edge cases & failure handling
- Multi-document files: applied in order; the first error stops and reports which document failed.
- `kind` missing or unknown → error listing the kinds in the kind registry (X-6).
- YAML tags (`!!python/...`): rejected by `safe_load`.

## 5. Consequences
**Positive:** familiar, small, one source of truth, precise error messages.
**Negative / accepted costs:** not K8s-compatible (by design); full-spec replace on apply.
**Risks & mitigations:** schema evolution later → there's an implicit version (v1); add `apiVersion` only if a breaking change is ever needed.

## 6. Verification
- Unit: every file in `examples/` parses; typo fields are rejected with a path (`spec.replica: extra fields not permitted`); every validation rule.
- Integration: `chronosctl apply` golden-output tests (create, update, no-op, conflict retry).

## 7. Revisit when
A breaking schema change is needed (introduce `apiVersion`), or another writer of `spec` appears (consider patch semantics).

## 8. Related & references
ADR-0007, ADR-0014, ADR-0023; PyYAML `safe_load`; Pydantic v2 `ConfigDict(extra="forbid")`.
