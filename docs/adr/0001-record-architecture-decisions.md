# ADR-0001: Record architecture decisions

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P0 | — | — |

## 1. What
Every significant design decision in Chronos is recorded as an Architecture Decision Record in `docs/adr/`, using the eight-section template ([template.md](template.md)): What, Why, Alternatives, How, Consequences, Verification, Revisit when, References.

## 2. Why
### Problem
Chronos is a portfolio systems project. Its value is in the reasoning behind it as much as the code: why level-triggered, why fence on the agent, why fsync before ack. Six weeks in, that reasoning has evaporated, and "why did you do X?" in an interview gets a vague answer. Design changes made without a record also leave the docs and code inconsistent.
### Forces & constraints
- Interviewers probe *judgment*: alternatives, tradeoffs, failure modes.
- Solo project: no reviewer to force decisions to be explicit.
- Limited time: the process must be lightweight.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: ADRs, one decision per file** | Linkable, diffable, show history via supersession | Some writing overhead | ✅ Chosen |
| A. One big DESIGN.md | Easy to start | Rots, rewritten in place, hard to link to one decision | ❌ |
| B. Commit/PR messages only | Zero extra files | No alternatives captured, not browsable | ❌ |
| C. No formal records | No overhead | Throws away the main evidence of judgment | ❌ |

**A** loses because a single design doc gets edited in place, so the history of *why it changed* disappears. **B** captures *what* changed at a moment but rarely the alternatives, and nobody browses commit logs. **C** defeats the purpose of a portfolio project.

## 4. How
### Components & files
- `docs/adr/NNNN-kebab-title.md`: one decision per file, 4-digit zero-padded number, never reused.
- `docs/adr/README.md`: index table (number, title, status, phase).
- `docs/adr/template.md`: the required structure.
### Mechanism (lifecycle)
```
draft (Proposed) ──implemented & confirmed──▶ Accepted
      │                                         │
      └─ designed, not scheduled ─▶ Deferred    └─ decision changes ─▶ Superseded by ADR-MMMM
                                                                       (new ADR says "Supersedes NNNN")
```
1. Write the ADR as **Proposed** *before* the phase that implements it.
2. In the PR that implements it, flip it to **Accepted** and fix any detail the implementation changed.
3. Once Accepted, it is **immutable**, except for typos and links. A changed decision means a new ADR.
4. Update the index in the same commit.
### Configuration
None. Rules live in [DOCUMENTATION.md §3](../DOCUMENTATION.md#3-adr-process).
### Edge cases
- Decision made mid-implementation without an ADR: write it retroactively in the same PR, marked Accepted.
- Small decision (library choice with no tradeoff): no ADR. Use a code comment.

## 5. Consequences
**Positive:** every significant choice can be explained in under a minute with a link; the Verification section forces each decision to be testable; the supersession chain shows how the design evolved.
**Negative / accepted costs:** ~30–60 min per ADR.
**Risks & mitigations:** statuses go stale → the per-phase Definition of Done checks them, and P10.7 does a full sweep.

## 6. Verification
- Phase DoD (PLAN.md §5) item 3: the ADRs touched by the phase have accurate statuses.
- P10.7: every ADR's How section matches the code; every Verification section links to tests that exist.

## 7. Revisit when
The ADR count exceeds ~40 or the overhead visibly slows delivery. Then consider lighter "decision notes" for minor choices.

## 8. Related & references
- Michael Nygard, "Documenting Architecture Decisions" (2011)
- MADR, adr.github.io/madr
