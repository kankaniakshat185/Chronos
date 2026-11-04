# ADR-NNNN: <short imperative title>

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed \| Accepted \| Deprecated \| Superseded \| Deferred | YYYY-MM-DD | PN | — | — |

## 1. What
The decision in 1–3 sentences, precise enough that someone could tell whether the code follows it.

## 2. Why
### Problem
What goes wrong (or can't be done) without a decision here? A concrete failure scenario is best.
### Forces & constraints
The things pulling in different directions: correctness, time budget, platform, what the project is trying to demonstrate, the author's skills.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: …** | | | ✅ Chosen |
| A … | | | ❌ Rejected / ⏸ Deferred |

One short paragraph per rejected option on *why* it lost. That's the argument an interviewer wants to hear.

## 4. How
### Components & files
Which modules own it (`src/chronos/...`).
### Mechanism
Data structures, algorithms, protocol steps. Pseudocode where it helps.
### Configuration
Parameters, defaults, constraints between them.
### Edge cases & failure handling
What happens on crash, timeout, conflict, restart.

## 5. Consequences
**Positive:** …
**Negative / accepted costs:** …
**Risks & mitigations:** …

## 6. Verification
Invariant IDs (TESTING.md §2), tests by layer, canaries, benchmarks that prove this decision holds.

## 7. Revisit when
Concrete signals that should reopen this decision.

## 8. Related & references
ADRs, docs, papers, talks.
