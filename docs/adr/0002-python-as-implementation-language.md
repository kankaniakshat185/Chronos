# ADR-0002: Python 3.12 as the implementation language

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P0 | — | — |

## 1. What
All of Chronos (control plane, node agent, CLI, simulator, test harnesses) is written in **Python 3.12**, fully type-annotated and checked with `mypy --strict`.

## 2. Why
### Problem
The language decides how much of the 140–180 h budget goes into distributed-systems correctness and how much into fighting the toolchain. Learning a language while also getting fencing, CAS retries and crash recovery right for the first time multiplies the risk of a half-finished project, and a half-finished orchestrator is worth very little.
### Forces & constraints
- Author: fluent in Python and FastAPI; no Go.
- What's evaluated: reconciliation, failure handling, verification rigor. Not throughput or language idiom.
- Chronos is built from scratch and does **not** integrate with the Go-native Kubernetes ecosystem, which removes Go's main practical advantage.
- The testing strategy relies on property-based testing, where Python's Hypothesis is best-in-class.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: Python 3.12** | Fluency, Hypothesis, asyncio, fast iteration | GIL, slower; dynamic typing | ✅ Chosen |
| A. Go, learned while building | Idiomatic for orchestrators; goroutines; resume signal | +3–4 weeks learning; high risk to rigor and completion | ❌ |
| B. Rust | Performance, safety | Steeper still; slower iteration | ❌ |
| C. Python core + Go agent | Some Go exposure | Two toolchains, duplicated API types, no new insight | ❌ |

**A (Go)** is the most tempting. But the project's differentiator is depth of verification, and spending the first month learning goroutine and channel idioms comes straight out of that. Go is better learned on a separate, small exercise. **B** has the same problem, only worse. **C** adds integration cost (two schema definitions, two build systems) without demonstrating anything new.

## 4. How
### Components & files
- `pyproject.toml`: `requires-python = ">=3.12,<3.13"`.
- `src/chronos/`: package (src layout).
### Mechanism
- **Typing:** every function is annotated; `NewType` for domain scalars (`Revision`, `Millicores`, `Bytes`); `enum.StrEnum` for phases and conditions; `typing.Protocol` for seams (`Clock`, `StoreClient`, `Runtime`).
- **Python 3.12 features used:** `type` aliases, `TaskGroup` for structured concurrency in the shell, `asyncio.timeout()`, improved error messages.
- **Validation at every boundary:** Pydantic v2 models for HTTP bodies, manifests and persisted records.
- **Performance hygiene:** keep hot paths simple; profile with `py-spy` before optimizing anything.
### Configuration
Python version pinned by `uv python pin 3.12`.
### Edge cases
- If a CPU-bound hot spot appears (scheduler at 1 000 nodes, WAL encode), first fix the algorithm, then use the stdlib C implementations (`json`, `zlib`). Rewriting in another language is out of scope.

## 5. Consequences
**Positive:** effort goes into correctness and testing; Hypothesis stateful testing is available; fast iteration; readable for reviewers.
**Negative / accepted costs:** lower absolute throughput. Benchmarks report numbers with this context, and the claims are about *behaviour*, not raw speed.
**Risks & mitigations:** dynamic-typing bugs → mypy strict + Pydantic; asyncio pitfalls (blocking calls in the loop) → ruff `ASYNC` rules, and the pure core has no asyncio at all (ADR-0006).

## 6. Verification
- Tier 1 is complete within the budget (PLAN.md §6 milestones hit).
- B4: scheduler handles 1 000 simulated nodes with acceptable per-decision latency (target < 10 ms p99).
- B5: write throughput is fsync-bound, not Python-bound (encode time ≪ fsync time).

## 7. Revisit when
A benchmark shows Python itself (not the algorithm) is the bottleneck at the stated scale, or a future version of the project integrates with real Kubernetes APIs (then Go becomes right).

## 8. Related & references
ADR-0003 (tooling), ADR-0006 (pure core makes testing strong).
