# ADR-0003: Tooling: uv, ruff, mypy strict, pytest + Hypothesis

| Status | Date | Phase | Supersedes | Superseded by |
|--------|------|-------|------------|---------------|
| Proposed | 2026-10-21 | P0 | — | — |

## 1. What
Use **uv** for environments and dependencies, **ruff** for lint and format, **mypy --strict** for types, **pytest + Hypothesis** (with pytest-asyncio, pytest-timeout, pytest-xdist, coverage) for tests, **pre-commit** for local hooks, and **GitHub Actions** (Ubuntu + macOS) for CI.

## 2. Why
### Problem
Without reproducible environments and strong static checks, time goes into "works on my machine" and into runtime type errors inside state machines, which are exactly the bugs that are painful to debug in a distributed system. Without a CI matrix, macOS-only behaviour (fsync, signals) silently diverges from Linux.
### Forces & constraints
- Dev on macOS, CI on Linux too.
- Fast feedback matters: the sim and property tests run often.
- Tool sprawl costs setup and maintenance time.

## 3. Alternatives considered
| Option | Pros | Cons | Verdict |
|--------|------|------|---------|
| **Chosen: uv + ruff + mypy + pytest/Hypothesis** | Fast, few tools, strict | mypy strict slows prototyping a bit | ✅ Chosen |
| A. Poetry or pip-tools + venv | Mature | Slower; no Python version management | ❌ |
| B. black + isort + flake8 | Familiar | Three tools where ruff is one | ❌ |
| C. pyright instead of mypy | Fast, good inference | No official Pydantic plugin | ⏸ Either is fine |
| D. unittest | Stdlib | No fixtures/parametrize; poor Hypothesis ergonomics | ❌ |

**A** and **B** work but are slower and spread over more tools, with no compensating benefit. **C** is a close call; mypy wins on the Pydantic plugin and on being more common in backend teams. **D** lacks the fixture model the cluster harness needs.

## 4. How
### Components & files
```
pyproject.toml          # deps, [tool.ruff], [tool.mypy], [tool.pytest.ini_options], [tool.coverage]
uv.lock                 # committed
.pre-commit-config.yaml
.github/workflows/ci.yml, nightly.yml
Makefile                # test, test-fast, sim, chaos, bench, cluster, lint
```
### Mechanism
- **Dependency groups:** runtime (`fastapi`, `uvicorn`, `httpx`, `pydantic`, `typer`, `rich`, `pyyaml`, `psutil`, `structlog`, `prometheus-client`), `dev` (`pytest*`, `hypothesis`, `mypy`, `ruff`, `pre-commit`, `matplotlib`).
- **ruff rules:** `E, F, I, B, UP, SIM, ASYNC, PL` (subset), `TID` with a banned-API list (`time.sleep` / `asyncio.sleep` banned under `tests/`; `time.monotonic` banned outside `common/clock.py`).
- **mypy:** `strict = true`, `plugins = ["pydantic.mypy"]`, `disallow_any_explicit = true` for `chronos.core` and `chronos.store`.
- **pytest:** `asyncio_mode = "auto"`, `timeout = 60`, markers `integration`, `chaos`, `durability`, `sim`, `invariant(id)`, `quarantine`; `addopts = "-m 'not chaos and not quarantine'"`.
- **CI:** described in TESTING.md §10; caches uv and the Hypothesis database.
### Configuration
`HYPOTHESIS_PROFILE` = `dev` | `ci` | `nightly` (registered in `tests/conftest.py`).
### Edge cases
- A dependency breaks on macOS only → the CI matrix catches it; pin in `uv.lock`.
- pytest-asyncio loop-scope issues → one loop per test function by default; the cluster fixtures are session-scoped but use subprocesses, not the test loop.

## 5. Consequences
**Positive:** one-command setup (`uv sync`); strict typing from day one; fast CI.
**Negative / accepted costs:** strict typing costs some prototyping speed; there's some ruff/mypy configuration to tune.
**Risks & mitigations:** tool churn → versions pinned in `uv.lock` and `.pre-commit-config.yaml`.

## 6. Verification
CI `lint-type` job green on every PR; a fresh-clone check (`git clone && uv sync && make test`) at each milestone tag.

## 7. Revisit when
mypy false positives cost more than they save (consider pyright), or uv becomes unmaintained.

## 8. Related & references
ADR-0002, ADR-0006; TESTING.md §4, §10.
