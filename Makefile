.PHONY: lint type test test-nightly test-durability test-sim test-chaos bench plots check demo-cluster observability observability-down

lint:
	uv run ruff check .
	uv run ruff format --check .

type:
	uv run mypy

test:
	uv run pytest

# 20 000 examples per property: no per-test timeout.
test-nightly:
	HYPOTHESIS_PROFILE=nightly uv run pytest --timeout=0 tests/property

# S8 exit run: every crash point x 50 seeds.
test-durability:
	DURABILITY_SEEDS=50 uv run pytest -n auto -q tests/durability

# Deterministic simulation: 200 seeds, the regressions and every canary (what CI runs).
test-sim:
	SIM_SEEDS=200 uv run pytest -n auto -q tests/sim

# Chaos on real processes: 60 s, 3 nodes (CHAOS_SEED, CHAOS_DURATION to change).
test-chaos:
	uv run pytest -m chaos -q tests/chaos

# All six benchmarks (several minutes each), then the plots in docs/img.
bench:
	cd bench && for b in b1_convergence b2_recovery b3_detection b4_scheduler b5_wal_sync b6_recovery; do uv run python $$b.py || exit 1; done
	cd bench && uv run python plot.py

plots:
	cd bench && uv run python plot.py

check: lint type test

# The demo cluster: chronosd + plain-1, slot-a, slot-b (Ctrl-C to stop).
demo-cluster:
	scripts/demo-cluster.sh

# Prometheus + Grafana (ADR-0024). Start the demo cluster too (it writes the targets),
# then open http://localhost:3000.
observability:
	@command -v docker >/dev/null || { echo "make observability needs Docker; Chronos itself runs without it" >&2; exit 1; }
	docker compose -f deploy/observability/docker-compose.yml up -d
	@echo "Grafana: http://localhost:3000   Prometheus: http://localhost:9090"

observability-down:
	docker compose -f deploy/observability/docker-compose.yml down
