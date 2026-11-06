.PHONY: lint type test test-nightly test-durability check demo-cluster

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

check: lint type test

# The demo cluster: chronosd + plain-1, slot-a, slot-b (Ctrl-C to stop).
demo-cluster:
	scripts/demo-cluster.sh
