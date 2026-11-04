.PHONY: lint type test test-nightly check

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

check: lint type test
