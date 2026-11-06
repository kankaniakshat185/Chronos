"""Dashboard lint (ADR-0024): panels may use only catalog metrics, and the committed
JSON is exactly what the generator produces (no hand edits that drift)."""

import importlib.util
import json
import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
DASHBOARD = ROOT / "deploy" / "observability" / "grafana" / "dashboards" / "chronos.json"
GENERATOR = DASHBOARD.parents[1] / "build_dashboard.py"
GOLDEN = ROOT / "tests" / "golden" / "metrics.txt"


def catalog_names() -> set[str]:
    return {
        line.split("{")[0] for line in GOLDEN.read_text().splitlines() if not line.startswith("#")
    }


def test_panels_use_only_catalog_metrics() -> None:
    known = catalog_names()
    used = set()
    for panel in json.loads(DASHBOARD.read_text())["panels"]:
        for target in panel.get("targets", []):
            for name in re.findall(r"\bchronos_[a-z_]+", target["expr"]):
                used.add(re.sub(r"_bucket$", "", name))
    assert used, "no queries found"
    assert used - known == set(), f"not in the metric catalog: {sorted(used - known)}"
    assert known - used == set(), f"catalog metrics with no panel: {sorted(known - used)}"


def test_committed_dashboard_matches_the_generator() -> None:
    spec = importlib.util.spec_from_file_location("build_dashboard", GENERATOR)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert DASHBOARD.read_text() == module.render()
