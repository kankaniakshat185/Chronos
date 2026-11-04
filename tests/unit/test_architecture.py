"""Architectural lint (TESTING.md section 4, ADR-0006, ADR-0023 X-1).

1. `chronos.core` stays pure: no I/O, no asyncio, no clocks, no randomness.
2. Stage 1 source never mentions `gpu`: GPU support is Stage 2 (deferred) and plugs
   in through the generic extended-resource seams.
"""

import ast
import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parents[2] / "src" / "chronos"

BANNED_IN_CORE = frozenset(
    {
        "asyncio",
        "time",
        "os",
        "subprocess",
        "socket",
        "signal",
        "threading",
        "random",
        "httpx",
        "psutil",
        "fastapi",
        "uvicorn",
    }
)


def banned_imports(source: str) -> list[str]:
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [node.module]
        else:
            continue
        found += [name for name in names if name.split(".")[0] in BANNED_IN_CORE]
    return found


def mentions_gpu(source: str) -> bool:
    return re.search(r"gpu", source, re.IGNORECASE) is not None


def test_core_modules_import_no_io() -> None:
    offenders = {
        str(path.relative_to(SRC)): bad
        for path in (SRC / "core").rglob("*.py")
        if (bad := banned_imports(path.read_text()))
    }
    assert offenders == {}


def test_no_gpu_literal_in_stage1_source() -> None:
    offenders = [str(p.relative_to(SRC)) for p in SRC.rglob("*.py") if mentions_gpu(p.read_text())]
    assert offenders == []


@pytest.mark.parametrize(
    "source",
    ["import asyncio", "import os.path", "from time import monotonic", "import random as r"],
)
def test_lint_catches_banned_imports(source: str) -> None:
    assert banned_imports(source)


@pytest.mark.parametrize(
    "source", ["from collections import Counter", "from chronos.api import Pod", "import math"]
)
def test_lint_allows_pure_imports(source: str) -> None:
    assert banned_imports(source) == []


def test_lint_catches_gpu_mentions() -> None:
    assert mentions_gpu("capacity['GPU'] = 1")
    assert not mentions_gpu("capacity['chronos.dev/slot'] = 1")


# X-8: Stage 1 never needs the GPU toolchain; numba (and numpy) belong to Stage 2's optional
# dependency group, so importing them from src/ would make an optional group mandatory.
OPTIONAL_ONLY = frozenset({"numba", "numpy", "cupy"})


def optional_imports(source: str) -> list[str]:
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names = [node.module]
        else:
            continue
        found += [name for name in names if name.split(".")[0] in OPTIONAL_ONLY]
    return found


def test_src_never_imports_optional_gpu_dependencies() -> None:
    offenders = {
        str(path.relative_to(SRC)): bad
        for path in SRC.rglob("*.py")
        if (bad := optional_imports(path.read_text()))
    }
    assert offenders == {}


def test_optional_import_guard_catches_violations() -> None:
    assert optional_imports("import numba\nfrom numpy import array\nimport os") == [
        "numba",
        "numpy",
    ]
