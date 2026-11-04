"""Resource quantities: parsing and formatting (ADR-0014, ADR-0023 X-2).

Every quantity becomes an exact non-negative integer; floats never reach accounting.
- `cpu` is stored in millicores: "2" -> 2000, "0.5" -> 500, "250m" -> 250.
- `memory` is stored in bytes: "64Mi", "1G", "1.5Gi", or a plain integer.
- Any other name is an *extended resource*: whole units only ("2" or 2).
"""

from __future__ import annotations

import re
from fractions import Fraction

CPU = "cpu"
MEMORY = "memory"

_NAME_RE = re.compile(r"^([a-z0-9.-]+/)?[a-z0-9-]+$")
_NUMBER_RE = re.compile(r"^(\d+(?:\.\d+)?)([A-Za-z]*)$")
_BINARY = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50, "Ei": 2**60}
_DECIMAL = {"k": 10**3, "M": 10**6, "G": 10**9, "T": 10**12, "P": 10**15, "E": 10**18}


class QuantityError(ValueError):
    pass


def is_extended(name: str) -> bool:
    return name not in (CPU, MEMORY)


def validate_resource_name(name: str) -> str:
    """Bare names are allowed; an optional `domain/` prefix is too (ADR-0014)."""
    if len(name) > 253 or not _NAME_RE.fullmatch(name):
        raise QuantityError(f"invalid resource name {name!r}")
    return name


def _split(value: object, resource: str) -> tuple[Fraction, str]:
    if isinstance(value, bool):
        raise QuantityError(f"{resource}: expected a quantity, got a boolean")
    if isinstance(value, int):
        number, suffix = Fraction(value), ""
    elif isinstance(value, float):
        number, suffix = Fraction(repr(value)), ""
    elif isinstance(value, str):
        match = _NUMBER_RE.fullmatch(value.strip())
        if not match:
            raise QuantityError(f"{resource}: cannot parse quantity {value!r}")
        number, suffix = Fraction(match.group(1)), match.group(2)
    else:
        raise QuantityError(f"{resource}: expected a quantity, got {type(value).__name__}")
    if number < 0:
        raise QuantityError(f"{resource}: quantity must be non-negative")
    return number, suffix


def _whole(value: Fraction, resource: str, raw: object) -> int:
    if value.denominator != 1:
        raise QuantityError(f"{resource}: {raw!r} is not a whole number of base units")
    return value.numerator


def parse_cpu(value: object) -> int:
    number, suffix = _split(value, CPU)
    if suffix == "m":
        return _whole(number, CPU, value)
    if suffix:
        raise QuantityError(f"cpu: unknown suffix {suffix!r}")
    return _whole(number * 1000, CPU, value)


def format_cpu(millicores: int) -> str:
    return str(millicores // 1000) if millicores % 1000 == 0 else f"{millicores}m"


def parse_memory(value: object) -> int:
    number, suffix = _split(value, MEMORY)
    if suffix in _BINARY:
        return _whole(number * _BINARY[suffix], MEMORY, value)
    if suffix in _DECIMAL:
        return _whole(number * _DECIMAL[suffix], MEMORY, value)
    if suffix:
        raise QuantityError(f"memory: unknown suffix {suffix!r}")
    return _whole(number, MEMORY, value)


def format_memory(nbytes: int) -> str:
    if nbytes == 0:
        return "0"
    for table in (_BINARY, _DECIMAL):
        for suffix, unit in sorted(table.items(), key=lambda kv: -kv[1]):
            if nbytes % unit == 0:
                return f"{nbytes // unit}{suffix}"
    return str(nbytes)


def parse_extended(name: str, value: object) -> int:
    """Extended resources are integer-only and never fractional (ADR-0023 X-2)."""
    if isinstance(value, float):
        raise QuantityError(f"{name}: must be a whole number (e.g. 2), got {value!r}")
    number, suffix = _split(value, name)
    if suffix or number.denominator != 1:
        raise QuantityError(f"{name}: must be a whole number (e.g. 2), got {value!r}")
    return number.numerator


def parse_quantity(name: str, value: object) -> int:
    validate_resource_name(name)
    if name == CPU:
        return parse_cpu(value)
    if name == MEMORY:
        return parse_memory(value)
    return parse_extended(name, value)


def format_quantity(name: str, amount: int) -> str:
    if name == CPU:
        return format_cpu(amount)
    if name == MEMORY:
        return format_memory(amount)
    return str(amount)
