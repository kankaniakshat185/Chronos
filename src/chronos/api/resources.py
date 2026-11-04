"""Generic resource map (ADR-0014, ADR-0023 X-1/X-2).

Resources are a map `name -> non-negative int`. `cpu` and `memory` are just two
named entries; every other name is an extended resource. All arithmetic works
over the union of names, with a missing name meaning 0, so a node that doesn't
advertise a resource has capacity 0 for it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Self

from pydantic import BaseModel, BeforeValidator, ConfigDict, PlainSerializer, model_validator

from chronos.api.quantity import CPU, MEMORY, format_quantity, is_extended, parse_quantity

Resources = Mapping[str, int]


def parse_resources(raw: object) -> dict[str, int]:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValueError("resources must be a mapping of name -> quantity")
    return {str(name): parse_quantity(str(name), value) for name, value in raw.items()}


def format_resources(resources: Resources) -> dict[str, str]:
    """Canonical strings ("2", "8Gi", "3"): a bare int would be ambiguous for cpu (cores vs m)."""
    return {name: format_quantity(name, amount) for name, amount in sorted(resources.items())}


ResourceMap = Annotated[
    dict[str, int], BeforeValidator(parse_resources), PlainSerializer(format_resources)
]


def add(a: Resources, b: Resources) -> dict[str, int]:
    return {name: a.get(name, 0) + b.get(name, 0) for name in a.keys() | b.keys()}


def sub(a: Resources, b: Resources) -> dict[str, int]:
    return {name: a.get(name, 0) - b.get(name, 0) for name in a.keys() | b.keys()}


def insufficient(request: Resources, free: Resources) -> list[str]:
    """Names whose requested amount exceeds what's free, sorted for deterministic messages."""
    return sorted(name for name, n in request.items() if n > free.get(name, 0))


def fits(request: Resources, free: Resources) -> bool:
    return not insufficient(request, free)


class ResourceRequirements(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests: ResourceMap = {}
    limits: ResourceMap = {}

    @model_validator(mode="after")
    def _check_limits(self) -> Self:
        for name, limit in self.limits.items():
            request = self.requests.get(name, 0)
            if is_extended(name) and limit != request:
                raise ValueError(
                    f"{name}: extended resources need requests == limits "
                    f"(got request {request}, limit {limit})"
                )
            if name in (CPU, MEMORY) and name in self.requests and limit < request:
                raise ValueError(f"{name}: limit {limit} is below request {request}")
        return self
