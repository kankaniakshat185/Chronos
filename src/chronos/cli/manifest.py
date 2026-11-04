"""Load YAML manifests into API objects, with error messages that name the field."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from chronos.api import Deployment, Node, Pod
from chronos.api.kinds import Obj

KINDS: dict[str, type[Obj]] = {"Deployment": Deployment, "Pod": Pod, "Node": Node}


class ManifestError(ValueError):
    pass


def load(path: Path) -> list[Obj]:
    try:
        docs = [d for d in yaml.safe_load_all(path.read_text()) if d is not None]
    except (OSError, yaml.YAMLError) as exc:
        raise ManifestError(f"{path}: {exc}") from None
    objects: list[Obj] = []
    for i, doc in enumerate(docs, start=1):
        where = f"{path}" + (f" (document {i})" if len(docs) > 1 else "")
        if not isinstance(doc, dict) or doc.get("kind") not in KINDS:
            kind = doc.get("kind") if isinstance(doc, dict) else None
            raise ManifestError(f"{where}: kind must be one of {', '.join(KINDS)} (got {kind!r})")
        try:
            objects.append(KINDS[doc["kind"]].model_validate(doc))
        except ValidationError as exc:
            problems = "; ".join(
                f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
            )
            raise ManifestError(f"{where}: {problems}") from None
    return objects
