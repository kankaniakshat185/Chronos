"""Kind registry (X-6, ADR-0023): everything the framework needs to know about a kind.

A kind registers its model, URL name, CLI aliases, store write rules and secondary
indexes once, with `register_kind`. The store, the API routes, both clients, the
manifest loader and the CLI look kinds up here; none of them names a kind. Node,
Pod and Deployment register through the same call a new kind would use.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from chronos.api import Deployment, Node, Pod
from chronos.api.meta import ApiObject
from chronos.store import validation

type Validator = Callable[[ApiObject], None]
type UpdateValidator = Callable[[ApiObject, ApiObject], None]  # (current, new)
type IndexFn = Callable[[ApiObject], str]

# The index that `list(..., node_name=)` and the watch `node=` filter use.
NODE_INDEX = "node"


def _accept(*_: ApiObject) -> None:
    return None


@dataclass(frozen=True)
class KindInfo:
    model: type[ApiObject]
    plural: str
    aliases: tuple[str, ...] = ()
    # Raise `Invalid` to refuse a write; called inside the store's transaction.
    validate_create: Validator = _accept
    validate_spec_update: UpdateValidator = _accept
    validate_status_update: UpdateValidator = _accept
    indexes: Mapping[str, IndexFn] = field(default_factory=dict)  # name -> key of an object

    @property
    def name(self) -> str:
        return self.model.__name__


KINDS: dict[str, KindInfo] = {}


class KindError(ValueError):
    pass


def register_kind(info: KindInfo) -> None:
    if info.name in KINDS:
        raise KindError(f"kind {info.name!r} is already registered")
    taken = {n for k in KINDS.values() for n in (k.plural, *k.aliases)}
    if clash := taken & {info.plural, *info.aliases}:
        raise KindError(f"kind {info.name!r}: name(s) already used: {', '.join(sorted(clash))}")
    KINDS[info.name] = info


def info(kind: type[ApiObject] | str) -> KindInfo:
    name = kind if isinstance(kind, str) else kind.__name__
    try:
        return KINDS[name]
    except KeyError:
        raise KindError(f"unknown kind {name!r}") from None


def lookup(word: str) -> KindInfo | None:
    """A kind by name, plural or alias, case-insensitively (for the CLI)."""
    word = word.lower()
    for k in KINDS.values():
        if word in (k.name.lower(), k.plural, *k.aliases):
            return k
    return None


def _pod_node(obj: ApiObject) -> str:
    assert isinstance(obj, Pod)
    return obj.spec.node_name


register_kind(KindInfo(Node, "nodes", ("node", "no")))
register_kind(
    KindInfo(
        Pod,
        "pods",
        ("pod", "po"),
        validate_create=validation.validate_pod_create,
        validate_spec_update=validation.validate_pod_spec_update,
        validate_status_update=validation.validate_pod_status_update,
        indexes={NODE_INDEX: _pod_node},  # "" = unbound
    )
)
register_kind(KindInfo(Deployment, "deployments", ("deployment", "deploy")))
