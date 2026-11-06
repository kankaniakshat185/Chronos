"""`chronosctl`: apply manifests, inspect and change the cluster over the HTTP API."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import typer

from chronos import __version__
from chronos.api import ApiObject, Deployment, Event, Node, Pod
from chronos.api.pod import TERMINAL_PHASES
from chronos.cli import render
from chronos.cli.manifest import ManifestError, load
from chronos.client.http import HttpStoreClient
from chronos.common.clock import Clock, RealClock
from chronos.store import AlreadyExists, Compacted, Conflict, NotFound, StoreError
from chronos.store.errors import Unavailable
from chronos.store.kinds import lookup

app = typer.Typer(
    help="chronosctl: the command-line client.", no_args_is_help=True, add_completion=False
)

# Replaced by tests: how to reach the server, and what time it is.
connect: Callable[[str], HttpStoreClient] = HttpStoreClient
clock: Clock = RealClock()

CONFLICT_RETRIES = 10


class Output(StrEnum):
    TABLE = "table"
    WIDE = "wide"
    YAML = "yaml"
    JSON = "json"


@app.callback()
def main(
    ctx: typer.Context,
    server: Annotated[
        str, typer.Option(envvar="CHRONOS_SERVER", help="chronosd URL.")
    ] = "http://127.0.0.1:7070",
) -> None:
    ctx.obj = server


def _run[T](ctx: typer.Context, work: Callable[[HttpStoreClient], Awaitable[T]]) -> T:
    """Run one command against the server; turn API errors into a message and exit 1."""
    server: str = ctx.obj

    async def go() -> T:
        async with connect(server) as client:
            return await work(client)

    try:
        return asyncio.run(go())
    except Unavailable:
        typer.echo(f"error: can't reach chronosd at {server} (is it running?)", err=True)
    except (StoreError, ManifestError) as exc:
        typer.echo(f"error: {exc}", err=True)
    raise typer.Exit(code=1)


def _kind(name: str) -> type[ApiObject]:
    if (found := lookup(name)) is None:
        typer.echo(f"error: unknown kind {name!r} (nodes, pods, deployments, events)", err=True)
        raise typer.Exit(code=2)
    return found.model


async def _apply_one(client: HttpStoreClient, obj: ApiObject) -> str:
    ref = f"{obj.kind.lower()}/{obj.metadata.name}"
    try:
        await client.create(obj)
        return f"{ref} created"
    except AlreadyExists:
        pass
    for _ in range(CONFLICT_RETRIES):
        current = await client.get(type(obj), obj.metadata.name)
        if current.spec == obj.spec and current.metadata.labels == obj.metadata.labels:
            return f"{ref} unchanged"
        obj.metadata.resource_version = current.metadata.resource_version
        try:
            await client.update(obj)
            return f"{ref} configured"
        except Conflict:
            continue  # someone wrote in between (e.g. a controller's status): re-read
    raise StoreError(f"{ref}: still conflicting after {CONFLICT_RETRIES} attempts")


@app.command()
def apply(
    ctx: typer.Context,
    filename: Annotated[Path, typer.Option("--filename", "-f", help="YAML manifest.")],
) -> None:
    """Create or update the objects in a manifest (compare-and-swap, retried on conflict)."""

    async def work(client: HttpStoreClient) -> None:
        for obj in load(filename):
            typer.echo(await _apply_one(client, obj))

    _run(ctx, work)


def _table(model: type[ApiObject], items: list[Any], now: datetime, wide: bool) -> str:
    if model is Node:
        return render.nodes(items, now)
    if model is Pod:
        return render.pods(items, now, wide=wide)
    if model is Deployment:
        return render.deployments(items, now)
    if model is Event:
        return render.events(items, now)
    return render.generic(items, now)


def _format(model: type[ApiObject], items: list[Any], output: Output) -> str:
    match output:
        case Output.YAML:
            return render.as_yaml(items).rstrip("\n")
        case Output.JSON:
            return render.as_json(items)
        case _:
            return _table(model, items, clock.wall(), output == Output.WIDE)


@app.command()
def get(
    ctx: typer.Context,
    kind: Annotated[str, typer.Argument(help="nodes, pods, deployments, events, ...")],
    name: Annotated[str | None, typer.Argument(help="Only this object.")] = None,
    *,
    output: Annotated[Output, typer.Option("--output", "-o")] = Output.TABLE,
    watch: Annotated[
        bool, typer.Option("--watch", "-w", help="Print again after every change (Ctrl-C).")
    ] = False,
    max_updates: Annotated[int, typer.Option(hidden=True)] = 0,  # tests: stop after N
) -> None:
    """List objects of a kind (or one of them), once or as they change."""
    model = _kind(kind)

    async def listing(client: HttpStoreClient) -> tuple[str, int]:
        if name is not None:
            obj = await client.get(model, name)
            return _format(model, [obj], output), obj.metadata.resource_version
        items, revision = await client.list(model)
        return _format(model, items, output), revision

    async def work(client: HttpStoreClient) -> None:
        text, revision = await listing(client)
        typer.echo(text)
        updates = 0
        while watch and (not max_updates or updates < max_updates):
            try:
                batch = await client.watch(model, revision)
            except Compacted:
                batch = None  # fell behind the history: just list again
            if batch is not None and not batch.events:
                revision = batch.revision
                continue
            text, revision = await listing(client)
            typer.echo(f"\n{text}")
            updates += 1

    _run(ctx, work)


@app.command()
def describe(ctx: typer.Context, kind: str, name: str) -> None:
    """Show one object in full, with its recent events."""
    model = _kind(kind)

    async def work(client: HttpStoreClient) -> str:
        obj = await client.get(model, name)
        events, _ = await client.list(Event)
        mine = [
            e for e in events
            if (e.spec.involved_kind, e.spec.involved_name) == (model.__name__, name)
        ]  # fmt: skip
        return render.describe(obj, mine, clock.wall())

    typer.echo(_run(ctx, work))


@app.command()
def logs(
    ctx: typer.Context,
    pod: str,
    stderr: Annotated[bool, typer.Option("--stderr", help="stderr instead of stdout.")] = False,
    agents_dir: Annotated[
        Path | None,
        typer.Option(
            envvar="CHRONOS_AGENTS_DIR",
            help="Where the agents keep their data. Default: ~/.chronos/agents",
        ),
    ] = None,
) -> None:
    """Print a pod's output (local mode: the agents run on this machine)."""

    async def work(client: HttpStoreClient) -> Pod:
        return await client.get(Pod, pod)

    found = _run(ctx, work)
    if not found.spec.node_name:
        typer.echo(f"error: pod {pod} isn't on a node yet", err=True)
        raise typer.Exit(code=1)
    root = agents_dir or Path.home() / ".chronos" / "agents"
    path = root / found.spec.node_name / "pods" / found.metadata.uid
    path /= "stderr.log" if stderr else "stdout.log"
    if not path.exists():
        typer.echo(f"error: no log at {path} (is --agents-dir right?)", err=True)
        raise typer.Exit(code=1)
    typer.echo(path.read_text(), nl=False)


def _set_unschedulable(ctx: typer.Context, name: str, value: bool) -> None:
    async def work(client: HttpStoreClient) -> None:
        for _ in range(CONFLICT_RETRIES):
            node = await client.get(Node, name)
            node.spec.unschedulable = value
            try:
                await client.update(node)
            except Conflict:
                continue  # a heartbeat-driven status write in between: re-read
            typer.echo(f"node/{name} {'cordoned' if value else 'uncordoned'}")
            return
        raise StoreError(f"node/{name}: still conflicting after {CONFLICT_RETRIES} attempts")

    _run(ctx, work)


@app.command()
def cordon(ctx: typer.Context, node: str) -> None:
    """Mark a node unschedulable: running pods stay, no new ones land there."""
    _set_unschedulable(ctx, node, value=True)


@app.command()
def uncordon(ctx: typer.Context, node: str) -> None:
    """Let new pods land on a node again."""
    _set_unschedulable(ctx, node, value=False)


@app.command()
def drain(
    ctx: typer.Context,
    node: str,
    timeout: Annotated[float, typer.Option(help="Seconds to wait for each pod to stop.")] = 60.0,
) -> None:
    """Cordon a node, then evict its pods one at a time (P10.4).

    Each pod is deleted (its agent stops the process, two-phase) and waited for before the
    next, so a deployment loses at most one replica at a time to the drain; its controller
    recreates the pod on another node. Run `uncordon` to use the node again.
    """
    _set_unschedulable(ctx, node, value=True)

    async def work(client: HttpStoreClient) -> None:
        pods, _ = await client.list(Pod, node_name=node)
        victims = [
            p for p in pods
            if p.status.phase not in TERMINAL_PHASES and p.metadata.deletion_timestamp is None
        ]  # fmt: skip
        for pod in victims:
            name = pod.metadata.name
            try:
                await client.delete(Pod, name)
            except NotFound:
                continue
            start = clock.monotonic()
            while clock.monotonic() - start < timeout:
                try:
                    await client.get(Pod, name)
                except NotFound:
                    break
                await clock.sleep(0.1)
            else:
                raise StoreError(f"pod/{name} still there after {timeout:g}s; drain stopped")
            typer.echo(f"pod/{name} evicted")
        typer.echo(f"node/{node} drained ({len(victims)} pods)")

    _run(ctx, work)


@app.command()
def scale(
    ctx: typer.Context,
    kind: str,
    name: str,
    replicas: Annotated[int, typer.Option(min=0)],
) -> None:
    """Set a deployment's replica count."""
    if _kind(kind) is not Deployment:
        typer.echo("error: only deployments can be scaled", err=True)
        raise typer.Exit(code=2)

    async def work(client: HttpStoreClient) -> None:
        for _ in range(CONFLICT_RETRIES):
            dep = await client.get(Deployment, name)
            dep.spec.replicas = replicas
            try:
                await client.update(dep)
            except Conflict:
                continue
            typer.echo(f"deployment/{name} scaled to {replicas}")
            return
        raise StoreError(f"deployment/{name}: still conflicting after {CONFLICT_RETRIES} attempts")

    _run(ctx, work)


@app.command()
def delete(ctx: typer.Context, kind: str, name: str) -> None:
    """Delete an object (a deployment's pods are garbage-collected after it)."""
    model = _kind(kind)

    async def work(client: HttpStoreClient) -> None:
        await client.delete(model, name)
        typer.echo(f"{model.__name__.lower()}/{name} deleted")

    _run(ctx, work)


@app.command()
def version() -> None:
    """Print the Chronos version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
