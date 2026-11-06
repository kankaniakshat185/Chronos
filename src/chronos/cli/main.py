"""`chronosctl`: apply manifests, inspect and change the cluster over the HTTP API."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import typer

from chronos import __version__
from chronos.api import ApiObject, Deployment, Node, Pod
from chronos.cli import render
from chronos.cli.manifest import ManifestError, load
from chronos.client.http import HttpStoreClient
from chronos.common.clock import Clock, RealClock
from chronos.store import AlreadyExists, Conflict, StoreError
from chronos.store.errors import Unavailable
from chronos.store.kinds import lookup

app = typer.Typer(
    help="chronosctl: the command-line client.", no_args_is_help=True, add_completion=False
)

# Replaced by tests: how to reach the server, and what time it is.
connect: Callable[[str], HttpStoreClient] = HttpStoreClient
clock: Clock = RealClock()

CONFLICT_RETRIES = 10


class Kind(StrEnum):
    NODES = "nodes"
    PODS = "pods"
    DEPLOYMENTS = "deployments"


class Output(StrEnum):
    TABLE = "table"
    WIDE = "wide"


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
        typer.echo(f"error: unknown kind {name!r} (nodes, pods, deployments)", err=True)
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


@app.command()
def get(
    ctx: typer.Context,
    kind: Kind,
    output: Annotated[Output, typer.Option("--output", "-o")] = Output.TABLE,
) -> None:
    """List nodes, pods or deployments."""

    async def work(client: HttpStoreClient) -> str:
        now = clock.wall()
        match kind:
            case Kind.NODES:
                return render.nodes((await client.list(Node))[0], now)
            case Kind.PODS:
                return render.pods((await client.list(Pod))[0], now, wide=output == Output.WIDE)
            case Kind.DEPLOYMENTS:
                return render.deployments((await client.list(Deployment))[0], now)

    typer.echo(_run(ctx, work))


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
