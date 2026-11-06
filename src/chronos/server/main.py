"""`chronosd` entry point: the API server, the in-memory store and its controllers."""

import logging
from pathlib import Path
from typing import Annotated

import typer
import uvicorn
from fastapi import FastAPI
from pydantic import ValidationError

from chronos import __version__
from chronos.client.local import LocalStoreClient
from chronos.common.clock import RealClock
from chronos.common.config import Timings
from chronos.controllers.deployment import DeploymentController
from chronos.controllers.node_lifecycle import NodeLifecycleController
from chronos.controllers.scheduler import SchedulerController
from chronos.core.schedule import SchedulerConfig, resolve
from chronos.server.app import create_app
from chronos.store import Store
from chronos.store.leases import Leases
from chronos.store.wal import SyncMode, WalCorrupt, recover

app = typer.Typer(
    help="chronosd: the control plane (API server, store, controllers).",
    no_args_is_help=True,
    add_completion=False,
)


def build(timings: Timings, store: Store, group_window: float = 0.002) -> FastAPI:
    clock = RealClock()
    # Leases start empty at boot, so every node gets a full grace period after a restart
    # (U2.1): none is NotReady earlier than eviction_timeout after the server starts.
    client = LocalStoreClient(
        store, Leases(clock), timings.eviction_timeout, clock=clock, group_window=group_window
    )
    lifecycle = NodeLifecycleController(client, clock, timings)
    scheduler = SchedulerController(
        client, clock, resolve(SchedulerConfig()), timings.scheduler_tick
    )
    deployments = DeploymentController(client, clock, timings.deployment_tick)
    return create_app(client, loops=[lifecycle.run, scheduler.run, deployments.run])


def _open_store(data_dir: Path, sync: SyncMode, snapshot_every: int) -> Store:
    try:
        return Store.open(data_dir, RealClock(), sync=sync, snapshot_every=snapshot_every)
    except WalCorrupt as exc:
        typer.echo(f"chronosd: {data_dir}: {exc}", err=True)
        raise typer.Exit(code=3) from None


@app.command()
def run(
    *,
    host: str = "127.0.0.1",
    port: int = 7070,
    data_dir: Annotated[
        Path | None, typer.Option(help="WAL and snapshots. Default: ~/.chronos/server")
    ] = None,
    wal_sync: Annotated[
        str,
        typer.Option(
            help="full: sync each write before acking it. group: batch syncs every "
            "--group-window ms (acks and reads wait for them). none: benchmarks only."
        ),
    ] = "full",
    group_window: Annotated[float, typer.Option(help="Group-commit window, ms.")] = 2.0,
    snapshot_every: int = 5_000,
    heartbeat_interval: float = 1.0,
    node_grace: float = 4.0,
    eviction_timeout: float = 10.0,
) -> None:
    """Serve the API on HOST:PORT, with durable state in DATA_DIR."""
    try:
        timings = Timings(
            heartbeat_interval=heartbeat_interval,
            heartbeat_timeout=heartbeat_interval * 0.8,
            node_grace=node_grace,
            eviction_timeout=eviction_timeout,
        )
    except ValidationError as exc:
        typer.echo(f"chronosd: invalid timings: {exc}", err=True)
        raise typer.Exit(code=2) from None
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)  # not one line per request
    if wal_sync not in ("full", "group", "none"):
        typer.echo(f"chronosd: --wal-sync must be full, group or none (got {wal_sync!r})", err=True)
        raise typer.Exit(code=2)
    mode: SyncMode = "group" if wal_sync == "group" else "none" if wal_sync == "none" else "full"
    store = _open_store(data_dir or Path.home() / ".chronos" / "server", mode, snapshot_every)
    try:
        app_ = build(timings, store, group_window / 1000)
        uvicorn.run(app_, host=host, port=port, log_level="warning")
    finally:
        store.close()


@app.command()
def wal_verify(data_dir: Path) -> None:
    """Check DATA_DIR's log and snapshots without starting (exit 3 if corrupt)."""
    try:
        found = recover(data_dir, repair=False)
    except WalCorrupt as exc:
        typer.echo(f"corrupt: {exc}", err=True)
        raise typer.Exit(code=3) from None
    snap = found.snapshot.revision if found.snapshot else "none"
    typer.echo(f"ok: snapshot {snap}, {len(found.records)} records, revision {found.revision}")
    if found.truncated:
        typer.echo(
            f"note: a torn final record was cut at {found.truncated[0]}:{found.truncated[1]}"
        )


@app.command()
def version() -> None:
    """Print the Chronos version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
