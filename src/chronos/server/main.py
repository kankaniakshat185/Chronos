"""`chronosd` entry point: the API server, the in-memory store and its controllers."""

import logging

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

app = typer.Typer(
    help="chronosd: the control plane (API server, store, controllers).",
    no_args_is_help=True,
    add_completion=False,
)


def build(timings: Timings) -> FastAPI:
    clock = RealClock()
    client = LocalStoreClient(Store(clock), Leases(clock), timings.eviction_timeout)
    lifecycle = NodeLifecycleController(client, clock, timings)
    scheduler = SchedulerController(
        client, clock, resolve(SchedulerConfig()), timings.scheduler_tick
    )
    deployments = DeploymentController(client, clock, timings.deployment_tick)
    return create_app(client, loops=[lifecycle.run, scheduler.run, deployments.run])


@app.command()
def run(
    host: str = "127.0.0.1",
    port: int = 7070,
    heartbeat_interval: float = 1.0,
    node_grace: float = 4.0,
    eviction_timeout: float = 10.0,
) -> None:
    """Serve the API on HOST:PORT (state is in memory; see the README's Limitations)."""
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
    uvicorn.run(build(timings), host=host, port=port, log_level="warning")


@app.command()
def version() -> None:
    """Print the Chronos version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
