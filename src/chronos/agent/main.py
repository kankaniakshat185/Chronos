"""`chronos-agent` entry point."""

import asyncio
import logging
import signal
from pathlib import Path
from typing import Annotated

import typer

from chronos import __version__
from chronos.agent.agent import Agent
from chronos.agent.runtime import ProcessRuntime
from chronos.api.quantity import CPU, MEMORY, QuantityError
from chronos.api.resources import parse_resources
from chronos.client.http import HttpStoreClient
from chronos.common.clock import RealClock

app = typer.Typer(help="chronos-agent: the node agent.", no_args_is_help=True, add_completion=False)


def parse_capacity(cpu: str, mem: str, resources: list[str]) -> dict[str, int]:
    raw: dict[str, str] = {CPU: cpu, MEMORY: mem}
    for item in resources:
        name, sep, qty = item.partition("=")
        if not sep or not name or not qty:
            raise QuantityError(f"--resource expects name=quantity, got {item!r}")
        if name in raw:
            raise QuantityError(f"--resource {name} given twice (cpu/memory use --cpu/--mem)")
        raw[name] = qty
    return parse_resources(raw)


@app.command()
def run(
    *,
    name: Annotated[str, typer.Option(help="Node name.")],
    cpu: Annotated[str, typer.Option(help='CPU capacity, e.g. "4" or "3500m".')],
    mem: Annotated[str, typer.Option(help='Memory capacity, e.g. "8Gi".')],
    resource: Annotated[
        list[str] | None,
        typer.Option(help="Extended resource name=units, e.g. chronos.dev/slot=1. Repeatable."),
    ] = None,
    server: str = "http://127.0.0.1:7070",
    data_dir: Annotated[Path | None, typer.Option(help="Default: ~/.chronos/agents/NAME")] = None,
    heartbeat_interval: float = 1.0,
    poll_interval: float = 0.5,
    termination_grace: float = 2.0,
) -> None:
    """Register NAME with the control plane and run the pods bound to it."""
    try:
        capacity = parse_capacity(cpu, mem, resource or [])
    except (QuantityError, ValueError) as exc:
        typer.echo(f"chronos-agent: {exc}", err=True)
        raise typer.Exit(code=2) from None
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {name} %(name)s %(message)s")
    client = HttpStoreClient(server, heartbeat_timeout=heartbeat_interval * 0.8)
    agent = Agent(
        name=name,
        capacity=capacity,
        client=client,
        runtime=ProcessRuntime(
            data_dir or Path.home() / ".chronos" / "agents" / name, name, termination_grace
        ),
        clock=RealClock(),
        heartbeat_interval=heartbeat_interval,
        poll_interval=poll_interval,
    )
    asyncio.run(_main(agent, client))


async def _main(agent: Agent, client: HttpStoreClient) -> None:
    task = asyncio.current_task()
    assert task is not None
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)  # graceful: stops the workloads first
    try:
        await agent.run()
    except asyncio.CancelledError:
        logging.getLogger(__name__).info("shutting down: workloads stopped")
    finally:
        await client.aclose()


@app.command()
def version() -> None:
    """Print the Chronos version."""
    typer.echo(__version__)


if __name__ == "__main__":
    app()
