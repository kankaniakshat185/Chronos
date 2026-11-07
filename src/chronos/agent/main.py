"""`chronos-agent` entry point."""

import asyncio
import logging
import signal
import socket
import socketserver
import threading
from pathlib import Path
from typing import Annotated
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import typer
from prometheus_client import CollectorRegistry, make_wsgi_app
from prometheus_client.exposition import ThreadingWSGIServer
from pydantic import ValidationError

from chronos import __version__
from chronos.agent.agent import Agent, AgentRejected, heartbeat_timeout
from chronos.agent.providers import Providers
from chronos.agent.runtime import ProcessRuntime
from chronos.api.quantity import CPU, MEMORY, QuantityError
from chronos.api.resources import parse_resources
from chronos.client.http import HttpStoreClient
from chronos.common import logs
from chronos.common.clock import RealClock
from chronos.common.config import FencingConfig
from chronos.common.metrics import AGENT_REGISTRY, AgentCollector, ProcessMetrics
from chronos.core.agent_sync import Backoff

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
    resync_interval: Annotated[
        float,
        typer.Option(help="Re-run the sync at least this often (it also runs on every change)."),
    ] = 1.0,
    fence_timeout: Annotated[
        float,
        typer.Option(help="Stop all workloads after this long without an accepted heartbeat."),
    ] = 6.0,
    termination_grace: float = 2.0,
    drift_margin: float = 1.0,
    shim_poll: float = 0.1,
    crash_backoff_base: Annotated[
        float, typer.Option(help="First restart delay after a crash; doubles per crash.")
    ] = 1.0,
    crash_backoff_cap: float = 10.0,
    crash_backoff_reset: Annotated[
        float, typer.Option(help="A run this long resets the crash count.")
    ] = 60.0,
    metrics_port: Annotated[
        int, typer.Option(help="Serve Prometheus /metrics on this port (0: off).")
    ] = 0,
    metrics_host: str = "127.0.0.1",
    log_format: Annotated[str, typer.Option(help="text, or json (one object per line).")] = "text",
) -> None:
    """Register NAME with the control plane and run the pods bound to it."""
    try:
        capacity = parse_capacity(cpu, mem, resource or [])
        fencing = FencingConfig(
            heartbeat_interval=heartbeat_interval,
            fence_timeout=fence_timeout,
            termination_grace=termination_grace,
            drift_margin=drift_margin,
            shim_poll=shim_poll,
        )
    except ValidationError as exc:
        for err in exc.errors():
            typer.echo(f"chronos-agent: {err['msg'].removeprefix('Value error, ')}", err=True)
        raise typer.Exit(code=2) from None
    except (QuantityError, ValueError) as exc:
        typer.echo(f"chronos-agent: {exc}", err=True)
        raise typer.Exit(code=2) from None
    if log_format not in ("text", "json"):
        typer.echo(
            f"chronos-agent: --log-format must be text or json (got {log_format!r})", err=True
        )
        raise typer.Exit(code=2)
    logs.setup(log_format, text_prefix=f"{name} ")
    logs.bind(node=name)
    client = HttpStoreClient(
        server, heartbeat_timeout=heartbeat_timeout(heartbeat_interval, fence_timeout)
    )
    agent = Agent(
        name=name,
        capacity=capacity,
        client=client,
        runtime=ProcessRuntime(
            data_dir or Path.home() / ".chronos" / "agents" / name,
            name,
            fencing,
            providers=Providers(capacity),
        ),
        clock=RealClock(),
        fencing=fencing,
        resync_interval=resync_interval,
        backoff=Backoff(crash_backoff_base, crash_backoff_cap, crash_backoff_reset),
    )
    try:
        if metrics_port:
            serve_metrics(agent, metrics_host, metrics_port)
        asyncio.run(_main(agent, client))
    except AgentRejected as exc:
        typer.echo(f"chronos-agent: refused by the server: {exc}", err=True)
        raise typer.Exit(code=2) from None


class _MetricsServer(ThreadingWSGIServer):
    def server_bind(self) -> None:
        # Not HTTPServer.server_bind: it calls socket.getfqdn(host), a reverse DNS lookup
        # that can stall for many seconds (it did on GitHub's macOS runners), and this
        # runs before the agent has registered. The name is only used for CGI variables.
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = str(self.server_address[0]), self.server_address[1]
        self.setup_environ()


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args: object) -> None:
        return None


def serve_metrics(agent: Agent, host: str, port: int) -> WSGIServer:
    """A separate listener that Prometheus scrapes; the control plane never connects to
    agents (ADR-0010). Runs on its own thread."""
    registry = CollectorRegistry(auto_describe=False)
    registry.register(ProcessMetrics(AGENT_REGISTRY))
    registry.register(AgentCollector(lambda: agent.stats))
    server_class = type(
        "MetricsServer",
        (_MetricsServer,),
        {"address_family": socket.AF_INET6 if ":" in host else socket.AF_INET},
    )
    server: WSGIServer = make_server(
        host, port, make_wsgi_app(registry), server_class, _QuietHandler
    )
    threading.Thread(target=server.serve_forever, name="metrics", daemon=True).start()
    return server


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
