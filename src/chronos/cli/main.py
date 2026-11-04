"""`chronosctl` entry point. A stub until MVP Day 7 (docs/PLAN.md section A.5)."""

import typer

from chronos import __version__

app = typer.Typer(
    help="chronosctl: the command-line client.", no_args_is_help=True, add_completion=False
)


@app.command()
def run() -> None:
    """Not implemented yet."""
    typer.echo("chronosctl: not implemented yet (MVP Day 7)", err=True)
    raise typer.Exit(code=2)


@app.command()
def version() -> None:
    """Print the Chronos version."""
    typer.echo(__version__)
