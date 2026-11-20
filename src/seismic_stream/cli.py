"""Command-line entry point for the seismic stream pipeline."""

from __future__ import annotations

import typer

from seismic_stream import __version__

app = typer.Typer(
    name="seismic-stream",
    help="Local real-time earthquake streaming pipeline with offline replay.",
    no_args_is_help=True,
    add_completion=False,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"seismic-stream {__version__}")
        raise typer.Exit


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show the installed version and exit.",
    ),
) -> None:
    """Local real-time earthquake streaming pipeline with offline replay."""


@app.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(f"seismic-stream {__version__}")
