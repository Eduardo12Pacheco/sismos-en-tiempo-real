"""CLI version contract."""

from __future__ import annotations

from typer.testing import CliRunner

from seismic_stream.cli import app

runner = CliRunner()


def test_version_option_prints_distribution_version() -> None:
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert "seismic-stream 0.1.0" in result.stdout


def test_version_command_prints_distribution_version() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert "seismic-stream 0.1.0" in result.stdout
