"""CLI-level tests for the ingest command's option handling and JSON output."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from seismic_stream import cli
from seismic_stream.adapters.kafka.producer import DeliveryError
from seismic_stream.application.ingest import IngestSummary
from seismic_stream.config import Settings

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(output: str) -> str:
    """Strip ANSI styling so assertions survive colorized CI runners."""
    return ANSI_ESCAPE.sub("", output)


@dataclass
class FakeService:
    summary: IngestSummary
    stop_seen: Any = None

    def run(self, stop: Any = None) -> IngestSummary:
        self.stop_seen = stop
        return self.summary


@dataclass
class FakeSession:
    service: FakeService
    shutdown_calls: int = field(default=0)

    def shutdown(self) -> None:
        self.shutdown_calls += 1


def _settings() -> Settings:
    return Settings(_env_file=None)


def test_dry_run_prints_the_json_summary(monkeypatch) -> None:
    summary = IngestSummary(published=2, filtered=1, invalid=0, duration_seconds=0.01)
    session = FakeSession(FakeService(summary))
    captured: dict[str, Any] = {}

    def fake_build(settings: Settings, **kwargs: Any) -> FakeSession:
        captured.update(kwargs)
        return session

    monkeypatch.setattr(cli, "_load_settings", _settings)
    monkeypatch.setattr(cli, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "build_ingest_session", fake_build)

    result = CliRunner().invoke(
        cli.app,
        ["ingest", "--source", "replay", "--speed", "max", "--dry-run"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {
        "published": 2,
        "filtered": 1,
        "invalid": 0,
        "duration_seconds": 0.01,
    }
    assert captured["source_name"] == "replay"
    assert captured["speed"] is None
    assert captured["dry_run"] is True
    assert session.shutdown_calls == 1


def test_options_override_settings(monkeypatch) -> None:
    session = FakeSession(FakeService(IngestSummary(0, 0, 0, 0.0)))
    captured: dict[str, Any] = {}

    def fake_build(settings: Settings, **kwargs: Any) -> FakeSession:
        captured.update(kwargs)
        return session

    monkeypatch.setattr(cli, "_load_settings", _settings)
    monkeypatch.setattr(cli, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "build_ingest_session", fake_build)

    result = CliRunner().invoke(
        cli.app,
        [
            "ingest",
            "--source",
            "usgs",
            "--file",
            "custom.geojson",
            "--speed",
            "3600",
            "--chaos",
            "--seed",
            "7",
            "--limit",
            "12",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["source_name"] == "usgs"
    assert captured["replay_file"] == Path("custom.geojson")
    assert captured["speed"] == 3600.0
    assert captured["chaos"] is True
    assert captured["seed"] == 7
    assert captured["limit"] == 12


def test_invalid_source_is_a_usage_error(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_load_settings", _settings)

    result = CliRunner().invoke(cli.app, ["ingest", "--source", "bogus", "--dry-run"])

    assert result.exit_code == 2
    assert "--source" in _plain(result.output)


def test_invalid_speed_is_a_usage_error(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_load_settings", _settings)

    result = CliRunner().invoke(cli.app, ["ingest", "--speed", "fast", "--dry-run"])

    assert result.exit_code == 2
    assert "--speed" in _plain(result.output)


def test_non_positive_limit_is_a_usage_error(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_load_settings", _settings)

    result = CliRunner().invoke(cli.app, ["ingest", "--limit", "0", "--dry-run"])

    assert result.exit_code == 2
    assert "--limit" in _plain(result.output)


def test_delivery_errors_exit_with_code_one(monkeypatch) -> None:
    class FailingService:
        def run(self, stop: Any = None) -> IngestSummary:
            raise DeliveryError("lost message")

    class FailingSession:
        service = FailingService()

        def shutdown(self) -> None:
            pass

    monkeypatch.setattr(cli, "_load_settings", _settings)
    monkeypatch.setattr(cli, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "build_ingest_session", lambda *args, **kwargs: FailingSession())

    result = CliRunner().invoke(cli.app, ["ingest", "--source", "replay", "--dry-run"])

    assert result.exit_code == 1
    assert "lost message" in _plain(result.output)
