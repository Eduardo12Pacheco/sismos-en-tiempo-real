"""CLI-level tests for the process command's wiring and usage errors."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from prometheus_client import CollectorRegistry
from typer.testing import CliRunner

from seismic_stream import cli
from seismic_stream.config import Settings

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(output: str) -> str:
    return ANSI_ESCAPE.sub("", output)


@dataclass
class FakeApplication:
    calls: list[dict[str, Any]] = field(default_factory=list)
    error: Exception | None = None

    def run(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error


@dataclass
class FakeMetrics:
    registry: CollectorRegistry = field(default_factory=CollectorRegistry)


@dataclass
class FakeSession:
    application: FakeApplication
    metrics: FakeMetrics


class FakeServer:
    instances: list[FakeServer] = []

    def __init__(self, port: int, registry: CollectorRegistry, readiness: Any) -> None:
        self.port = port
        self.registry = registry
        self.readiness = readiness
        self.started = False
        self.stopped = False
        FakeServer.instances.append(self)

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True


def _settings(**overrides: Any) -> Settings:
    return Settings(_env_file=None, **overrides)


def _install(monkeypatch, session: FakeSession, settings: Settings) -> None:
    FakeServer.instances = []
    monkeypatch.setattr(cli, "_load_settings", lambda: settings)
    monkeypatch.setattr(cli, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "build_processor_session", lambda _settings: session)
    monkeypatch.setattr(cli, "ObservabilityServer", FakeServer)


def test_process_runs_with_the_default_processor_metrics_port(monkeypatch) -> None:
    application = FakeApplication()
    session = FakeSession(application, FakeMetrics())
    _install(monkeypatch, session, _settings())

    result = CliRunner().invoke(cli.app, ["process", "--timeout", "5", "--count", "3"])

    assert result.exit_code == 0, result.output
    assert application.calls == [{"timeout": 5.0, "count": 3, "collect": False}]
    assert FakeServer.instances[0].port == 9102
    assert FakeServer.instances[0].started is True
    assert FakeServer.instances[0].stopped is True


def test_process_honors_an_explicit_metrics_port(monkeypatch) -> None:
    session = FakeSession(FakeApplication(), FakeMetrics())
    _install(monkeypatch, session, _settings(metrics_port=12345))

    result = CliRunner().invoke(cli.app, ["process"])

    assert result.exit_code == 0, result.output
    assert FakeServer.instances[0].port == 12345


def test_process_maps_runtime_errors_to_exit_code_one(monkeypatch) -> None:
    application = FakeApplication(error=RuntimeError("broker down"))
    session = FakeSession(application, FakeMetrics())
    _install(monkeypatch, session, _settings())

    result = CliRunner().invoke(cli.app, ["process"])

    assert result.exit_code == 1
    assert FakeServer.instances[0].stopped is True


def test_process_rejects_negative_limits_as_usage_errors(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_load_settings", _settings)

    result = CliRunner().invoke(cli.app, ["process", "--timeout", "-1"])

    assert result.exit_code == 2
    assert "--timeout" in _plain(result.output)


def test_process_help_documents_every_option() -> None:
    result = CliRunner().invoke(cli.app, ["process", "--help"])

    assert result.exit_code == 0
    for option in ("--timeout", "--count"):
        assert option in _plain(result.output)
