"""CLI tests for the doctor and verify commands (S13, S14, S20)."""

from __future__ import annotations

import json
import re

from typer.testing import CliRunner

from seismic_stream import cli
from seismic_stream.adapters.diagnostics import CheckResult
from seismic_stream.config import Settings

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(output: str) -> str:
    return ANSI_ESCAPE.sub("", output)


def _settings(**overrides: object) -> Settings:
    return Settings(_env_file=None, **overrides)


def _install(monkeypatch, checks: tuple[CheckResult, ...]) -> None:
    monkeypatch.setattr(cli, "_load_settings", _settings)
    monkeypatch.setattr(cli, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "run_doctor", lambda _settings: checks)


def test_doctor_prints_a_row_per_check_and_exits_zero_when_all_pass(monkeypatch) -> None:
    _install(
        monkeypatch,
        (
            CheckResult("kafka", "PASS", "4/4 topics"),
            CheckResult("clickhouse", "PASS", "ping ok"),
        ),
    )

    result = CliRunner().invoke(cli.app, ["doctor"])

    assert result.exit_code == 0, result.output
    output = _plain(result.output)
    assert "CHECK" in output
    assert "STATUS" in output
    assert "kafka" in output
    assert "4/4 topics" in output
    assert "PASS" in output


def test_doctor_exits_one_when_a_check_fails(monkeypatch) -> None:
    _install(
        monkeypatch,
        (
            CheckResult("kafka", "PASS", "broker reachable"),
            CheckResult("schema_registry", "FAIL", "missing subjects"),
        ),
    )

    result = CliRunner().invoke(cli.app, ["doctor"])

    assert result.exit_code == 1
    assert "FAIL" in _plain(result.output)


class FakeLagReader:
    instances: list[FakeLagReader] = []

    def __init__(self, *args: object, **kwargs: object) -> None:
        self.closed = False
        FakeLagReader.instances.append(self)

    def end_offsets(self) -> dict[tuple[str, int], int]:
        return {("seismic.events.v1", 0): 2}

    def consumer_offsets(self, group: str) -> dict[tuple[str, int], int]:
        return {("seismic.events.v1", 0): 2}

    def close(self) -> None:
        self.closed = True


def _install_verify(monkeypatch, stored: set[str]) -> None:
    FakeLagReader.instances = []
    monkeypatch.setattr(cli, "_load_settings", _settings)
    monkeypatch.setattr(cli, "configure_logging", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "_earthquake_ids", lambda _path: {"a", "b"})
    monkeypatch.setattr(cli, "stored_event_ids", lambda _settings: stored)
    monkeypatch.setattr(cli, "KafkaLagReader", FakeLagReader)


def test_verify_reports_success_when_every_id_is_stored_and_lag_is_zero(monkeypatch) -> None:
    _install_verify(monkeypatch, {"a", "b"})

    result = CliRunner().invoke(cli.app, ["verify", "--timeout", "0", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["expected"] == 2
    assert payload["missing"] == 0
    assert payload["lag"]["seismic.events.v1"] == 0
    assert FakeLagReader.instances[0].closed is True


def test_verify_exits_one_with_a_missing_id_diff(monkeypatch) -> None:
    _install_verify(monkeypatch, {"a"})

    result = CliRunner().invoke(cli.app, ["verify", "--timeout", "0", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["ok"] is False
    assert payload["missing"] == 1
    assert payload["missing_ids"] == ["b"]
    assert FakeLagReader.instances[0].closed is True


def test_verify_rejects_a_negative_timeout(monkeypatch) -> None:
    monkeypatch.setattr(cli, "_load_settings", _settings)

    result = CliRunner().invoke(cli.app, ["verify", "--timeout", "-1"])

    assert result.exit_code == 2
    assert "--timeout" in _plain(result.output)


def test_doctor_and_verify_help_document_their_options() -> None:
    doctor = CliRunner().invoke(cli.app, ["doctor", "--help"])
    verify = CliRunner().invoke(cli.app, ["verify", "--help"])
    assert doctor.exit_code == 0
    assert verify.exit_code == 0
    for option in ("--file", "--timeout", "--json"):
        assert option in _plain(verify.output)
