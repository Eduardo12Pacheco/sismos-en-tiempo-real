"""Tests for the typed application settings and their CLI exit-code mapping."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from typer.testing import CliRunner

from seismic_stream.adapters.sources.usgs_live import ALLOWED_FEEDS
from seismic_stream.cli import app
from seismic_stream.config import (
    ALLOWED_LIVE_FEEDS,
    ConfigError,
    Settings,
    load_settings,
)

SAMPLE = Path(__file__).parents[2] / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"


@pytest.fixture(autouse=True)
def clean_seismic_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove ambient SEISMIC_* variables so tests see deterministic defaults."""
    for key in list(os.environ):
        if key.startswith("SEISMIC_"):
            monkeypatch.delenv(key)


def test_defaults_match_the_documented_values() -> None:
    settings = Settings(_env_file=None)

    assert settings.kafka_bootstrap == "localhost:19092"
    assert settings.schema_registry_url == "http://localhost:18081"
    assert settings.clickhouse_host == "localhost"
    assert settings.clickhouse_port == 8123
    assert settings.clickhouse_user == "seismic"
    assert settings.clickhouse_password == "seismic-local"
    assert settings.clickhouse_database == "seismic"
    assert settings.ingest_source == "replay"
    assert settings.replay_file == Path("data/samples/usgs_2026-11-02_2026-11-09_m1.geojson")
    assert settings.replay_speed == 1440.0
    assert settings.replay_chaos is False
    assert settings.chaos_seed == 42
    assert settings.chaos_malformed_rate == 0.02
    assert settings.chaos_duplicate_rate == 0.05
    assert settings.chaos_revision_rate == 0.02
    assert settings.live_feed == "all_hour"
    assert settings.live_poll_seconds == 60.0
    assert settings.include_event_types == ("earthquake",)
    assert settings.alert_warning_mag == 6.0
    assert settings.alert_watch_mag == 5.0
    assert settings.alert_ecuador_mag == 4.0
    assert settings.window_minutes == 60
    assert settings.window_grace_minutes == 10
    assert settings.state_dir == Path("./.state")
    assert settings.sink_batch_size == 500
    assert settings.sink_batch_max_wait_seconds == 1.0
    assert settings.retry_base_seconds == 0.5
    assert settings.retry_max_seconds == 30.0
    assert settings.producer_queue_max_messages == 10_000
    assert settings.metrics_port == 9101
    assert settings.monitor_groups == ("seismic-processor", "seismic-sink")
    assert settings.monitor_interval_seconds == 5.0
    assert settings.log_level == "INFO"
    assert settings.log_format == "json"


def test_environment_uses_the_seismic_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEISMIC_KAFKA_BOOTSTRAP", "broker:9092")
    monkeypatch.setenv("SEISMIC_WINDOW_MINUTES", "15")
    monkeypatch.setenv("SEISMIC_REPLAY_CHAOS", "true")
    monkeypatch.setenv("SEISMIC_INCLUDE_EVENT_TYPES", "earthquake, explosion")
    monkeypatch.setenv("SEISMIC_MONITOR_GROUPS", "group-a,group-b")

    settings = load_settings(env_file=None)

    assert settings.kafka_bootstrap == "broker:9092"
    assert settings.window_minutes == 15
    assert settings.replay_chaos is True
    assert settings.include_event_types == ("earthquake", "explosion")
    assert settings.monitor_groups == ("group-a", "group-b")


def test_env_file_is_optional_and_read_when_present(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert load_settings().kafka_bootstrap == "localhost:19092"

    env_file = tmp_path / "custom.env"
    env_file.write_text("SEISMIC_WINDOW_MINUTES=20\nSEISMIC_LOG_LEVEL=debug\n", encoding="utf-8")
    settings = load_settings(env_file=env_file)

    assert settings.window_minutes == 20
    assert settings.log_level == "DEBUG"


def test_replay_speed_accepts_max(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEISMIC_REPLAY_SPEED", " max ")
    assert load_settings(env_file=None).replay_speed is None

    monkeypatch.setenv("SEISMIC_REPLAY_SPEED", "MAX")
    assert load_settings(env_file=None).replay_speed is None


def test_live_feed_catalog_is_shared_with_the_adapter() -> None:
    assert ALLOWED_LIVE_FEEDS == ALLOWED_FEEDS


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("SEISMIC_REPLAY_SPEED", "abc"),
        ("SEISMIC_REPLAY_SPEED", "0"),
        ("SEISMIC_REPLAY_SPEED", "-5"),
        ("SEISMIC_LIVE_POLL_SECONDS", "30"),
        ("SEISMIC_LIVE_POLL_SECONDS", "nan"),
        ("SEISMIC_LIVE_FEED", "hourly"),
        ("SEISMIC_INGEST_SOURCE", "kafka"),
        ("SEISMIC_LOG_FORMAT", "xml"),
        ("SEISMIC_LOG_LEVEL", "VERBOSE"),
        ("SEISMIC_CHAOS_MALFORMED_RATE", "1.5"),
        ("SEISMIC_CHAOS_DUPLICATE_RATE", "-0.1"),
        ("SEISMIC_CHAOS_REVISION_RATE", "nan"),
        ("SEISMIC_WINDOW_MINUTES", "0"),
        ("SEISMIC_WINDOW_GRACE_MINUTES", "-1"),
        ("SEISMIC_SINK_BATCH_SIZE", "0"),
        ("SEISMIC_SINK_BATCH_MAX_WAIT_SECONDS", "-1"),
        ("SEISMIC_RETRY_BASE_SECONDS", "-1"),
        ("SEISMIC_RETRY_MAX_SECONDS", "0"),
        ("SEISMIC_PRODUCER_QUEUE_MAX_MESSAGES", "0"),
        ("SEISMIC_METRICS_PORT", "70000"),
        ("SEISMIC_MONITOR_INTERVAL_SECONDS", "0"),
        ("SEISMIC_INCLUDE_EVENT_TYPES", " , "),
        ("SEISMIC_MONITOR_GROUPS", ""),
    ],
)
def test_invalid_values_raise_a_readable_config_error(
    monkeypatch: pytest.MonkeyPatch, variable: str, value: str
) -> None:
    monkeypatch.setenv(variable, value)

    with pytest.raises(ConfigError) as excinfo:
        load_settings(env_file=None)

    message = str(excinfo.value)
    assert variable in message
    assert "\n" in message


def test_config_error_does_not_echo_secret_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SEISMIC_CLICKHOUSE_PASSWORD", "super-secret-value")
    monkeypatch.setenv("SEISMIC_REPLAY_SPEED", "abc")

    with pytest.raises(ConfigError) as excinfo:
        load_settings(env_file=None)

    assert "super-secret-value" not in str(excinfo.value)


def test_replay_info_uses_the_configured_file(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEISMIC_REPLAY_FILE", str(SAMPLE))

    result = CliRunner().invoke(app, ["replay-info"])

    assert result.exit_code == 0, result.output
    assert "features:" in result.output
    assert "checksum: OK" in result.output


def test_cli_maps_config_errors_to_exit_code_two(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SEISMIC_REPLAY_SPEED", "abc")

    result = CliRunner().invoke(app, ["replay-info"])

    assert result.exit_code == 2
    assert "SEISMIC_REPLAY_SPEED" in result.output + result.stderr
