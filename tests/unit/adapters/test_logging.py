"""Tests for structured logging configuration."""

from __future__ import annotations

import json
import logging

import pytest
import structlog

from seismic_stream.adapters.observability.logging import configure_logging


@pytest.fixture(autouse=True)
def reset_logging() -> None:
    """Restore structlog and stdlib logging after each test."""
    yield
    structlog.reset_defaults()
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    root.setLevel(logging.WARNING)


def _only_json_line(output: str) -> dict[str, object]:
    lines = [line for line in output.splitlines() if line.strip()]
    assert len(lines) == 1, output
    parsed = json.loads(lines[0])
    assert isinstance(parsed, dict)
    return parsed


def test_structlog_json_contains_the_documented_keys(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("processor", "INFO", "json")

    structlog.get_logger("seismic.processor").info("started", event_id="nc123")

    payload = _only_json_line(capsys.readouterr().out)
    assert payload["level"] == "info"
    assert payload["service"] == "processor"
    assert payload["event"] == "started"
    assert payload["logger"] == "seismic.processor"
    assert payload["event_id"] == "nc123"
    assert isinstance(payload["ts"], str)
    assert "T" in str(payload["ts"])


def test_stdlib_logging_is_bridged_to_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("sink", "INFO", "json")

    logging.getLogger("confluent_kafka").info("hi")

    payload = _only_json_line(capsys.readouterr().out)
    assert payload["event"] == "hi"
    assert payload["level"] == "info"
    assert payload["service"] == "sink"
    assert payload["logger"] == "confluent_kafka"
    assert "ts" in payload


def test_console_format_is_human_readable(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("ingest", "DEBUG", "console")

    structlog.get_logger("seismic.ingest").debug("hello", event_id="abc")

    output = capsys.readouterr().out
    assert "hello" in output
    assert "event_id=abc" in output
    assert not output.lstrip().startswith("{")


def test_unknown_format_and_level_are_rejected() -> None:
    with pytest.raises(ValueError, match="log format"):
        configure_logging("ingest", "INFO", "xml")
    with pytest.raises(ValueError, match="log level"):
        configure_logging("ingest", "VERBOSE", "json")
