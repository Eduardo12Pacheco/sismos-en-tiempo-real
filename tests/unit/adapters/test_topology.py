"""Unit tests for the Quix Streams adapter wiring and error callbacks."""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from seismic_stream.adapters.stream.topology import (
    PROCESSOR_CONSUMER_GROUP,
    build_application,
    consumer_error_handler,
    processing_error_handler,
)
from seismic_stream.config import Settings


class FakeMetrics:
    """Observability fake recording only the processor counters under test."""

    def __init__(self) -> None:
        self.skipped: list[str] = []
        self.events: list[str] = []
        self.alerts: list[str] = []
        self.windows = 0

    def record_processor_skipped(self, reason: str) -> None:
        self.skipped.append(reason)

    def record_processor_event(self, outcome: str) -> None:
        self.events.append(outcome)

    def record_processor_alert(self, level: str) -> None:
        self.alerts.append(level)

    def record_window_update(self) -> None:
        self.windows += 1


def _settings(state_dir: Path) -> Settings:
    return Settings(_env_file=None, state_dir=state_dir)


def test_consumer_errors_are_logged_counted_and_suppressed(tmp_path: Path) -> None:
    metrics = FakeMetrics()
    handler = consumer_error_handler(metrics)  # type: ignore[arg-type]
    records: list[str] = []

    class Logger:
        def exception(self, message: str, *args: object, **kwargs: object) -> None:
            records.append(message)

    assert handler(ValueError("undecodable bytes"), None, Logger()) is True  # type: ignore[arg-type]
    assert metrics.skipped == ["undecodable"]
    assert len(records) == 1


@pytest.mark.parametrize("error", [KeyError("magnitude"), ValueError("bad row")])
def test_skippable_processing_errors_are_suppressed_and_counted(
    error: Exception, tmp_path: Path
) -> None:
    metrics = FakeMetrics()
    handler = processing_error_handler(metrics)  # type: ignore[arg-type]

    assert handler(error, None, logging.getLogger("test")) is True  # type: ignore[arg-type]
    assert metrics.skipped == ["processing_error"]


def test_unexpected_processing_errors_propagate() -> None:
    metrics = FakeMetrics()
    handler = processing_error_handler(metrics)  # type: ignore[arg-type]

    assert handler(RuntimeError("boom"), None, logging.getLogger("test")) is False  # type: ignore[arg-type]
    assert metrics.skipped == ["processing_error"]


def test_application_uses_the_planned_processor_consumer(tmp_path: Path) -> None:
    metrics = FakeMetrics()
    settings = _settings(tmp_path / "state")

    app = build_application(settings, metrics)  # type: ignore[arg-type]

    assert app.config.consumer_group == PROCESSOR_CONSUMER_GROUP
    assert app.config.auto_offset_reset == "earliest"
    assert app.config.processing_guarantee == "at-least-once"
    assert Path(app.config.state_dir) == settings.state_dir
