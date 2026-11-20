"""Unit tests for the lag monitor use case (S14).

The monitor is pure application code: it reads committed and end offsets through
the :class:`LagReader` port and publishes the catalog gauges. These tests use an
in-memory reader and metrics recorder, so the exact arithmetic (including Kafka's
``-1001`` "no committed offset" sentinel) is pinned without Docker.
"""

from __future__ import annotations

import threading
from collections.abc import Mapping

import pytest

from seismic_stream.application.monitoring import LagMonitor
from seismic_stream.application.ports import LagReader

TOPICS = ("seismic.events.v1", "seismic.alerts.v1")


class FakeLagReader:
    """Reader with scripted end offsets and per-group committed offsets."""

    def __init__(
        self,
        ends: Mapping[tuple[str, int], int],
        committed: Mapping[str, Mapping[tuple[str, int], int]],
    ) -> None:
        self.ends = dict(ends)
        self.committed = {group: dict(values) for group, values in committed.items()}
        self.end_calls = 0
        self.group_calls: list[str] = []
        self.failures: list[Exception] = []
        self.on_end = None

    def end_offsets(self) -> dict[tuple[str, int], int]:
        self.end_calls += 1
        if self.on_end is not None:
            self.on_end(self)
        if self.failures:
            raise self.failures.pop(0)
        return dict(self.ends)

    def consumer_offsets(self, group: str) -> dict[tuple[str, int], int]:
        self.group_calls.append(group)
        return dict(self.committed.get(group, {}))


class RecordingMetrics:
    """Minimal recorder for the two gauges the monitor owns."""

    def __init__(self) -> None:
        self.lags: list[tuple[str, str, int]] = []
        self.end_offsets: list[tuple[str, int]] = []

    def set_consumer_lag(self, group: str, topic: str, lag: int) -> None:
        self.lags.append((group, topic, lag))

    def set_topic_end_offset(self, topic: str, offset: int) -> None:
        self.end_offsets.append((topic, offset))


def test_reader_protocol_is_satisfied() -> None:
    assert isinstance(FakeLagReader({}, {}), LagReader)


def test_poll_once_publishes_partition_summed_lag_per_group_and_topic() -> None:
    reader = FakeLagReader(
        ends={
            ("seismic.events.v1", 0): 100,
            ("seismic.events.v1", 1): 20,
            ("seismic.alerts.v1", 0): 7,
        },
        committed={
            "seismic-sink": {
                ("seismic.events.v1", 0): 40,
                ("seismic.events.v1", 1): 20,
                ("seismic.alerts.v1", 0): 3,
            }
        },
    )
    metrics = RecordingMetrics()
    monitor = LagMonitor(
        reader,
        metrics,
        groups=("seismic-sink",),
        topics=TOPICS,
    )

    snapshot = monitor.poll_once()

    assert snapshot.lags == {
        ("seismic-sink", "seismic.events.v1"): 60,
        ("seismic-sink", "seismic.alerts.v1"): 4,
    }
    assert snapshot.end_offsets == {
        "seismic.events.v1": 120,
        "seismic.alerts.v1": 7,
    }
    assert sorted(metrics.lags) == [
        ("seismic-sink", "seismic.alerts.v1", 4),
        ("seismic-sink", "seismic.events.v1", 60),
    ]
    assert sorted(metrics.end_offsets) == [
        ("seismic.alerts.v1", 7),
        ("seismic.events.v1", 120),
    ]


def test_missing_and_stale_committed_offsets_never_produce_negative_lag() -> None:
    reader = FakeLagReader(
        ends={("seismic.events.v1", 0): 50},
        committed={
            "fresh": {},
            "beyond": {("seismic.events.v1", 0): 70},
            "sentinel": {("seismic.events.v1", 0): -1001},
        },
    )
    metrics = RecordingMetrics()
    monitor = LagMonitor(reader, metrics, groups=("fresh", "beyond", "sentinel"), topics=TOPICS)

    snapshot = monitor.poll_once()

    assert snapshot.lags[("fresh", "seismic.events.v1")] == 50
    assert snapshot.lags[("beyond", "seismic.events.v1")] == 0
    assert snapshot.lags[("sentinel", "seismic.events.v1")] == 50


def test_topics_without_messages_report_zero_lag_and_end_offset() -> None:
    reader = FakeLagReader(ends={}, committed={"seismic-sink": {}})
    metrics = RecordingMetrics()
    monitor = LagMonitor(reader, metrics, groups=("seismic-sink",), topics=TOPICS)

    snapshot = monitor.poll_once()

    assert snapshot.end_offsets == dict.fromkeys(TOPICS, 0)
    assert snapshot.lags == dict.fromkeys(
        (("seismic-sink", topic) for topic in TOPICS),
        0,
    )


def test_run_polls_until_stopped_and_sleeps_the_configured_interval(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reader = FakeLagReader(ends={}, committed={"seismic-sink": {}})
    metrics = RecordingMetrics()
    stop = threading.Event()
    sleeps: list[float] = []

    class Clock:
        def now(self) -> float:
            return 0.0

        def sleep(self, seconds: float) -> None:
            sleeps.append(seconds)

    def stop_after_second_poll(_reader: FakeLagReader) -> None:
        if _reader.end_calls == 2:
            stop.set()

    reader.on_end = stop_after_second_poll
    monitor = LagMonitor(
        reader,
        metrics,
        groups=("seismic-sink",),
        topics=TOPICS,
        interval_seconds=5.0,
        clock=Clock(),
    )

    polls = monitor.run(stop)

    assert polls == 2
    assert reader.end_calls == 2
    assert sleeps == [5.0]


def test_run_reports_reader_errors_and_keeps_polling() -> None:
    reader = FakeLagReader(ends={}, committed={})
    reader.failures = [RuntimeError("broker unavailable")]
    metrics = RecordingMetrics()
    stop = threading.Event()
    errors: list[Exception] = []
    sleeps: list[float] = []

    class Clock:
        def now(self) -> float:
            return 0.0

        def sleep(self, seconds: float) -> None:
            sleeps.append(seconds)
            stop.set()

    monitor = LagMonitor(
        reader,
        metrics,
        groups=(),
        topics=TOPICS,
        interval_seconds=5.0,
        clock=Clock(),
        on_error=errors.append,
    )

    polls = monitor.run(stop)

    assert polls == 1
    assert [str(error) for error in errors] == ["broker unavailable"]
    assert sleeps == [5.0]
