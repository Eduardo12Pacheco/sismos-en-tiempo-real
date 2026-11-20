"""Unit tests for the ingest use case.

The service is exercised with in-memory sources and publishers so the full
validation/filtering/backpressure contract is deterministic and needs no broker.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator, Mapping
from typing import Any

import pytest

from seismic_stream.application.ingest import (
    HEADER_INGESTED_AT_MS,
    HEADER_SOURCE,
    IngestService,
    IngestSummary,
    InMemoryDeadLetterPublisher,
    InMemoryEventPublisher,
)
from seismic_stream.application.pacing import ReplayPacer
from seismic_stream.application.ports import SourceRecord
from seismic_stream.domain.models import DeadLetter, DlqStage, Event, dlq_id
from tests.support.factories import BASE_TIME_MS, make_feature
from tests.support.fakes import FakeClock, FakeSource


class FakeMetrics:
    """Minimal observability port recording only what ingest calls."""

    def __init__(self) -> None:
        self.ingest: list[tuple[str, str]] = []
        self.backpressure_waits = 0
        self.lags: list[float] = []
        self.progress: list[float] = []

    def record_ingest(self, source: str, outcome: str) -> None:
        self.ingest.append((source, outcome))

    def record_backpressure_wait(self) -> None:
        self.backpressure_waits += 1

    def observe_replay_schedule_lag(self, seconds: float) -> None:
        self.lags.append(seconds)

    def set_replay_progress(self, ratio: float) -> None:
        self.progress.append(ratio)


class RecordingEventPublisher:
    """Event publisher that keeps every event and header for assertions."""

    def __init__(self) -> None:
        self.published: list[tuple[Event, dict[str, str]]] = []
        self.flushes: list[float | None] = []

    def publish(self, event: Event, headers: Mapping[str, str]) -> None:
        self.published.append((event, dict(headers)))

    def flush(self, timeout: float | None = None) -> None:
        self.flushes.append(timeout)


class RecordingDeadLetterPublisher:
    """Dead-letter publisher that keeps every letter for assertions."""

    def __init__(self) -> None:
        self.letters: list[DeadLetter] = []
        self.flushes: list[float | None] = []

    def publish(self, letter: DeadLetter) -> None:
        self.letters.append(letter)

    def flush(self, timeout: float | None = None) -> None:
        self.flushes.append(timeout)


def _record(**overrides: Any) -> SourceRecord:
    return SourceRecord(feature=make_feature(**overrides), event_time_ms=BASE_TIME_MS)


def _service(
    records: list[SourceRecord],
    *,
    clock: FakeClock | None = None,
    speed: float | None = None,
    limit: int | None = None,
    include_event_types: tuple[str, ...] = ("earthquake",),
    metrics: FakeMetrics | None = None,
    events: RecordingEventPublisher | None = None,
    dead_letters: RecordingDeadLetterPublisher | None = None,
) -> tuple[
    IngestService,
    FakeClock,
    FakeMetrics,
    RecordingEventPublisher,
    RecordingDeadLetterPublisher,
]:
    fake_clock = clock or FakeClock(1_000.0)
    fake_metrics = metrics or FakeMetrics()
    event_publisher = events or RecordingEventPublisher()
    dead_letter_publisher = dead_letters or RecordingDeadLetterPublisher()
    service = IngestService(
        source=FakeSource(records),
        pacer=ReplayPacer(fake_clock, speed),
        events=event_publisher,
        dead_letters=dead_letter_publisher,
        metrics=fake_metrics,
        clock=fake_clock,
        include_event_types=include_event_types,
        limit=limit,
    )
    return service, fake_clock, fake_metrics, event_publisher, dead_letter_publisher


def test_run_publishes_valid_earthquakes_with_source_headers() -> None:
    service, clock, metrics, events, dead_letters = _service([_record()])

    summary = service.run()

    assert summary.published == 1
    assert summary.filtered == 0
    assert summary.invalid == 0
    assert summary.total == 1
    event, headers = events.published[0]
    assert event.event_id == "nc75468637"
    assert headers == {
        HEADER_SOURCE: "usgs",
        HEADER_INGESTED_AT_MS: str(int(clock.now() * 1000)),
    }
    assert metrics.ingest == [("usgs", "published")]
    assert dead_letters.letters == []
    assert events.flushes == [None]
    assert dead_letters.flushes == [None]


def test_run_filters_event_types_outside_the_include_set() -> None:
    service, _, metrics, events, dead_letters = _service(
        [_record(properties={"type": "quarry blast"})]
    )

    summary = service.run()

    assert summary.published == 0
    assert summary.filtered == 1
    assert summary.invalid == 0
    assert events.published == []
    assert dead_letters.letters == []
    assert metrics.ingest == [("usgs", "filtered")]


def test_run_routes_invalid_features_to_the_dead_letter_channel() -> None:
    feature = make_feature(properties={"mag": None})
    service, clock, metrics, events, dead_letters = _service(
        [SourceRecord(feature=feature, event_time_ms=BASE_TIME_MS)]
    )

    summary = service.run()

    assert summary.invalid == 1
    assert events.published == []
    assert len(dead_letters.letters) == 1
    letter = dead_letters.letters[0]
    payload = json.dumps(feature, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )
    assert letter.stage is DlqStage.INGEST_VALIDATION
    assert letter.error_type == "missing"
    assert "properties.mag" in letter.error_message
    assert letter.payload == payload
    assert letter.dlq_id == dlq_id(DlqStage.INGEST_VALIDATION, payload)
    assert letter.failed_at_ms == int(clock.now() * 1000)
    assert letter.attempts == 1
    assert letter.source_topic is None
    assert metrics.ingest == [("usgs", "invalid")]


def test_run_counts_every_outcome_in_one_pass() -> None:
    service, _, _, events, dead_letters = _service(
        [
            _record(),
            _record(properties={"type": "ice quake"}),
            _record(properties={"mag": 99.0}),
        ]
    )

    summary = service.run()

    assert summary.published == 1
    assert summary.filtered == 1
    assert summary.invalid == 1
    assert summary.total == 3
    assert len(events.published) == 1
    assert len(dead_letters.letters) == 1


def test_limit_caps_the_number_of_source_records_read() -> None:
    records = [_record(id=f"nc{i}") for i in range(5)]
    service, _, metrics, events, _ = _service(records, limit=2)

    summary = service.run()

    assert summary.total == 2
    assert summary.published == 2
    assert len(events.published) == 2
    assert metrics.progress[-1] == pytest.approx(1.0)


def test_limit_larger_than_the_source_reads_everything() -> None:
    service, _, _, events, _ = _service([_record()], limit=10)

    summary = service.run()

    assert summary.total == 1
    assert len(events.published) == 1


def test_invalid_limit_is_rejected() -> None:
    with pytest.raises(ValueError, match="limit"):
        _service([_record()], limit=0)


def test_stop_event_ends_the_run_and_flushes_each_publisher_once() -> None:
    stop = threading.Event()
    stop.set()
    service, _, _, events, dead_letters = _service([_record()])

    summary = service.run(stop)

    assert summary.total == 0
    assert events.published == []
    assert events.flushes == [None]
    assert dead_letters.flushes == [None]


def test_stop_event_set_mid_stream_skips_the_remaining_records() -> None:
    stop = threading.Event()

    class StoppingSource(FakeSource):
        def records(self) -> Iterator[SourceRecord]:
            for index, record in enumerate(super().records()):
                yield record
                if index == 0:
                    stop.set()

    clock = FakeClock(1_000.0)
    metrics = FakeMetrics()
    events = RecordingEventPublisher()
    dead_letters = RecordingDeadLetterPublisher()
    service = IngestService(
        source=StoppingSource([_record(), _record(id="nc2")]),
        pacer=ReplayPacer(clock, None),
        events=events,
        dead_letters=dead_letters,
        metrics=metrics,
        clock=clock,
    )

    summary = service.run(stop)

    assert summary.total == 1
    assert len(events.published) == 1
    assert events.flushes == [None]


def test_pacer_waits_and_reports_progress_and_lag() -> None:
    clock = FakeClock(1_000.0)
    first = SourceRecord(feature=make_feature(id="a"), event_time_ms=BASE_TIME_MS)
    second = SourceRecord(feature=make_feature(id="b"), event_time_ms=BASE_TIME_MS + 10_000)
    service, _, metrics, events, _ = _service([first, second], clock=clock, speed=1.0)

    summary = service.run()

    assert clock.sleeps == [10.0]
    assert metrics.lags == [0.0, 0.0]
    assert metrics.progress == [0.5, 1.0]
    assert summary.duration_seconds == 10.0
    assert len(events.published) == 2


def test_summary_as_dict_is_json_ready() -> None:
    summary = IngestSummary(published=3, filtered=2, invalid=1, duration_seconds=0.123456789)

    assert summary.total == 6
    assert summary.as_dict() == {
        "published": 3,
        "filtered": 2,
        "invalid": 1,
        "duration_seconds": 0.123457,
    }


def test_in_memory_publishers_collect_records_and_count_flushes() -> None:
    events = InMemoryEventPublisher()
    dead_letters = InMemoryDeadLetterPublisher()
    event = Event(
        event_id="nc75468637",
        source="usgs",
        event_type="earthquake",
        event_time_ms=BASE_TIME_MS,
        updated_at_ms=BASE_TIME_MS,
        magnitude=4.5,
        magnitude_type=None,
        place="place",
        region="CA",
        latitude=1.0,
        longitude=2.0,
        depth_km=3.0,
        status="automatic",
        tsunami=False,
        significance=1,
        network=None,
        detail_url=None,
        in_ecuador_region=False,
    )
    letter = DeadLetter(
        dlq_id="x",
        stage=DlqStage.INGEST_VALIDATION,
        error_type="missing",
        error_message="bad",
        source_topic=None,
        source_partition=None,
        source_offset=None,
        payload=b"{}",
        failed_at_ms=0,
        attempts=1,
    )

    events.publish(event, {HEADER_SOURCE: "usgs"})
    dead_letters.publish(letter)
    events.flush()
    dead_letters.flush(1.0)

    assert events.events == [(event, {HEADER_SOURCE: "usgs"})]
    assert dead_letters.letters == [letter]
    assert events.flushes == 1
    assert dead_letters.flushes == 1


def test_invalid_feature_payload_is_deterministic() -> None:
    feature = make_feature(properties={"time": "yesterday"})
    first_service, _, _, _, first_letters = _service(
        [SourceRecord(feature=feature, event_time_ms=None)]
    )
    second_service, _, _, _, second_letters = _service(
        [SourceRecord(feature=feature, event_time_ms=None)]
    )

    first_service.run()
    second_service.run()

    assert first_letters.letters[0].dlq_id == second_letters.letters[0].dlq_id
    assert first_letters.letters[0].payload == second_letters.letters[0].payload
