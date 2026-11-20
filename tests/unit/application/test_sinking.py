"""Unit tests for the sink's retry, bisection, DLQ and commit policies."""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest

from seismic_stream.application.ports import ConsumedRecord
from seismic_stream.application.retry import (
    PermanentStoreError,
    RetryPolicy,
    TransientStoreError,
)
from seismic_stream.application.sinking import SinkService, bisect_write
from seismic_stream.domain.models import Alert, AlertLevel, DeadLetter, DlqStage, Event, WindowStats
from tests.support.factories import make_event
from tests.support.fakes import FakeClock


class FakeCodec:
    def __init__(self, mapping: Mapping[bytes, Any]) -> None:
        self._mapping = mapping

    def deserialize(self, payload: bytes) -> Any:
        try:
            return self._mapping[payload]
        except KeyError:
            raise ValueError(f"cannot decode {payload!r}") from None


class FakeConsumer:
    def __init__(self, batches: Sequence[Sequence[ConsumedRecord]]) -> None:
        self.batches = list(batches)
        self.events: list[str] = []
        self.commits: list[dict[tuple[str, int], int]] = []

    def subscribe(self, topics: Sequence[str]) -> None:
        self.events.append("subscribe")

    def poll_batch(self, max_records: int, max_wait_seconds: float) -> list[ConsumedRecord]:
        self.events.append("poll")
        return list(self.batches.pop(0)) if self.batches else []

    def pause(self) -> None:
        self.events.append("pause")

    def resume(self) -> None:
        self.events.append("resume")

    def commit(self, offsets: Mapping[tuple[str, int], int]) -> None:
        self.events.append("commit")
        self.commits.append(dict(offsets))

    def close(self) -> None:
        self.events.append("close")


@dataclass
class Write:
    table: str
    rows: list[Any]
    token: str


class FakeStore:
    def __init__(self, *, failures: int = 0, poison_ids: set[str] | None = None) -> None:
        self.failures = failures
        self.poison_ids = poison_ids or set()
        self.writes: list[Write] = []
        self.attempts = 0

    def _write(self, table: str, rows: Sequence[Any], token: str) -> None:
        self.attempts += 1
        if self.failures > 0:
            self.failures -= 1
            raise TransientStoreError("connection refused")
        if any(getattr(row, "event_id", None) in self.poison_ids for row in rows):
            raise PermanentStoreError("cannot parse row")
        self.writes.append(Write(table=table, rows=list(rows), token=token))

    def insert_events(
        self,
        events: Sequence[Event],
        *,
        ingested_at_ms: Sequence[int | None],
        token: str,
    ) -> None:
        self._write("events", events, token)

    def insert_window_stats(self, rows: Sequence[WindowStats], *, token: str) -> None:
        self._write("window_stats", rows, token)

    def insert_alerts(self, rows: Sequence[Alert], *, token: str) -> None:
        self._write("alerts", rows, token)

    def insert_dead_letters(self, rows: Sequence[DeadLetter], *, token: str) -> None:
        self._write("dead_letters", rows, token)


class FakeDeadLetters:
    def __init__(self) -> None:
        self.letters: list[DeadLetter] = []
        self.flushes = 0

    def publish(self, letter: DeadLetter) -> None:
        self.letters.append(letter)

    def flush(self, timeout: float | None = None) -> None:
        self.flushes += 1


class FakeMetrics:
    def __init__(self) -> None:
        self.retries = 0
        self.paused: list[bool] = []
        self.batches: list[tuple[str, str]] = []
        self.rows: list[tuple[str, int]] = []
        self.dlq: list[tuple[str, str]] = []
        self.latencies: list[float] = []
        self.commit_markers: list[str] = []

    def record_sink_rows(self, table: str, count: int) -> None:
        self.rows.append((table, count))

    def record_sink_batch(self, table: str, outcome: str) -> None:
        self.batches.append((table, outcome))

    def observe_sink_insert_seconds(self, table: str, seconds: float) -> None:
        pass

    def record_sink_retry(self) -> None:
        self.retries += 1

    def set_sink_paused(self, paused: bool) -> None:
        self.paused.append(paused)

    def observe_end_to_end_seconds(self, seconds: float) -> None:
        self.latencies.append(seconds)

    def record_dlq_message(self, stage: str, error_type: str) -> None:
        self.dlq.append((stage, error_type))


def _record(
    offset: int,
    value: bytes,
    *,
    topic: str = "seismic.events.v1",
    headers: Mapping[str, str] | None = None,
) -> ConsumedRecord:
    return ConsumedRecord(
        topic=topic,
        partition=0,
        offset=offset,
        key=value,
        value=value,
        headers=headers or {},
        timestamp_ms=None,
    )


def _service(  # noqa: PLR0917 - test helper mirrors service collaborators
    consumer: FakeConsumer,
    store: FakeStore,
    letters: FakeDeadLetters,
    metrics: FakeMetrics,
    clock: FakeClock,
    codecs: Mapping[str, FakeCodec],
    *,
    retry_base: float = 1.0,
) -> SinkService:
    return SinkService(
        consumer=consumer,
        store=store,
        dead_letters=letters,
        metrics=metrics,
        clock=clock,
        retry_policy=RetryPolicy(base_seconds=retry_base, max_seconds=30.0),
        codecs=codecs,
        batch_size=500,
        batch_max_wait_seconds=0.0,
        rng=random.Random(0),
    )


def test_transient_failure_pauses_retries_and_commits_only_after_success() -> None:
    event = make_event()
    consumer = FakeConsumer([[_record(0, b"e1")]])
    store = FakeStore(failures=2)
    letters = FakeDeadLetters()
    metrics = FakeMetrics()
    clock = FakeClock()
    service = _service(
        consumer,
        store,
        letters,
        metrics,
        clock,
        {"seismic.events.v1": FakeCodec({b"e1": event})},
    )

    summary = service.run(max_batches=1)

    assert summary.rows == 1
    assert consumer.events.index("pause") < consumer.events.index("resume")
    assert "commit" not in consumer.events[: consumer.events.index("resume")]
    assert consumer.commits == [{("seismic.events.v1", 0): 1}]
    assert consumer.events[-1] == "commit"
    assert metrics.paused == [True, False]
    assert metrics.retries == 2
    assert consumer.events.count("pause") == 1
    assert clock.sleeps, "retry sleeps must use the clock"
    assert len(store.writes) == 1
    assert store.writes[0].token == "seismic.events.v1:0:0-0"


def test_permanent_failure_bisects_batch_and_dead_letters_only_poison() -> None:
    events = [make_event(event_id=f"e{i}") for i in range(5)]
    values = {f"e{i}".encode(): event for i, event in enumerate(events)}
    consumer = FakeConsumer([[_record(i, f"e{i}".encode()) for i in range(5)]])
    store = FakeStore(poison_ids={"e2"})
    letters = FakeDeadLetters()
    metrics = FakeMetrics()
    clock = FakeClock()
    service = _service(
        consumer,
        store,
        letters,
        metrics,
        clock,
        {"seismic.events.v1": FakeCodec(values)},
    )

    summary = service.run(max_batches=1)

    stored = [row.event_id for write in store.writes for row in write.rows]
    assert sorted(stored) == ["e0", "e1", "e3", "e4"]
    assert len(stored) == len(set(stored))
    assert [letter.stage for letter in letters.letters] == [DlqStage.SINK_PERSIST]
    assert letters.letters[0].payload == b"e2"
    assert letters.flushes == 1
    assert metrics.batches == [("events", "bisected")]
    assert summary.rows == 4
    assert summary.dead_letters == 1
    assert consumer.commits == [{("seismic.events.v1", 0): 5}]


def test_undecodable_record_goes_to_sink_decode_with_raw_payload() -> None:
    consumer = FakeConsumer([[_record(3, b"\x00garbage")]])
    store = FakeStore()
    letters = FakeDeadLetters()
    metrics = FakeMetrics()
    service = _service(
        consumer,
        store,
        letters,
        metrics,
        FakeClock(),
        {"seismic.events.v1": FakeCodec({})},
    )

    summary = service.run(max_batches=1)

    assert summary.rows == 0
    assert len(letters.letters) == 1
    letter = letters.letters[0]
    assert letter.stage is DlqStage.SINK_DECODE
    assert letter.payload == b"\x00garbage"
    assert (letter.source_topic, letter.source_partition, letter.source_offset) == (
        "seismic.events.v1",
        0,
        3,
    )
    assert metrics.dlq == [("sink_decode", "ValueError")]
    assert letters.flushes == 1
    assert consumer.commits == [{("seismic.events.v1", 0): 4}]


def test_events_use_ingest_header_for_end_to_end_latency() -> None:
    consumer = FakeConsumer([[_record(0, b"e1", headers={"ingested_at_ms": "1000"})]])
    store = FakeStore()
    metrics = FakeMetrics()
    clock = FakeClock(now=5.0)
    service = _service(
        consumer,
        store,
        FakeDeadLetters(),
        metrics,
        clock,
        {"seismic.events.v1": FakeCodec({b"e1": make_event()})},
    )

    service.run(max_batches=1)

    assert metrics.latencies == [pytest.approx(4.0)]


def test_tokens_are_scoped_per_table_topic_and_partition() -> None:
    event = make_event()
    alert = Alert(
        event_id="e1",
        level=AlertLevel.WATCH,
        reason="magnitude>=5.0",
        magnitude=5.1,
        place="CA",
        event_time_ms=1,
        updated_at_ms=2,
        tsunami=False,
        in_ecuador_region=False,
    )
    consumer = FakeConsumer(
        [
            [
                _record(4, b"event", topic="seismic.events.v1"),
                _record(9, b"alert", topic="seismic.alerts.v1"),
            ]
        ]
    )
    store = FakeStore()
    service = _service(
        consumer,
        store,
        FakeDeadLetters(),
        FakeMetrics(),
        FakeClock(),
        {
            "seismic.events.v1": FakeCodec({b"event": event}),
            "seismic.alerts.v1": FakeCodec({b"alert": alert}),
        },
    )

    service.run(max_batches=1)

    tokens = {write.table: write.token for write in store.writes}
    assert tokens == {
        "events": "seismic.events.v1:0:4-4",
        "alerts": "seismic.alerts.v1:0:9-9",
    }


def test_bisect_write_sub_batch_token_suffixes_are_row_ranges() -> None:
    rows = ["a", "b", "c", "d"]
    calls: list[tuple[list[str], str]] = []

    def write(chunk: Sequence[str], suffix: str) -> None:
        calls.append((list(chunk), suffix))
        if "b" in chunk:
            raise PermanentStoreError("poison")

    result = bisect_write(rows, write)

    assert result.stored == ("a", "c", "d")
    assert result.rejected == ("b",)
    assert (["a"], ":0-1") in calls
    assert any(suffix == ":0-1" for _chunk, suffix in calls)
