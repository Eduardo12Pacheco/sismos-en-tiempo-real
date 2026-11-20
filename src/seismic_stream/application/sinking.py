"""Sink use case: decode Kafka batches, write idempotently and dead-letter poison.

The service depends only on ports (batch consumer, event store, dead-letter
publisher, clock, metrics), so the failure modes required by S8-S11 are unit
tested without Kafka or ClickHouse. Two policies live here:

* transient failures pause the consumer, retry with capped jittered backoff and
  never commit offsets until the batch succeeds;
* permanent failures bisect the batch until the poison rows are isolated, store
  the healthy rows and dead-letter only the rejected ones.
"""

from __future__ import annotations

import random
import threading
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

from seismic_stream.application.ports import (
    BatchConsumer,
    Clock,
    ConsumedRecord,
    DeadLetterPublisher,
    EventStore,
    Metrics,
    StoredEvent,
)
from seismic_stream.application.retry import PermanentStoreError, RetryPolicy, TransientStoreError
from seismic_stream.domain.models import (
    Alert,
    DeadLetter,
    DlqStage,
    Event,
    WindowStats,
    dlq_id,
)

TABLE_EVENTS = "events"
TABLE_WINDOW_STATS = "window_stats"
TABLE_ALERTS = "alerts"
TABLE_DEAD_LETTERS = "dead_letters"

#: Continuous transient-failure time after which the sink reports unready (D10).
UNREADY_AFTER_SECONDS = 30.0

_TOPIC_TABLES: Mapping[str, str] = {
    "seismic.events.v1": TABLE_EVENTS,
    "seismic.aggregates.v1": TABLE_WINDOW_STATS,
    "seismic.alerts.v1": TABLE_ALERTS,
    "seismic.dlq.v1": TABLE_DEAD_LETTERS,
}


class RecordDecoder(Protocol):
    """Typed boundary implemented by the Avro codecs."""

    def deserialize(self, payload: bytes) -> Any:
        """Decode one Kafka value."""
        ...


@dataclass(frozen=True, slots=True)
class BisectResult[T]:
    """Rows stored after bisection and rows rejected as poison."""

    stored: tuple[T, ...]
    rejected: tuple[T, ...]


@dataclass(frozen=True, slots=True)
class SinkSummary:
    """Outcome of one sink run, ready to render as JSON."""

    batches: int
    rows: int
    dead_letters: int

    def as_dict(self) -> dict[str, int]:
        """Render a stable JSON-friendly summary."""
        return {
            "batches": self.batches,
            "rows": self.rows,
            "dead_letters": self.dead_letters,
        }


@dataclass(frozen=True, slots=True)
class _Row:
    """One decoded row plus its Kafka coordinates for tokens and DLQ envelopes."""

    table: str
    value: Event | WindowStats | Alert | DeadLetter
    record: ConsumedRecord


def bisect_write[T](
    rows: Sequence[T],
    write: Callable[[Sequence[T], str], None],
) -> BisectResult[T]:
    """Write ``rows``, splitting the chunk whenever the store rejects it.

    ``write`` receives the rows of a sub-batch and the token suffix to append
    (``""`` for the original batch, ``":<lo>-<hi>"`` for bisected row ranges, as
    required by S9). A :class:`PermanentStoreError` on a single-row chunk marks
    that row as poison; transient errors propagate to the retry loop.
    """
    if not rows:
        return BisectResult(stored=(), rejected=())
    stored: list[T] = []
    rejected: list[T] = []
    total = len(rows)

    def attempt(chunk: Sequence[T], start: int, end: int) -> None:
        suffix = "" if (start == 0 and end == total) else f":{start}-{end}"
        try:
            write(chunk, suffix)
        except PermanentStoreError:
            if len(chunk) == 1:
                rejected.append(chunk[0])
                return
            middle = start + len(chunk) // 2
            attempt(chunk[: middle - start], start, middle)
            attempt(chunk[middle - start :], middle, end)
        else:
            stored.extend(chunk)

    attempt(rows, 0, total)
    return BisectResult(stored=tuple(stored), rejected=tuple(rejected))


class SinkService:
    """Consume configured topics until stopped, storing and dead-lettering rows."""

    def __init__(
        self,
        *,
        consumer: BatchConsumer,
        store: EventStore,
        dead_letters: DeadLetterPublisher,
        metrics: Metrics,
        clock: Clock,
        retry_policy: RetryPolicy,
        codecs: Mapping[str, RecordDecoder],
        batch_size: int = 500,
        batch_max_wait_seconds: float = 1.0,
        rng: random.Random | None = None,
    ) -> None:
        if batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if batch_max_wait_seconds < 0:
            raise ValueError("batch_max_wait_seconds must be zero or positive")
        self._consumer = consumer
        self._store = store
        self._dead_letters = dead_letters
        self._metrics = metrics
        self._clock = clock
        self._retry_policy = retry_policy
        self._codecs = codecs
        self._batch_size = batch_size
        self._batch_max_wait_seconds = batch_max_wait_seconds
        self._rng = rng or random.Random()
        self._batches = 0
        self._rows = 0
        self._dead_letter_count = 0
        self._failure_started: float | None = None

    def readiness(self) -> tuple[bool, str | None]:
        """Report unready after continuous transient failures for 30 seconds (D10)."""
        if self._failure_started is None:
            return True, None
        elapsed = self._clock.now() - self._failure_started
        if elapsed < UNREADY_AFTER_SECONDS:
            return True, None
        return False, f"store unavailable for {elapsed:.0f}s"

    def run(
        self,
        stop: threading.Event | None = None,
        *,
        max_batches: int | None = None,
        idle_timeout_seconds: float | None = None,
    ) -> SinkSummary:
        """Poll, store and commit batches until stopped, exhausted or idle."""
        last_progress = self._clock.now()
        while stop is None or not stop.is_set():
            if max_batches is not None and self._batches >= max_batches:
                break
            records = self._consumer.poll_batch(
                self._batch_size,
                self._batch_max_wait_seconds,
            )
            if not records:
                if (
                    idle_timeout_seconds is not None
                    and self._clock.now() - last_progress >= idle_timeout_seconds
                ):
                    break
                continue
            last_progress = self._clock.now()
            self._process_batch(records)
            self._batches += 1
        return SinkSummary(
            batches=self._batches,
            rows=self._rows,
            dead_letters=self._dead_letter_count,
        )

    def _process_batch(self, records: Sequence[ConsumedRecord]) -> None:
        letters_before = self._dead_letter_count
        groups: dict[tuple[str, str, int], list[_Row]] = defaultdict(list)
        for record in records:
            row = self._decode(record)
            if row is not None:
                groups[(row.table, record.topic, record.partition)].append(row)

        for (table, topic, partition), rows in groups.items():
            offsets = [row.record.offset for row in rows]
            token = f"{topic}:{partition}:{min(offsets)}-{max(offsets)}"
            outcome = self._write_with_retry(table, rows, token)
            self._metrics.record_sink_batch(table, outcome)

        if self._dead_letter_count > letters_before:
            self._dead_letters.flush()
        self._consumer.commit(_next_offsets(records))

    def _decode(self, record: ConsumedRecord) -> _Row | None:
        table = _TOPIC_TABLES.get(record.topic)
        codec = self._codecs.get(record.topic)
        if table is None or codec is None:
            self._dead_letter(record, "unknown_topic", f"unexpected topic {record.topic}")
            return None
        try:
            value = codec.deserialize(record.value)
        except Exception as error:  # any decoder failure is a poison record
            self._dead_letter(record, type(error).__name__, str(error))
            return None
        return _Row(table=table, value=value, record=record)

    def _write_with_retry(self, table: str, rows: Sequence[_Row], token: str) -> str:
        attempt = 0
        retried = False
        paused = False

        def write_chunk(chunk: Sequence[_Row], suffix: str) -> None:
            nonlocal attempt, retried, paused
            while True:
                try:
                    self._write_chunk(table, chunk, token + suffix)
                except TransientStoreError:
                    retried = True
                    if not paused:
                        self._consumer.pause()
                        self._metrics.set_sink_paused(True)
                        paused = True
                        self._failure_started = self._clock.now()
                    self._metrics.record_sink_retry()
                    self._clock.sleep(self._retry_policy.delay(attempt, self._rng))
                    attempt += 1
                    continue
                return

        try:
            write_chunk(rows, "")
        except PermanentStoreError:
            result = bisect_write(rows, write_chunk)
            for row in result.rejected:
                self._dead_letter(
                    row.record,
                    "permanent_store_error",
                    "ClickHouse rejected the row permanently",
                )
            return "bisected"
        finally:
            if paused:
                self._consumer.resume()
                self._metrics.set_sink_paused(False)
                self._failure_started = None
        return "retried" if retried else "ok"

    def _write_chunk(self, table: str, rows: Sequence[_Row], token: str) -> None:
        started = self._clock.now()
        if table == TABLE_EVENTS:
            events = [
                StoredEvent(
                    event=cast(Event, row.value),
                    ingested_at_ms=_ingested_at(row.record),
                )
                for row in rows
            ]
            self._store.insert_events(
                [item.event for item in events],
                ingested_at_ms=[item.ingested_at_ms for item in events],
                token=token,
            )
            for item in events:
                if item.ingested_at_ms is not None:
                    self._metrics.observe_end_to_end_seconds(
                        (self._clock.now() * 1000 - item.ingested_at_ms) / 1000
                    )
        elif table == TABLE_WINDOW_STATS:
            self._store.insert_window_stats(
                [cast(WindowStats, row.value) for row in rows], token=token
            )
        elif table == TABLE_ALERTS:
            self._store.insert_alerts([cast(Alert, row.value) for row in rows], token=token)
        elif table == TABLE_DEAD_LETTERS:
            self._store.insert_dead_letters(
                [cast(DeadLetter, row.value) for row in rows], token=token
            )
        else:  # pragma: no cover - _TOPIC_TABLES only yields known tables
            raise PermanentStoreError(f"unknown sink table {table!r}")
        self._metrics.observe_sink_insert_seconds(table, self._clock.now() - started)
        self._metrics.record_sink_rows(table, len(rows))
        self._rows += len(rows)

    def _dead_letter(self, record: ConsumedRecord, error_type: str, message: str) -> None:
        stage = (
            DlqStage.SINK_PERSIST if error_type == "permanent_store_error" else DlqStage.SINK_DECODE
        )
        letter = DeadLetter(
            dlq_id=dlq_id(stage, record.value),
            stage=stage,
            error_type=error_type,
            error_message=message,
            source_topic=record.topic,
            source_partition=record.partition,
            source_offset=record.offset,
            payload=record.value,
            failed_at_ms=self._now_ms(),
            attempts=1,
        )
        self._dead_letters.publish(letter)
        self._metrics.record_dlq_message(letter.stage.value, error_type)
        self._dead_letter_count += 1

    def _now_ms(self) -> int:
        return int(self._clock.now() * 1000)


def _next_offsets(records: Sequence[ConsumedRecord]) -> dict[tuple[str, int], int]:
    offsets: dict[tuple[str, int], int] = {}
    for record in records:
        key = (record.topic, record.partition)
        offsets[key] = max(offsets.get(key, 0), record.offset + 1)
    return offsets


def _ingested_at(record: ConsumedRecord) -> int | None:
    raw = record.headers.get("ingested_at_ms")
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None
