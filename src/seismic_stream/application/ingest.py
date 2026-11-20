"""Ingest use case: validate source records and publish events or dead letters.

The service is deliberately I/O-free: it depends on the :mod:`ports` protocols
for the source, the replay pacer, both publishers, the clock and metrics. The
composition root supplies Kafka-backed or in-memory implementations.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import islice
from typing import Any

from seismic_stream.application.pacing import ReplayPacer
from seismic_stream.application.ports import (
    Clock,
    DeadLetterPublisher,
    EventPublisher,
    EventSource,
    Metrics,
    SourceRecord,
)
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.models import DeadLetter, DlqStage, Event, dlq_id
from seismic_stream.domain.normalize import feature_to_event

HEADER_INGESTED_AT_MS = "ingested_at_ms"
HEADER_SOURCE = "source"


@dataclass(frozen=True, slots=True)
class IngestSummary:
    """Outcome of one ingest run, ready to render as JSON."""

    published: int
    filtered: int
    invalid: int
    duration_seconds: float

    @property
    def total(self) -> int:
        """Number of source records read in this run."""
        return self.published + self.filtered + self.invalid

    def as_dict(self) -> dict[str, int | float]:
        """Render the summary with a stable, bounded precision."""
        return {
            "published": self.published,
            "filtered": self.filtered,
            "invalid": self.invalid,
            "duration_seconds": round(self.duration_seconds, 6),
        }


class InMemoryEventPublisher:
    """Collecting event publisher used by ``--dry-run`` and unit tests."""

    def __init__(self) -> None:
        self.events: list[tuple[Event, dict[str, str]]] = []
        self.flushes = 0

    def publish(self, event: Event, headers: Mapping[str, str]) -> None:
        """Keep the event and a copy of its headers."""
        self.events.append((event, dict(headers)))

    def flush(self, timeout: float | None = None) -> None:
        """Count flushes; there is no queue to drain."""
        self.flushes += 1


class InMemoryDeadLetterPublisher:
    """Collecting dead-letter publisher used by ``--dry-run`` and unit tests."""

    def __init__(self) -> None:
        self.letters: list[DeadLetter] = []
        self.flushes = 0

    def publish(self, letter: DeadLetter) -> None:
        """Keep the dead letter."""
        self.letters.append(letter)

    def flush(self, timeout: float | None = None) -> None:
        """Count flushes; there is no queue to drain."""
        self.flushes += 1


class IngestService:
    """Read a source once, publish valid events and dead-letter the rest."""

    def __init__(
        self,
        *,
        source: EventSource,
        pacer: ReplayPacer,
        events: EventPublisher,
        dead_letters: DeadLetterPublisher,
        metrics: Metrics,
        clock: Clock,
        source_name: str = "usgs",
        include_event_types: Sequence[str] = ("earthquake",),
        limit: int | None = None,
        total_records: int | None = None,
    ) -> None:
        if limit is not None and limit <= 0:
            raise ValueError(f"limit must be a positive integer, got {limit!r}")
        self._source = source
        self._pacer = pacer
        self._events = events
        self._dead_letters = dead_letters
        self._metrics = metrics
        self._clock = clock
        self._source_name = source_name
        self._include_event_types = frozenset(include_event_types)
        self._limit = limit
        self._total_records = total_records if total_records is not None else _length(source)

    def run(self, stop: threading.Event | None = None) -> IngestSummary:
        """Process the source until exhaustion, the limit or ``stop`` is set.

        Publishers are always flushed exactly once per run, including when a
        SIGTERM event stops the loop early, so queued deliveries are not lost.
        """
        started = self._clock.now()
        published = 0
        filtered = 0
        invalid = 0
        expected_total = self._expected_total()

        try:
            records: Iterable[SourceRecord] = self._source.records()
            if self._limit is not None:
                records = islice(records, self._limit)

            for index, record in enumerate(records, start=1):
                if stop is not None and stop.is_set():
                    break
                self._pacer.wait(record.event_time_ms)
                self._metrics.observe_replay_schedule_lag(self._pacer.schedule_lag_seconds)
                if expected_total is not None:
                    self._metrics.set_replay_progress(min(1.0, index / expected_total))

                try:
                    event = feature_to_event(record.feature)
                except EventValidationError as error:
                    invalid += 1
                    self._metrics.record_ingest(self._source_name, "invalid")
                    self._dead_letters.publish(self._dead_letter(record, error))
                    continue

                if event.event_type not in self._include_event_types:
                    filtered += 1
                    self._metrics.record_ingest(event.source, "filtered")
                    continue

                published += 1
                self._metrics.record_ingest(event.source, "published")
                self._events.publish(event, self._event_headers(event))
        finally:
            self._events.flush()
            self._dead_letters.flush()

        return IngestSummary(
            published=published,
            filtered=filtered,
            invalid=invalid,
            duration_seconds=self._clock.now() - started,
        )

    def _expected_total(self) -> int | None:
        if self._limit is not None:
            if self._total_records is None:
                return self._limit
            return min(self._limit, self._total_records)
        return self._total_records

    def _event_headers(self, event: Event) -> dict[str, str]:
        return {
            HEADER_SOURCE: event.source,
            HEADER_INGESTED_AT_MS: str(self._now_ms()),
        }

    def _dead_letter(
        self,
        record: SourceRecord,
        error: EventValidationError,
    ) -> DeadLetter:
        payload = _payload_bytes(record.feature)
        return DeadLetter(
            dlq_id=dlq_id(DlqStage.INGEST_VALIDATION, payload),
            stage=DlqStage.INGEST_VALIDATION,
            error_type=error.reason,
            error_message=str(error),
            source_topic=None,
            source_partition=None,
            source_offset=None,
            payload=payload,
            failed_at_ms=self._now_ms(),
            attempts=1,
        )

    def _now_ms(self) -> int:
        return int(self._clock.now() * 1000)


def _payload_bytes(feature: Mapping[str, Any]) -> bytes:
    """Serialize the raw feature deterministically for the dead-letter envelope."""
    return json.dumps(
        dict(feature),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def _length(source: EventSource) -> int | None:
    length = getattr(source, "__len__", None)
    if not callable(length):
        return None
    try:
        return int(length())
    except TypeError:  # pragma: no cover - defensive: unusual __len__ signature
        return None
