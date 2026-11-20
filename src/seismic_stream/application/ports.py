"""Application ports shared by the ingest and replay use cases.

Every port is a :class:`typing.Protocol` so adapters depend on behaviour instead
of concrete classes, and the pure application layer never imports
infrastructure libraries.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from seismic_stream.domain.models import Alert, DeadLetter, Event, WindowStats


class SourceError(Exception):
    """Raised when an event source cannot be read or yields unusable data."""


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """One raw feature read from a source, with its event timestamp when known.

    ``injected`` marks records added by the chaos decorator. It is excluded from
    equality so an exact duplicate still compares equal to its source record.
    """

    feature: Mapping[str, Any]
    event_time_ms: int | None
    injected: bool = field(default=False, compare=False)


class EventPublisher(Protocol):
    """Sink for validated events, with delivery flushing owned by the adapter."""

    def publish(self, event: Event, headers: Mapping[str, str]) -> None:
        """Enqueue ``event`` with Kafka headers; backpressure is adapter-owned."""
        ...

    def flush(self, timeout: float | None = None) -> None:
        """Wait for queued deliveries, raising a typed delivery error on failure."""
        ...


class DeadLetterPublisher(Protocol):
    """Sink for records that could not be validated or stored."""

    def publish(self, letter: DeadLetter) -> None:
        """Publish one dead letter with a deterministic ``dlq_id`` key."""
        ...

    def flush(self, timeout: float | None = None) -> None:
        """Wait for queued dead letters, raising a typed delivery error on failure."""
        ...


class Clock(Protocol):
    """Wall-clock abstraction so replay pacing is deterministic in tests."""

    def now(self) -> float:
        """Return the current wall time in seconds."""
        ...

    def sleep(self, seconds: float) -> None:
        """Block for ``seconds`` (never negative)."""
        ...


class EventSource(Protocol):
    """Stream of raw source records, oldest first."""

    def records(self) -> Iterator[SourceRecord]:
        """Yield records lazily."""
        ...


@runtime_checkable
class Metrics(Protocol):
    """Observability port mirroring the metric catalog in the metrics adapter.

    Named methods keep the application layer free of metric-name strings and
    make each use case testable with an in-memory fake.
    """

    def set_build_info(self, service: str, version: str) -> None:
        """Expose the service name and version as build information."""
        ...

    def record_ingest(self, source: str, outcome: str) -> None:
        """Count one ingest record by source and outcome (published/filtered/invalid)."""
        ...

    def record_backpressure_wait(self) -> None:
        """Count one producer backpressure wait."""
        ...

    def observe_replay_schedule_lag(self, seconds: float) -> None:
        """Record how late the replay schedule is running, in seconds."""
        ...

    def set_replay_progress(self, ratio: float) -> None:
        """Set replay progress as a fraction in ``[0, 1]``."""
        ...

    def record_processor_event(self, outcome: str) -> None:
        """Count one processed event by outcome (new/revision/duplicate)."""
        ...

    def record_processor_alert(self, level: str) -> None:
        """Count one alert by level."""
        ...

    def record_window_update(self) -> None:
        """Count one emitted window update."""
        ...

    def record_processor_skipped(self, reason: str) -> None:
        """Count one skipped processor record (undecodable/processing_error)."""
        ...

    def record_sink_rows(self, table: str, count: int) -> None:
        """Add ``count`` stored rows for ``table``."""
        ...

    def record_sink_batch(self, table: str, outcome: str) -> None:
        """Count one sink batch by table and outcome (ok/retried/bisected)."""
        ...

    def observe_sink_insert_seconds(self, table: str, seconds: float) -> None:
        """Record the duration of one ClickHouse insert for ``table``."""
        ...

    def record_sink_retry(self) -> None:
        """Count one transient sink retry."""
        ...

    def set_sink_paused(self, paused: bool) -> None:
        """Set the sink pause gauge (1 while the consumer assignment is paused)."""
        ...

    def observe_end_to_end_seconds(self, seconds: float) -> None:
        """Record stored-minus-ingested latency for one event."""
        ...

    def record_dlq_message(self, stage: str, error_type: str) -> None:
        """Count one dead-letter message by stage and error type."""
        ...

    def set_consumer_lag(self, group: str, topic: str, lag: int) -> None:
        """Set the consumer lag in messages for ``group``/``topic``."""
        ...

    def set_topic_end_offset(self, topic: str, offset: int) -> None:
        """Set the end offset for ``topic``."""
        ...


@dataclass(frozen=True, slots=True)
class StoredEvent:
    """An event paired with the ingest timestamp used for latency metrics."""

    event: Event
    ingested_at_ms: int | None


@dataclass(frozen=True, slots=True)
class ConsumedRecord:
    """One raw Kafka message handed from the consumer adapter to the sink."""

    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes
    headers: Mapping[str, str]
    timestamp_ms: int | None


class BatchConsumer(Protocol):
    """Batched consumer with explicit pause/resume and manual commits (D9)."""

    def subscribe(self, topics: Sequence[str]) -> None:
        """Subscribe to the configured topics."""
        ...

    def poll_batch(self, max_records: int, max_wait_seconds: float) -> list[ConsumedRecord]:
        """Return up to ``max_records`` messages, waiting at most the given time."""
        ...

    def pause(self) -> None:
        """Pause every currently assigned partition."""
        ...

    def resume(self) -> None:
        """Resume every paused partition."""
        ...

    def commit(self, offsets: Mapping[tuple[str, int], int]) -> None:
        """Commit next offsets keyed by ``(topic, partition)``."""
        ...

    def close(self) -> None:
        """Release the consumer."""
        ...


@runtime_checkable
class LagReader(Protocol):
    """Read committed consumer offsets and topic end offsets for the monitor.

    Offsets are keyed by ``(topic, partition)`` so a topic with more than one
    partition is summed correctly if the stack is ever scaled out (ADR 0007).
    """

    def consumer_offsets(self, group: str) -> Mapping[tuple[str, int], int]:
        """Return the last committed offset per topic partition for ``group``."""
        ...

    def end_offsets(self) -> Mapping[tuple[str, int], int]:
        """Return the current end offset per topic partition for the probed topics."""
        ...


class EventStore(Protocol):
    """Idempotent columnar store for every table the sink writes."""

    def insert_events(
        self,
        events: Sequence[Event],
        *,
        ingested_at_ms: Sequence[int | None],
        token: str,
    ) -> None:
        """Insert events with their ingest timestamps and a deduplication token."""
        ...

    def insert_window_stats(self, rows: Sequence[WindowStats], *, token: str) -> None:
        """Insert window aggregates with a deduplication token."""
        ...

    def insert_alerts(self, rows: Sequence[Alert], *, token: str) -> None:
        """Insert alerts with a deduplication token."""
        ...

    def insert_dead_letters(self, rows: Sequence[DeadLetter], *, token: str) -> None:
        """Insert dead letters with a deduplication token."""
        ...
