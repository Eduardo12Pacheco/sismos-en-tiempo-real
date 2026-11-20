"""Domain model objects shared by every layer.

They are frozen, slotted dataclasses so they cross process boundaries cheaply
and cannot be mutated accidentally by adapters.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum


class AlertLevel(StrEnum):
    """Severity of a published alert."""

    WATCH = "WATCH"
    WARNING = "WARNING"


class DlqStage(StrEnum):
    """Pipeline stage that produced a dead letter."""

    INGEST_VALIDATION = "ingest_validation"
    SINK_DECODE = "sink_decode"
    SINK_PERSIST = "sink_persist"


@dataclass(frozen=True, slots=True)
class Event:
    """Canonical earthquake event derived from a source feature."""

    event_id: str
    source: str
    event_type: str
    event_time_ms: int
    updated_at_ms: int
    magnitude: float
    magnitude_type: str | None
    place: str
    region: str
    latitude: float
    longitude: float
    depth_km: float
    status: str
    tsunami: bool
    significance: int
    network: str | None
    detail_url: str | None
    in_ecuador_region: bool

    @property
    def version(self) -> int:
        """Version used by the processor and the ClickHouse sink."""
        return self.updated_at_ms


@dataclass(frozen=True, slots=True)
class Alert:
    """Alert emitted by the domain policy for a single event."""

    event_id: str
    level: AlertLevel
    reason: str
    magnitude: float
    place: str
    event_time_ms: int
    updated_at_ms: int
    tsunami: bool
    in_ecuador_region: bool


@dataclass(frozen=True, slots=True)
class WindowStats:
    """Aggregated statistics for one region and one event-time window."""

    region: str
    window_start_ms: int
    window_end_ms: int
    event_count: int
    max_magnitude: float
    mean_magnitude: float
    mean_depth_km: float
    total_energy_joules: float
    emitted_at_ms: int


@dataclass(frozen=True, slots=True)
class DeadLetter:
    """Envelope carrying a payload that could not be processed."""

    dlq_id: str
    stage: DlqStage
    error_type: str
    error_message: str
    source_topic: str | None
    source_partition: int | None
    source_offset: int | None
    payload: bytes
    failed_at_ms: int
    attempts: int


def dlq_id(stage: DlqStage | str, payload: bytes) -> str:
    """Return the deterministic dead-letter identifier for a stage and payload."""
    return hashlib.sha256(stage.encode("utf-8") + b"|" + payload).hexdigest()[:32]
