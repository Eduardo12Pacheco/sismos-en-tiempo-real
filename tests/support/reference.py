"""Independent reference for processor windows and alerts.

The reference walks the *raw* source records and derives aggregates with plain
Python loops. It never imports Quix Streams, so comparing its output with the
broker topics proves the topology, not just a shared helper.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from seismic_stream.adapters.sources.replay import ReplaySource
from seismic_stream.application.ports import SourceRecord
from seismic_stream.domain.alerts import AlertPolicy
from seismic_stream.domain.energy import radiated_energy_joules
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.models import Alert, Event, WindowStats
from seismic_stream.domain.normalize import feature_to_event
from seismic_stream.domain.versioning import supersedes

DEFAULT_SAMPLE: Final = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "samples"
    / "usgs_2026-11-02_2026-11-09_m1.geojson"
)


@dataclass(frozen=True, slots=True)
class ReferenceResult:
    """Expected window statistics and final alerts for a source stream."""

    windows: Mapping[tuple[str, int], WindowStats]
    alerts: Mapping[str, Alert]


def events_from_records(
    records: Iterable[SourceRecord],
) -> Iterable[tuple[SourceRecord, Event]]:
    """Yield valid earthquake events, skipping invalid features and other types."""
    for record in records:
        try:
            event = feature_to_event(record.feature)
        except EventValidationError:
            continue
        if event.event_type != "earthquake":
            continue
        yield record, event


def window_start_ms(event_time_ms: int, window_minutes: int = 60) -> int:
    """Align an event timestamp to the start of its tumbling window."""
    window_ms = window_minutes * 60_000
    return event_time_ms - (event_time_ms % window_ms)


def reference_windows(
    records: Iterable[SourceRecord], *, window_minutes: int = 60
) -> dict[tuple[str, int], WindowStats]:
    """Aggregate the clean (non-injected) earthquakes exactly as the topology should."""
    window_ms = window_minutes * 60_000
    accumulators: dict[tuple[str, int], dict[str, Any]] = {}
    for record, event in events_from_records(records):
        if record.injected:
            continue
        start = window_start_ms(event.event_time_ms, window_minutes)
        key = (event.region, start)
        accumulator = accumulators.get(key)
        if accumulator is None:
            accumulator = {
                "event_count": 0,
                "max_magnitude": event.magnitude,
                "sum_magnitude": 0.0,
                "sum_depth_km": 0.0,
                "total_energy_joules": 0,
            }
            accumulators[key] = accumulator
        accumulator["event_count"] += 1
        accumulator["max_magnitude"] = max(accumulator["max_magnitude"], event.magnitude)
        accumulator["sum_magnitude"] += event.magnitude
        accumulator["sum_depth_km"] += event.depth_km
        accumulator["total_energy_joules"] += radiated_energy_joules(event.magnitude)

    windows: dict[tuple[str, int], WindowStats] = {}
    for (region, start), accumulator in accumulators.items():
        count = int(accumulator["event_count"])
        windows[(region, start)] = WindowStats(
            region=region,
            window_start_ms=start,
            window_end_ms=start + window_ms,
            event_count=count,
            max_magnitude=float(accumulator["max_magnitude"]),
            mean_magnitude=accumulator["sum_magnitude"] / count,
            mean_depth_km=accumulator["sum_depth_km"] / count,
            total_energy_joules=float(accumulator["total_energy_joules"]),
            emitted_at_ms=start + window_ms,
        )
    return windows


def reference_alerts(
    records: Iterable[SourceRecord],
    *,
    policy: AlertPolicy | None = None,
) -> dict[str, Alert]:
    """Evaluate the policy against the final version of every event id."""
    final: dict[str, Event] = {}
    for _record, event in events_from_records(records):
        current = final.get(event.event_id)
        if current is None or supersedes(event.updated_at_ms, current.updated_at_ms):
            final[event.event_id] = event

    alert_policy = policy or AlertPolicy()
    alerts: dict[str, Alert] = {}
    for event_id, event in final.items():
        alert = alert_policy.evaluate(event)
        if alert is not None:
            alerts[event_id] = alert
    return alerts


def reference_result(
    sample: Path | None = None,
    *,
    records: Iterable[SourceRecord] | None = None,
    policy: AlertPolicy | None = None,
    window_minutes: int = 60,
) -> ReferenceResult:
    """Compute the expected windows and alerts from a sample or source records."""
    source = records if records is not None else ReplaySource(sample or DEFAULT_SAMPLE).records()
    materialized = list(source)
    return ReferenceResult(
        windows=reference_windows(materialized, window_minutes=window_minutes),
        alerts=reference_alerts(materialized, policy=policy),
    )
