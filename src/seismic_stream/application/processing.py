"""Pure helpers for the stateful stream processor.

Deduplication, window-input enrichment and window-output shaping are plain
functions so the Quix Streams topology stays a thin adapter and the exact
semantics can be tested without a broker.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Any

from seismic_stream.domain.energy import radiated_energy_joules
from seismic_stream.domain.versioning import supersedes


class DedupDecision(StrEnum):
    """Outcome of comparing an event version with the state kept for its id."""

    NEW = "new"
    REVISION = "revision"
    DUPLICATE = "duplicate"


class Deduplicator:
    """Decide whether an ``(event_id, updated_at_ms)`` pair is new, newer or stale."""

    @staticmethod
    def decide(state: int | None, event_updated_ms: int) -> tuple[DedupDecision, int]:
        """Return the decision and the version state to persist for the id.

        ``None`` means the id was never seen, a strictly newer version is a
        revision and anything else is an exact duplicate (the state never moves
        backwards).
        """
        if state is None:
            return DedupDecision.NEW, event_updated_ms
        if supersedes(event_updated_ms, state):
            return DedupDecision.REVISION, event_updated_ms
        return DedupDecision.DUPLICATE, state


def to_window_input(value: Mapping[str, Any]) -> dict[str, Any]:
    """Copy an event record and add the radiated energy used by window sums.

    The internal dedup marker is removed so the repartition payload contains only
    event fields plus ``energy_joules``. A missing magnitude raises ``KeyError``,
    which the topology classifies as a skippable processing error.
    """
    shaped = {key: item for key, item in value.items() if key != "dedup_decision"}
    shaped["energy_joules"] = radiated_energy_joules(float(value["magnitude"]))
    return shaped


def shape_window_stats(region: str, window: Mapping[str, Any]) -> dict[str, Any]:
    """Map a flat Quix Streams multi-aggregation window to ``window_stats``.

    ``emitted_at_ms`` uses the window end, never the wall clock, so aggregates are
    byte-for-byte reproducible regardless of replay speed.
    """
    return {
        "region": region,
        "window_start_ms": int(window["start"]),
        "window_end_ms": int(window["end"]),
        "event_count": int(window["event_count"]),
        "max_magnitude": float(window["max_magnitude"]),
        "mean_magnitude": float(window["mean_magnitude"]),
        "mean_depth_km": float(window["mean_depth_km"]),
        "total_energy_joules": float(window["total_energy_joules"]),
        "emitted_at_ms": int(window["end"]),
    }
