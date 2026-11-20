"""Unit tests for the processor's pure deduplication and shaping helpers."""

from __future__ import annotations

import pytest

from seismic_stream.application.processing import (
    DedupDecision,
    Deduplicator,
    shape_window_stats,
    to_window_input,
)
from seismic_stream.domain.energy import radiated_energy_joules
from tests.support.factories import make_event


def test_first_version_is_new_and_stores_the_version() -> None:
    decision, state = Deduplicator.decide(None, 1_730_000_000_000)

    assert decision is DedupDecision.NEW
    assert state == 1_730_000_000_000


def test_strictly_newer_version_is_a_revision_and_advances_state() -> None:
    decision, state = Deduplicator.decide(1_000, 2_000)

    assert decision is DedupDecision.REVISION
    assert state == 2_000


@pytest.mark.parametrize("updated", [1_000, 999])
def test_same_or_older_version_is_a_duplicate_and_keeps_state(updated: int) -> None:
    decision, state = Deduplicator.decide(1_000, updated)

    assert decision is DedupDecision.DUPLICATE
    assert state == 1_000


def test_to_window_input_adds_radiated_energy_and_strips_the_dedup_marker() -> None:
    event = make_event(magnitude=4.5)
    value = {
        "event_id": event.event_id,
        "magnitude": event.magnitude,
        "region": event.region,
        "dedup_decision": "new",
    }

    shaped = to_window_input(value)

    assert shaped["event_id"] == event.event_id
    assert shaped["region"] == event.region
    assert "dedup_decision" not in shaped
    assert shaped["energy_joules"] == pytest.approx(radiated_energy_joules(4.5))
    assert "dedup_decision" in value
    assert "energy_joules" not in value


def test_to_window_input_requires_a_magnitude() -> None:
    with pytest.raises(KeyError):
        to_window_input({"region": "CA"})


def test_shape_window_stats_maps_quix_window_fields_to_the_contract() -> None:
    window = {
        "start": 1_700_000_000_000,
        "end": 1_700_003_600_000,
        "event_count": 3,
        "max_magnitude": 5.4,
        "mean_magnitude": 4.3,
        "mean_depth_km": 8.5,
        "total_energy_joules": 1.25e14,
    }

    stats = shape_window_stats("CA", window)

    assert stats == {
        "region": "CA",
        "window_start_ms": 1_700_000_000_000,
        "window_end_ms": 1_700_003_600_000,
        "event_count": 3,
        "max_magnitude": 5.4,
        "mean_magnitude": 4.3,
        "mean_depth_km": 8.5,
        "total_energy_joules": 1.25e14,
        "emitted_at_ms": 1_700_003_600_000,
    }
    assert stats["event_count"] == int(stats["event_count"])
    assert isinstance(stats["max_magnitude"], float)


def test_shape_window_stats_rejects_missing_aggregates() -> None:
    with pytest.raises(KeyError):
        shape_window_stats("CA", {"start": 0, "end": 1})
