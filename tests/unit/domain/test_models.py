"""Tests for the pure domain model objects."""

from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError

import pytest

from seismic_stream.domain.models import (
    AlertLevel,
    DeadLetter,
    DlqStage,
    WindowStats,
    dlq_id,
)
from tests.support.factories import make_event


def test_event_is_frozen_and_slotted() -> None:
    event = make_event()

    with pytest.raises(FrozenInstanceError):
        event.event_id = "other"  # type: ignore[misc]

    assert not hasattr(event, "__dict__")


def test_event_version_is_updated_at_ms() -> None:
    assert make_event(updated_at_ms=123_456).version == 123_456


def test_dlq_id_matches_the_documented_definition() -> None:
    payload = b'{"bad": true}'
    expected = hashlib.sha256(b"ingest_validation|" + payload).hexdigest()[:32]

    assert dlq_id(DlqStage.INGEST_VALIDATION, payload) == expected
    assert dlq_id("ingest_validation", payload) == expected
    assert len(expected) == 32


def test_dlq_id_depends_on_stage_and_payload() -> None:
    payload = b"payload"

    assert dlq_id(DlqStage.SINK_DECODE, payload) != dlq_id(DlqStage.SINK_PERSIST, payload)
    assert dlq_id(DlqStage.SINK_DECODE, payload) != dlq_id(DlqStage.SINK_DECODE, b"other")


def test_enum_values_match_the_avro_contracts() -> None:
    assert {stage.value for stage in DlqStage} == {
        "ingest_validation",
        "sink_decode",
        "sink_persist",
    }
    assert {level.value for level in AlertLevel} == {"WATCH", "WARNING"}


def test_window_stats_and_dead_letter_are_frozen_and_slotted() -> None:
    stats = WindowStats(
        region="CA",
        window_start_ms=1_000,
        window_end_ms=2_000,
        event_count=3,
        max_magnitude=4.5,
        mean_magnitude=3.5,
        mean_depth_km=8.0,
        total_energy_joules=1.5e12,
        emitted_at_ms=2_500,
    )
    letter = DeadLetter(
        dlq_id="abc",
        stage=DlqStage.SINK_PERSIST,
        error_type="PermanentStoreError",
        error_message="rejected",
        source_topic="seismic.events.v1",
        source_partition=0,
        source_offset=7,
        payload=b"raw",
        failed_at_ms=2_500,
        attempts=1,
    )

    assert not hasattr(stats, "__dict__")
    assert not hasattr(letter, "__dict__")
    assert letter.stage is DlqStage.SINK_PERSIST

    with pytest.raises(FrozenInstanceError):
        letter.attempts = 2  # type: ignore[misc]
