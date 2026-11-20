"""Offline contract tests for the four versioned Avro schemas.

They parse every schema with ``fastavro`` and round-trip one representative
domain object per topic without touching a Schema Registry (network is disabled
in the test suite).
"""

from __future__ import annotations

import json
from io import BytesIO
from typing import Any

import fastavro
import pytest

from seismic_stream.adapters.kafka import codecs
from seismic_stream.adapters.kafka.topics import (
    AGGREGATES_TOPIC,
    ALERTS_TOPIC,
    DLQ_TOPIC,
    EVENTS_TOPIC,
    SUBJECTS,
    TOPIC_SPECS,
)
from seismic_stream.domain.models import Alert, AlertLevel, DeadLetter, DlqStage, WindowStats
from tests.support.factories import make_event

SCHEMAS = {
    EVENTS_TOPIC: codecs.EVENT_SCHEMA_FILE,
    AGGREGATES_TOPIC: codecs.WINDOW_STATS_SCHEMA_FILE,
    ALERTS_TOPIC: codecs.ALERT_SCHEMA_FILE,
    DLQ_TOPIC: codecs.DEAD_LETTER_SCHEMA_FILE,
}


def _schema(filename: str) -> dict[str, Any]:
    return dict(fastavro.parse_schema(json.loads(codecs.load_schema(filename))))


def _round_trip(filename: str, record: dict[str, Any]) -> dict[str, Any]:
    schema = _schema(filename)
    buffer = BytesIO()
    fastavro.schemaless_writer(buffer, schema, record)
    return dict(fastavro.schemaless_reader(BytesIO(buffer.getvalue()), schema))


def test_all_schemas_parse() -> None:
    for filename in SCHEMAS.values():
        parsed = _schema(filename)
        assert parsed["type"] == "record"
        assert parsed["fields"]


def test_topic_and_subject_names_are_stable() -> None:
    assert tuple(spec.name for spec in TOPIC_SPECS) == (
        EVENTS_TOPIC,
        AGGREGATES_TOPIC,
        ALERTS_TOPIC,
        DLQ_TOPIC,
    )
    assert SUBJECTS == (
        "seismic.events.v1-value",
        "seismic.aggregates.v1-value",
        "seismic.alerts.v1-value",
        "seismic.dlq.v1-value",
    )


def test_event_schema_round_trip() -> None:
    event = make_event()
    record = codecs.event_to_dict(event)
    assert _round_trip(codecs.EVENT_SCHEMA_FILE, record) == record
    assert codecs.event_from_dict(record) == event


def test_event_schema_optional_fields_round_trip_as_null() -> None:
    event = make_event(magnitude_type=None, network=None, detail_url=None)
    record = codecs.event_to_dict(event)
    assert _round_trip(codecs.EVENT_SCHEMA_FILE, record) == record
    assert codecs.event_from_dict(record) == event


def test_window_stats_schema_round_trip() -> None:
    stats = WindowStats(
        region="CA",
        window_start_ms=1_730_000_000_000,
        window_end_ms=1_730_003_600_000,
        event_count=7,
        max_magnitude=4.9,
        mean_magnitude=3.2,
        mean_depth_km=8.5,
        total_energy_joules=1.5e10,
        emitted_at_ms=1_730_003_600_500,
    )
    record = codecs.window_stats_to_dict(stats)
    assert _round_trip(codecs.WINDOW_STATS_SCHEMA_FILE, record) == record
    assert codecs.window_stats_from_dict(record) == stats


def test_alert_schema_round_trip_with_enum_level() -> None:
    alert = Alert(
        event_id="nc75468637",
        level=AlertLevel.WARNING,
        reason="magnitude>=6.0",
        magnitude=6.1,
        place="6 km WNW of Cobb, CA",
        event_time_ms=1_730_000_000_000,
        updated_at_ms=1_730_000_060_000,
        tsunami=False,
        in_ecuador_region=False,
    )
    record = codecs.alert_to_dict(alert)
    assert _round_trip(codecs.ALERT_SCHEMA_FILE, record) == record
    assert codecs.alert_from_dict(record) == alert


def test_dead_letter_schema_round_trip_with_bytes_payload() -> None:
    letter = DeadLetter(
        dlq_id="0123456789abcdef0123456789abcdef",
        stage=DlqStage.SINK_DECODE,
        error_type="undecodable",
        error_message="not an Avro payload",
        source_topic=EVENTS_TOPIC,
        source_partition=0,
        source_offset=42,
        payload=b"\x00\x01garbage",
        failed_at_ms=1_730_000_000_000,
        attempts=1,
    )
    record = codecs.dead_letter_to_dict(letter)
    assert _round_trip(codecs.DEAD_LETTER_SCHEMA_FILE, record) == record
    assert codecs.dead_letter_from_dict(record) == letter


@pytest.mark.parametrize(
    ("filename", "field"),
    [
        (codecs.EVENT_SCHEMA_FILE, "event_id"),
        (codecs.WINDOW_STATS_SCHEMA_FILE, "region"),
        (codecs.ALERT_SCHEMA_FILE, "level"),
        (codecs.DEAD_LETTER_SCHEMA_FILE, "dlq_id"),
    ],
)
def test_required_fields_are_documented(filename: str, field: str) -> None:
    fields = {entry["name"]: entry for entry in _schema(filename)["fields"]}
    assert field in fields
    assert "default" not in fields[field]
