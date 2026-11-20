"""Offline Avro schema-evolution checks (BACKWARD compatibility).

A v2 schema that adds an optional field with a default can read v1 data; a v2
schema that adds a required field without a default cannot. The live Schema
Registry API is exercised separately by the integration test.
"""

from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import fastavro
import pytest
from fastavro._read_common import SchemaResolutionError

from seismic_stream.adapters.kafka import codecs
from tests.support.factories import make_event

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "schemas"
V1_SCHEMA = dict(fastavro.parse_schema(json.loads(codecs.load_schema(codecs.EVENT_SCHEMA_FILE))))


def _fixture(name: str) -> dict[str, Any]:
    return dict(fastavro.parse_schema(json.loads((FIXTURES / name).read_text(encoding="utf-8"))))


def _v1_payload() -> bytes:
    buffer = BytesIO()
    fastavro.schemaless_writer(buffer, V1_SCHEMA, codecs.event_to_dict(make_event()))
    return buffer.getvalue()


def test_v2_compatible_reader_reads_v1_data_with_default() -> None:
    reader = _fixture("earthquake_event.v2_compatible.avsc")
    record = fastavro.schemaless_reader(BytesIO(_v1_payload()), V1_SCHEMA, reader)
    assert record["sequence_number"] is None
    assert record["event_id"] == "nc75468637"


def test_v2_breaking_reader_rejects_v1_data() -> None:
    reader = _fixture("earthquake_event.v2_breaking.avsc")
    with pytest.raises(SchemaResolutionError, match="No default value for field sequence_number"):
        fastavro.schemaless_reader(BytesIO(_v1_payload()), V1_SCHEMA, reader)


def test_compatible_fixture_adds_optional_field_with_default() -> None:
    schema = _fixture("earthquake_event.v2_compatible.avsc")
    fields = {entry["name"]: entry for entry in schema["fields"]}
    assert "default" in fields["sequence_number"]
    assert fields["sequence_number"]["type"] == ["null", "long"]


def test_breaking_fixture_adds_required_field_without_default() -> None:
    schema = _fixture("earthquake_event.v2_breaking.avsc")
    fields = {entry["name"]: entry for entry in schema["fields"]}
    assert "default" not in fields["sequence_number"]
    assert fields["sequence_number"]["type"] == "long"
