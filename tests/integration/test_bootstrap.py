"""Integration checks for bootstrap against a real Redpanda Schema Registry."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.schema_registry import SchemaRegistryClient, SchemaRegistryError
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from seismic_stream.adapters.kafka import codecs
from seismic_stream.adapters.kafka.registry import SchemaRegistryAdmin
from seismic_stream.adapters.kafka.topics import SUBJECTS, TOPIC_SPECS
from tests.support.factories import make_event

pytestmark = pytest.mark.integration

SCHEMA_REGISTRY_URL = "http://127.0.0.1:13081"
EVENTS_SUBJECT = "seismic.events.v1-value"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "schemas"


def _registry() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


def _run_bootstrap() -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(
        {
            "SEISMIC_CLICKHOUSE_HOST": "127.0.0.1",
            "SEISMIC_CLICKHOUSE_PORT": "13123",
        }
    )
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "from seismic_stream.cli import app; app()",
            "bootstrap",
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env=environment,
    )


def _summary(output: str) -> dict[str, Any]:
    return dict(json.loads(output.strip().splitlines()[-1]))


@pytest.fixture(scope="session")
def first_bootstrap(stack: None) -> dict[str, Any]:
    completed = _run_bootstrap()
    assert completed.returncode == 0, completed.stderr
    return _summary(completed.stdout)


def test_first_run_creates_four_topics_and_subjects(first_bootstrap: dict[str, Any]) -> None:
    assert set(first_bootstrap["topics"]["created"]) == {spec.name for spec in TOPIC_SPECS}
    assert first_bootstrap["topics"]["unchanged"] == []
    assert set(first_bootstrap["subjects"]["created"]) == set(SUBJECTS)
    clickhouse = first_bootstrap["clickhouse"]
    assert len(clickhouse["applied"]) + len(clickhouse["skipped"]) == 6

    second = _run_bootstrap()
    assert second.returncode == 0, second.stderr
    summary = _summary(second.stdout)
    assert summary["topics"]["created"] == []
    assert set(summary["topics"]["unchanged"]) == {spec.name for spec in TOPIC_SPECS}
    assert summary["subjects"]["created"] == []
    assert set(summary["subjects"]["unchanged"]) == set(SUBJECTS)
    assert set(summary["compatibility"]["unchanged"]) == set(SUBJECTS)
    assert summary["clickhouse"]["applied"] == []


def test_registry_has_subjects_and_backward_compatibility(first_bootstrap: dict[str, Any]) -> None:
    client = _registry()
    assert set(client.get_subjects()) == set(SUBJECTS)
    assert client.get_compatibility(EVENTS_SUBJECT) == "BACKWARD"


def test_registry_compatibility_api(first_bootstrap: dict[str, Any]) -> None:
    admin = SchemaRegistryAdmin(_registry())
    compatible = (FIXTURES / "earthquake_event.v2_compatible.avsc").read_text(encoding="utf-8")
    breaking = (FIXTURES / "earthquake_event.v2_breaking.avsc").read_text(encoding="utf-8")
    assert admin.check_compatibility(EVENTS_SUBJECT, compatible) is True
    assert admin.check_compatibility(EVENTS_SUBJECT, breaking) is False


def test_serializer_rejects_unregistered_subject(first_bootstrap: dict[str, Any]) -> None:
    serializer = AvroSerializer(
        _registry(),
        codecs.load_schema(codecs.EVENT_SCHEMA_FILE),
        conf={"auto.register.schemas": False, "use.latest.version": True},
    )
    context = SerializationContext("seismic.unregistered.v1", MessageField.VALUE)
    with pytest.raises(SchemaRegistryError):
        serializer(codecs.event_to_dict(make_event()), context)
