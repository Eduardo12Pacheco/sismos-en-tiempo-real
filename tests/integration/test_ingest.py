"""Integration checks for ingest against a real Redpanda broker.

The tests run the real CLI as a subprocess (the same entry point users and the
Compose service use), then consume the topics back with the Avro codecs.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient

from seismic_stream.adapters.kafka.codecs import DeadLetterCodec, EventCodec
from seismic_stream.adapters.kafka.topics import DLQ_TOPIC, EVENTS_TOPIC
from seismic_stream.adapters.sources.chaos import ChaosPolicy, ChaoticSource
from seismic_stream.adapters.sources.replay import ReplaySource
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.models import DlqStage, Event
from seismic_stream.domain.normalize import feature_to_event

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE = REPO_ROOT / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"
KAFKA_BOOTSTRAP = "127.0.0.1:19092"
SCHEMA_REGISTRY_URL = "http://127.0.0.1:13081"
LIMIT = 200
POLL_TIMEOUT_SECONDS = 30.0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_cli(*args: str, timeout: float = 180.0) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment.update(
        {
            "SEISMIC_KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
            "SEISMIC_SCHEMA_REGISTRY_URL": SCHEMA_REGISTRY_URL,
            "SEISMIC_REPLAY_FILE": str(SAMPLE),
            "SEISMIC_METRICS_PORT": str(_free_port()),
            "SEISMIC_LOG_FORMAT": "json",
        }
    )
    return subprocess.run(
        [sys.executable, "-c", "from seismic_stream.cli import app; app()", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=environment,
        check=False,
    )


def _summary(stdout: str) -> dict[str, Any]:
    return dict(json.loads(stdout.strip().splitlines()[-1]))


def _registry() -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})


def _consume(topic: str, codec: Any) -> list[Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"seismic-test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
        }
    )
    partition = TopicPartition(topic, 0)
    try:
        _, high = consumer.get_watermark_offsets(partition, timeout=10)
        messages: list[Any] = []
        consumer.subscribe([topic])
        deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
        while len(messages) < high and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            assert not message.error(), message.error()
            assert message.key() is not None
            messages.append((message.key(), codec.deserialize(message.value())))
        assert len(messages) == high, f"expected {high} messages on {topic}, got {len(messages)}"
        return messages
    finally:
        consumer.close()


def _expected_events(records: Iterator[Any]) -> list[Event]:
    events: list[Event] = []
    for record in records:
        try:
            event = feature_to_event(record.feature)
        except EventValidationError:
            continue
        if event.event_type == "earthquake":
            events.append(event)
    return events


@pytest.fixture(scope="session")
def bootstrapped(stack: None) -> None:
    completed = _run_cli("bootstrap")
    assert completed.returncode == 0, completed.stderr


def test_ingest_limit_publishes_events_with_event_id_keys(bootstrapped: None) -> None:
    completed = _run_cli(
        "ingest",
        "--source",
        "replay",
        "--speed",
        "max",
        "--limit",
        str(LIMIT),
    )
    assert completed.returncode == 0, completed.stderr
    summary = _summary(completed.stdout)
    assert summary["published"] + summary["filtered"] + summary["invalid"] == LIMIT
    assert summary["invalid"] == 0

    expected = _expected_events(
        record for index, record in enumerate(ReplaySource(SAMPLE).records()) if index < LIMIT
    )
    assert summary["published"] == len(expected)

    messages = _consume(EVENTS_TOPIC, EventCodec(_registry()))
    assert len(messages) == summary["published"]
    for (key, event), expected_event in zip(messages, expected, strict=True):
        assert key == expected_event.event_id.encode("utf-8")
        assert event == expected_event


def test_ingest_chaos_routes_every_malformed_feature_to_the_dlq(bootstrapped: None) -> None:
    policy = ChaosPolicy(seed=42)
    plan = policy.plan(ReplaySource(SAMPLE).records())
    expected = {"published": 0, "filtered": 0, "invalid": 0}
    for record in ChaoticSource(ReplaySource(SAMPLE), policy).records():
        try:
            event = feature_to_event(record.feature)
        except EventValidationError:
            expected["invalid"] += 1
            continue
        expected["published" if event.event_type == "earthquake" else "filtered"] += 1

    completed = _run_cli(
        "ingest",
        "--source",
        "replay",
        "--speed",
        "max",
        "--chaos",
        "--seed",
        "42",
    )
    assert completed.returncode == 0, completed.stderr
    summary = _summary(completed.stdout)
    assert summary["invalid"] == plan.malformed == expected["invalid"]
    assert summary["published"] == expected["published"]
    assert summary["filtered"] == expected["filtered"]

    letters = _consume(DLQ_TOPIC, DeadLetterCodec(_registry()))
    assert len(letters) == plan.malformed
    for key, letter in letters:
        assert key == letter.dlq_id.encode("utf-8")
        assert letter.stage is DlqStage.INGEST_VALIDATION
        assert letter.payload
