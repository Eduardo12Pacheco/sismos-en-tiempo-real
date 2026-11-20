"""Integration checks for the Quix Streams processor against Redpanda.

Each test starts from empty topics so the processor consumes exactly the data it
produced and its aggregates can be compared with the independent reference.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient

from seismic_stream.adapters.kafka.codecs import AlertCodec, WindowStatsCodec
from seismic_stream.adapters.kafka.topics import AGGREGATES_TOPIC, ALERTS_TOPIC
from tests.support.reference import reference_result

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE = REPO_ROOT / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"
KAFKA_BOOTSTRAP = "127.0.0.1:19092"
SCHEMA_REGISTRY_URL = "http://127.0.0.1:13081"
PROCESSOR_IDLE_TIMEOUT = 15.0
POLL_TIMEOUT_SECONDS = 60.0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _environment(metrics_port: int, state_dir: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "SEISMIC_KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
            "SEISMIC_SCHEMA_REGISTRY_URL": SCHEMA_REGISTRY_URL,
            "SEISMIC_REPLAY_FILE": str(SAMPLE),
            "SEISMIC_METRICS_PORT": str(metrics_port),
            "SEISMIC_STATE_DIR": str(state_dir),
            "SEISMIC_LOG_FORMAT": "json",
        }
    )
    return environment


def _cli_command(*args: str) -> list[str]:
    return [sys.executable, "-c", "from seismic_stream.cli import app; app()", *args]


def _run_cli(
    *args: str,
    metrics_port: int,
    state_dir: Path,
    timeout: float = 300.0,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        _cli_command(*args),
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_environment(metrics_port, state_dir),
        check=False,
    )


def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})


def _delete_all_topics_and_groups() -> None:
    admin = _admin()
    metadata = admin.list_topics(timeout=15)
    topics = [
        name
        for name in metadata.topics
        if name.startswith("seismic.") or name.startswith("changelog__")
    ]
    if topics:
        for future in admin.delete_topics(topics, operation_timeout=30).values():
            future.result(timeout=30)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        remaining = [
            name
            for name in admin.list_topics(timeout=15).topics
            if name.startswith("seismic.") or name.startswith("changelog__")
        ]
        if not remaining:
            break
        time.sleep(0.5)
    else:
        raise AssertionError(f"topics were not deleted in time: {remaining}")

    groups = admin.list_consumer_groups(request_timeout=15).result().valid
    group_ids = [group.group_id for group in groups if group.group_id.startswith("seismic")]
    if group_ids:
        for future in admin.delete_consumer_groups(group_ids, request_timeout=30).values():
            # Already empty or concurrently deleted groups are fine.
            with suppress(Exception):
                future.result(timeout=30)


@pytest.fixture(autouse=True)
def empty_topics(stack: None) -> None:
    _delete_all_topics_and_groups()
    result = _run_cli("bootstrap", metrics_port=_free_port(), state_dir=Path("/tmp"))
    assert result.returncode == 0, result.stderr


def _consume(topic: str, codec: Any) -> list[tuple[bytes, Any]]:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": f"seismic-test-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    partition = TopicPartition(topic, 0)
    try:
        _, high = consumer.get_watermark_offsets(partition, timeout=10)
        messages: list[tuple[bytes, Any]] = []
        consumer.subscribe([topic])
        deadline = time.monotonic() + POLL_TIMEOUT_SECONDS
        while len(messages) < high and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            assert not message.error(), message.error()
            key = message.key()
            assert key is not None
            messages.append((key, codec.deserialize(message.value())))
        assert len(messages) == high, f"expected {high} messages on {topic}, got {len(messages)}"
        return messages
    finally:
        consumer.close()


def _ingest_and_process(tmp_path: Path, *, chaos: bool = False) -> None:
    ingest_args = ["ingest", "--source", "replay", "--speed", "max"]
    if chaos:
        ingest_args.extend(["--chaos", "--seed", "42"])
    ingested = _run_cli(
        *ingest_args,
        metrics_port=_free_port(),
        state_dir=tmp_path / "ingest-state",
    )
    assert ingested.returncode == 0, ingested.stderr
    processed = _run_cli(
        "process",
        "--timeout",
        str(PROCESSOR_IDLE_TIMEOUT),
        metrics_port=_free_port(),
        state_dir=tmp_path / "processor-state",
        timeout=300,
    )
    assert processed.returncode == 0, processed.stderr + processed.stdout


def _assert_aggregates_match_reference(reference: Any) -> None:
    messages = _consume(
        AGGREGATES_TOPIC, WindowStatsCodec(SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL}))
    )
    final: dict[tuple[str, int], Any] = {}
    for _key, stats in messages:
        final[(stats.region, stats.window_start_ms)] = stats

    assert set(final) == set(reference.windows)
    for key, expected in reference.windows.items():
        actual = final[key]
        assert actual.window_end_ms == expected.window_end_ms
        assert actual.event_count == expected.event_count
        assert actual.max_magnitude == expected.max_magnitude
        assert actual.mean_magnitude == pytest.approx(expected.mean_magnitude, rel=1e-9, abs=1e-12)
        assert actual.mean_depth_km == pytest.approx(expected.mean_depth_km, rel=1e-9, abs=1e-12)
        assert actual.total_energy_joules == pytest.approx(expected.total_energy_joules, rel=1e-9)


def _assert_alerts_match_reference(reference: Any) -> None:
    messages = _consume(
        ALERTS_TOPIC, AlertCodec(SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL}))
    )
    final: dict[str, Any] = {}
    for _key, alert in messages:
        final[alert.event_id] = alert

    assert set(final) == set(reference.alerts)
    for event_id, expected in reference.alerts.items():
        actual = final[event_id]
        assert actual.level is expected.level
        assert actual.reason == expected.reason
        assert actual.updated_at_ms == expected.updated_at_ms
        assert actual.magnitude == pytest.approx(expected.magnitude)


def test_clean_replay_aggregates_and_alerts_match_the_reference(tmp_path: Path) -> None:
    _ingest_and_process(tmp_path)
    reference = reference_result()

    _assert_aggregates_match_reference(reference)
    _assert_alerts_match_reference(reference)
