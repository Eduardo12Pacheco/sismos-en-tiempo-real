"""Integration checks for the sink service end to end (S8, S9, S10)."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from typing import Any

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

from seismic_stream.adapters.kafka.topics import EVENTS_TOPIC
from seismic_stream.adapters.sources.replay import ReplaySource
from seismic_stream.adapters.storage.migrations import ClickHouseMigrator
from seismic_stream.config import Settings
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.normalize import feature_to_event

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE = REPO_ROOT / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"
KAFKA_BOOTSTRAP = "127.0.0.1:19092"
SCHEMA_REGISTRY_URL = "http://127.0.0.1:13081"
CLICKHOUSE_HOST = "127.0.0.1"
CLICKHOUSE_PORT = 13123
CLICKHOUSE_USER = "seismic"
CLICKHOUSE_PASSWORD = "seismic-local"
CLICKHOUSE_DATABASE = "seismic"
SINK_IDLE_TIMEOUT = 8.0
TABLES = ("events", "window_stats", "alerts", "dead_letters")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _environment(metrics_port: int) -> dict[str, str]:
    environment = os.environ.copy()
    environment.update(
        {
            "SEISMIC_KAFKA_BOOTSTRAP": KAFKA_BOOTSTRAP,
            "SEISMIC_SCHEMA_REGISTRY_URL": SCHEMA_REGISTRY_URL,
            "SEISMIC_CLICKHOUSE_HOST": CLICKHOUSE_HOST,
            "SEISMIC_CLICKHOUSE_PORT": str(CLICKHOUSE_PORT),
            "SEISMIC_CLICKHOUSE_USER": CLICKHOUSE_USER,
            "SEISMIC_CLICKHOUSE_PASSWORD": CLICKHOUSE_PASSWORD,
            "SEISMIC_CLICKHOUSE_DATABASE": CLICKHOUSE_DATABASE,
            "SEISMIC_METRICS_PORT": str(metrics_port),
            "SEISMIC_LOG_FORMAT": "json",
        }
    )
    return environment


def _run_cli(*args: str, timeout: float = 300.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", "from seismic_stream.cli import app; app()", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=_environment(_free_port()),
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
            with suppress(Exception):
                future.result(timeout=30)


@pytest.fixture(scope="module")
def client(stack: None) -> Iterator[Client]:
    ClickHouseMigrator.from_settings(
        Settings(
            clickhouse_host=CLICKHOUSE_HOST,
            clickhouse_port=CLICKHOUSE_PORT,
            clickhouse_user=CLICKHOUSE_USER,
            clickhouse_password=CLICKHOUSE_PASSWORD,
            clickhouse_database=CLICKHOUSE_DATABASE,
            _env_file=None,
        )
    ).apply()
    connection = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )
    try:
        yield connection
    finally:
        connection.close()


@pytest.fixture(autouse=True)
def clean_pipeline(client: Client) -> None:
    _delete_all_topics_and_groups()
    for table in TABLES:
        client.command(f"TRUNCATE TABLE {CLICKHOUSE_DATABASE}.{table}")
    bootstrapped = _run_cli("bootstrap")
    assert bootstrapped.returncode == 0, bootstrapped.stderr


def _earthquake_ids() -> set[str]:
    ids: set[str] = set()
    for record in ReplaySource(SAMPLE).records():
        try:
            event = feature_to_event(record.feature)
        except EventValidationError:
            continue
        if event.event_type == "earthquake":
            ids.add(event.event_id)
    return ids


def _ingest() -> None:
    result = _run_cli("ingest", "--source", "replay", "--speed", "max")
    assert result.returncode == 0, result.stderr


def _sink() -> dict[str, Any]:
    result = _run_cli("sink", "--timeout", str(SINK_IDLE_TIMEOUT), timeout=300)
    assert result.returncode == 0, result.stderr + result.stdout
    return json.loads(result.stdout)


def _latest_event_ids(client: Client) -> set[str]:
    rows = client.query("SELECT event_id FROM seismic.events_latest").result_rows
    return {str(row[0]) for row in rows}


def test_clean_replay_stores_every_earthquake_and_is_idempotent(client: Client) -> None:
    expected = _earthquake_ids()
    _ingest()

    first = _sink()

    assert _latest_event_ids(client) == expected
    assert first["rows"] == len(expected)
    latest_count = int(client.query("SELECT count() FROM seismic.events_latest").result_rows[0][0])

    _ingest()
    _sink()

    assert _latest_event_ids(client) == expected
    assert (
        int(client.query("SELECT count() FROM seismic.events_latest").result_rows[0][0])
        == latest_count
    )
    raw_count = int(client.query("SELECT count() FROM seismic.events").result_rows[0][0])
    assert raw_count > latest_count


def test_undecodable_bytes_produce_one_sink_decode_dead_letter(client: Client) -> None:
    garbage = b"\x00not-an-avro-payload"
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    producer.produce(EVENTS_TOPIC, key=b"garbage", value=garbage)
    assert producer.flush(15) == 0

    _sink()

    rows = client.query(
        "SELECT stage, payload FROM seismic.dead_letters_latest WHERE stage = 'sink_decode'"
    ).result_rows
    assert len(rows) == 1
    payload = rows[0][1]
    if isinstance(payload, str):
        payload = payload.encode()
    assert payload == garbage
