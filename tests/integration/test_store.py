"""Integration checks for the ClickHouse repository (S8, S9, D10)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import clickhouse_connect
import pytest
from clickhouse_connect.driver.client import Client

from seismic_stream.adapters.storage.clickhouse import ClickHouseStore
from seismic_stream.adapters.storage.migrations import ClickHouseMigrator
from seismic_stream.application.retry import PermanentStoreError, TransientStoreError
from seismic_stream.config import Settings
from tests.support.factories import make_event

pytestmark = pytest.mark.integration

CLICKHOUSE_HOST = "127.0.0.1"
CLICKHOUSE_PORT = 13123
CLICKHOUSE_USER = "seismic"
CLICKHOUSE_PASSWORD = "seismic-local"
CLICKHOUSE_DATABASE = "seismic"

TABLES = ("events", "window_stats", "alerts", "dead_letters")


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "kafka_bootstrap": "127.0.0.1:19092",
        "schema_registry_url": "http://127.0.0.1:13081",
        "clickhouse_host": CLICKHOUSE_HOST,
        "clickhouse_port": CLICKHOUSE_PORT,
        "clickhouse_user": CLICKHOUSE_USER,
        "clickhouse_password": CLICKHOUSE_PASSWORD,
        "clickhouse_database": CLICKHOUSE_DATABASE,
        "_env_file": None,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


@pytest.fixture(scope="module")
def client(stack: None) -> Iterator[Client]:
    settings = _settings()
    ClickHouseMigrator.from_settings(settings).apply()
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
def empty_tables(client: Client) -> None:
    for table in TABLES:
        client.command(f"TRUNCATE TABLE {CLICKHOUSE_DATABASE}.{table}")


@pytest.fixture
def store(client: Client) -> ClickHouseStore:
    return ClickHouseStore(client, database=CLICKHOUSE_DATABASE)


def _count(client: Client, expression: str) -> int:
    return int(client.query(f"SELECT {expression}").result_rows[0][0])


def test_same_event_in_two_batches_is_latest_once(store: ClickHouseStore, client: Client) -> None:
    event = make_event()

    store.insert_events([event], ingested_at_ms=[1], token="batch-a")
    store.insert_events([event], ingested_at_ms=[2], token="batch-b")

    assert _count(client, "count() FROM seismic.events_latest") == 1


def test_revision_replaces_and_older_revision_arriving_later_never_wins(
    store: ClickHouseStore, client: Client
) -> None:
    base = make_event()
    newer = make_event(updated_at_ms=base.updated_at_ms + 60_000)

    store.insert_events([base], ingested_at_ms=[1], token="base")
    store.insert_events([newer], ingested_at_ms=[1], token="newer")
    store.insert_events([base], ingested_at_ms=[1], token="older-again")

    rows = client.query(
        "SELECT updated_at FROM seismic.events_latest ORDER BY updated_at"
    ).result_rows
    assert [row[0].replace(tzinfo=UTC) for row in rows] == [
        datetime.fromtimestamp(newer.updated_at_ms / 1000, tz=UTC)
    ]
    assert _count(client, "count() FROM seismic.events_latest") == 1


def test_identical_deduplication_token_inserts_no_extra_raw_rows(
    store: ClickHouseStore, client: Client
) -> None:
    event = make_event()

    store.insert_events([event], ingested_at_ms=[1], token="seismic.events.v1:0:4-4")
    assert _count(client, "count() FROM seismic.events") == 1

    store.insert_events([event], ingested_at_ms=[1], token="seismic.events.v1:0:4-4")

    assert _count(client, "count() FROM seismic.events") == 1


def test_clickhouse_rejection_is_permanent_and_wrong_port_is_transient(
    store: ClickHouseStore,
) -> None:
    with pytest.raises(PermanentStoreError):
        store.insert_events(
            [make_event(significance=2**40)],
            ingested_at_ms=[1],
            token="too-big",
        )

    with pytest.raises(TransientStoreError):
        ClickHouseStore.from_settings(_settings(clickhouse_port=1))


def test_bootstrap_cli_creates_every_table_idempotently() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    environment = os.environ.copy()
    environment.update(
        {
            "SEISMIC_KAFKA_BOOTSTRAP": "127.0.0.1:19092",
            "SEISMIC_SCHEMA_REGISTRY_URL": "http://127.0.0.1:13081",
            "SEISMIC_CLICKHOUSE_HOST": CLICKHOUSE_HOST,
            "SEISMIC_CLICKHOUSE_PORT": str(CLICKHOUSE_PORT),
            "SEISMIC_CLICKHOUSE_USER": CLICKHOUSE_USER,
            "SEISMIC_CLICKHOUSE_PASSWORD": CLICKHOUSE_PASSWORD,
            "SEISMIC_CLICKHOUSE_DATABASE": CLICKHOUSE_DATABASE,
        }
    )
    command = [sys.executable, "-c", "from seismic_stream.cli import app; app()", "bootstrap"]
    first = subprocess.run(
        command, cwd=repo_root, capture_output=True, text=True, env=environment, check=False
    )
    second = subprocess.run(
        command, cwd=repo_root, capture_output=True, text=True, env=environment, check=False
    )

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert json.loads(second.stdout)["clickhouse"]["applied"] == []
