"""Unit tests for ClickHouse migrations, error mapping and row encoding."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from clickhouse_connect.driver import exceptions
from clickhouse_connect.driver.client import Client

from seismic_stream.adapters.storage.clickhouse import (
    ALERT_COLUMNS,
    DEAD_LETTER_COLUMNS,
    EVENT_COLUMNS,
    WINDOW_STATS_COLUMNS,
    ClickHouseStore,
)
from seismic_stream.adapters.storage.migrations import (
    ClickHouseMigrator,
    classify_clickhouse_error,
    load_migration,
)
from seismic_stream.application.retry import PermanentStoreError, TransientStoreError
from seismic_stream.domain.models import Alert, AlertLevel, DeadLetter, DlqStage, WindowStats
from tests.support.factories import make_event
from tests.support.fakes import FakeClock


@dataclass
class FakeResult:
    result_rows: list[tuple[str]]


@dataclass
class FakeClient:
    existing: set[str] = field(default_factory=set)
    commands: list[str] = field(default_factory=list)
    inserted: list[tuple[str, list[list[Any]], list[str], dict[str, Any]]] = field(
        default_factory=list
    )

    def command(self, cmd: str, parameters: object = None) -> str:
        self.commands.append(cmd)
        return ""

    def query(self, query: str) -> FakeResult:
        return FakeResult(result_rows=[(version,) for version in sorted(self.existing)])

    def insert(
        self,
        table: str,
        data: Sequence[Sequence[Any]],
        column_names: Sequence[str],
        settings: dict[str, Any] | None = None,
    ) -> None:
        self.inserted.append(
            (table, [list(row) for row in data], list(column_names), settings or {})
        )
        if table.endswith("schema_migrations"):
            self.existing.update(str(row[0]) for row in data)


def _migrator(client: FakeClient) -> ClickHouseMigrator:
    return ClickHouseMigrator(cast(Client, client))


def test_first_migration_run_applies_every_file_in_order() -> None:
    client = FakeClient()

    result = _migrator(client).apply()

    assert result.applied == (
        "001_database",
        "002_events",
        "003_window_stats",
        "004_alerts",
        "005_dead_letters",
        "006_views",
    )
    assert result.skipped == ()
    assert client.commands[0] == "CREATE DATABASE IF NOT EXISTS seismic"
    assert any("ReplacingMergeTree(updated_at)" in cmd for cmd in client.commands)
    assert any("non_replicated_deduplication_window = 1000" in cmd for cmd in client.commands)
    assert any("events_latest" in cmd for cmd in client.commands)
    assert any("data_clock" in cmd for cmd in client.commands)
    assert [row[0] for _table, rows, _cols, _settings in client.inserted for row in rows] == [
        "001_database",
        "002_events",
        "003_window_stats",
        "004_alerts",
        "005_dead_letters",
        "006_views",
    ]


def test_second_migration_run_is_a_noop() -> None:
    client = FakeClient()
    _migrator(client).apply()
    commands_before = len(client.commands)
    inserts_before = len(client.inserted)

    result = _migrator(client).apply()

    assert result.applied == ()
    assert len(result.skipped) == 6
    # Only the idempotent database/ledger setup runs again; no DDL migration.
    assert len(client.commands) == commands_before + 2
    assert len(client.inserted) == inserts_before


def test_migration_files_split_statements_and_strip_comments() -> None:
    statements = load_migration("006_views.sql")

    assert len(statements) == 5
    assert all(statement.startswith("CREATE VIEW") for statement in statements)
    assert all("--" not in statement for statement in statements)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (exceptions.DatabaseError("bad row", code=60), PermanentStoreError),
        (exceptions.DatabaseError("timeout", code=159), TransientStoreError),
        (exceptions.DatabaseError("too many parts", code=252), TransientStoreError),
        (exceptions.OperationalError("connection refused"), TransientStoreError),
        (exceptions.InterfaceError("closed"), TransientStoreError),
        (ConnectionError("boom"), TransientStoreError),
        (TimeoutError("slow"), TransientStoreError),
        (ValueError("bad datetime"), PermanentStoreError),
        (RuntimeError("unknown"), TransientStoreError),
    ],
)
def test_driver_errors_follow_the_transient_permanent_taxonomy(
    error: BaseException, expected: type[Exception]
) -> None:
    assert isinstance(classify_clickhouse_error(error), expected)


def test_store_inserts_events_with_datetime_columns_headers_and_token() -> None:
    client = FakeClient()
    store = ClickHouseStore(cast(Client, client), clock=FakeClock(now=1_700_000_000.0))
    event = make_event()

    store.insert_events([event], ingested_at_ms=[1_700_000_000_500], token="events:0:1-1")

    table, rows, columns, settings = client.inserted[0]
    assert table == "seismic.events"
    assert columns == list(EVENT_COLUMNS)
    assert settings == {"insert_deduplication_token": "events:0:1-1"}
    assert rows[0][0] == event.event_id
    assert rows[0][3] == datetime.fromtimestamp(event.event_time_ms / 1000, tz=UTC)
    assert rows[0][18] == datetime.fromtimestamp(1_700_000_000.5, tz=UTC)
    assert rows[0][-1] == datetime.fromtimestamp(1_700_000_000.0, tz=UTC)


def test_store_inserts_every_other_table_with_its_columns() -> None:
    client = FakeClient()
    store = ClickHouseStore(cast(Client, client), clock=FakeClock(now=0.0))
    stats = WindowStats(
        region="CA",
        window_start_ms=1_000,
        window_end_ms=2_000,
        event_count=2,
        max_magnitude=4.5,
        mean_magnitude=4.0,
        mean_depth_km=8.0,
        total_energy_joules=1.0,
        emitted_at_ms=2_000,
    )
    alert = Alert(
        event_id="e1",
        level=AlertLevel.WARNING,
        reason="tsunami flag set",
        magnitude=6.1,
        place="CA",
        event_time_ms=1_000,
        updated_at_ms=2_000,
        tsunami=True,
        in_ecuador_region=False,
    )
    letter = DeadLetter(
        dlq_id="a" * 32,
        stage=DlqStage.SINK_DECODE,
        error_type="ValueError",
        error_message="bad bytes",
        source_topic="seismic.events.v1",
        source_partition=0,
        source_offset=9,
        payload=b"\x00raw",
        failed_at_ms=3_000,
        attempts=1,
    )

    store.insert_window_stats([stats], token="stats")
    store.insert_alerts([alert], token="alerts")
    store.insert_dead_letters([letter], token="dlq")

    assert [entry[0] for entry in client.inserted] == [
        "seismic.window_stats",
        "seismic.alerts",
        "seismic.dead_letters",
    ]
    assert client.inserted[0][2] == list(WINDOW_STATS_COLUMNS)
    assert client.inserted[1][2] == list(ALERT_COLUMNS)
    assert client.inserted[2][2] == list(DEAD_LETTER_COLUMNS)
    assert client.inserted[2][1][0][7] == b"\x00raw"


def test_empty_insert_is_not_sent_to_clickhouse() -> None:
    client = FakeClient()
    store = ClickHouseStore(cast(Client, client))

    store.insert_events([], ingested_at_ms=[], token="empty")

    assert client.inserted == []


def test_mismatched_event_and_header_lengths_are_rejected() -> None:
    store = ClickHouseStore(cast(Client, FakeClient()))

    with pytest.raises(ValueError, match="same length"):
        store.insert_events([make_event()], ingested_at_ms=[], token="bad")
