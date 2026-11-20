"""ClickHouse repository implementing the sink's :class:`EventStore` port.

Every insert carries an explicit ``insert_deduplication_token`` (S9) so a
retried Kafka batch cannot create duplicate raw rows, and every table is a
``ReplacingMergeTree`` versioned by a domain column (D7). Driver exceptions are
translated into the D10 transient/permanent taxonomy before leaving the adapter.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Final

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from seismic_stream.adapters.clock import SystemClock
from seismic_stream.adapters.storage.migrations import classify_clickhouse_error
from seismic_stream.application.ports import Clock
from seismic_stream.config import Settings
from seismic_stream.domain.models import Alert, DeadLetter, Event, WindowStats

EVENT_COLUMNS: Final = (
    "event_id",
    "source",
    "event_type",
    "event_time",
    "updated_at",
    "magnitude",
    "magnitude_type",
    "place",
    "region",
    "latitude",
    "longitude",
    "depth_km",
    "status",
    "tsunami",
    "significance",
    "network",
    "detail_url",
    "in_ecuador_region",
    "ingested_at",
    "stored_at",
)
WINDOW_STATS_COLUMNS: Final = (
    "region",
    "window_start",
    "window_end",
    "event_count",
    "max_magnitude",
    "mean_magnitude",
    "mean_depth_km",
    "total_energy_joules",
    "emitted_at",
    "stored_at",
)
ALERT_COLUMNS: Final = (
    "event_id",
    "level",
    "reason",
    "magnitude",
    "place",
    "event_time",
    "updated_at",
    "tsunami",
    "in_ecuador_region",
    "stored_at",
)
DEAD_LETTER_COLUMNS: Final = (
    "dlq_id",
    "stage",
    "error_type",
    "error_message",
    "source_topic",
    "source_partition",
    "source_offset",
    "payload",
    "failed_at",
    "attempts",
    "stored_at",
)


class ClickHouseStore:
    """Typed repository over one ClickHouse client and database."""

    def __init__(
        self,
        client: Client,
        *,
        database: str = "seismic",
        clock: Clock | None = None,
    ) -> None:
        self._client = client
        self._database = database
        self._clock = clock or SystemClock()

    @classmethod
    def from_settings(
        cls,
        settings: Settings,
        *,
        clock: Clock | None = None,
    ) -> ClickHouseStore:
        """Open a client for the configured ClickHouse database."""
        try:
            client = clickhouse_connect.get_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
                database=settings.clickhouse_database,
            )
        except Exception as error:
            raise classify_clickhouse_error(error) from error
        return cls(client, database=settings.clickhouse_database, clock=clock)

    def insert_events(
        self,
        events: Sequence[Event],
        *,
        ingested_at_ms: Sequence[int | None],
        token: str,
    ) -> None:
        """Insert events and their ingest headers with a deduplication token."""
        if len(events) != len(ingested_at_ms):
            raise ValueError("events and ingested_at_ms must have the same length")
        stored_at = self._now()
        rows = [
            [
                event.event_id,
                event.source,
                event.event_type,
                _dt(event.event_time_ms),
                _dt(event.updated_at_ms),
                event.magnitude,
                event.magnitude_type,
                event.place,
                event.region,
                event.latitude,
                event.longitude,
                event.depth_km,
                event.status,
                event.tsunami,
                event.significance,
                event.network,
                event.detail_url,
                event.in_ecuador_region,
                _dt(ingested) if ingested is not None else None,
                stored_at,
            ]
            for event, ingested in zip(events, ingested_at_ms, strict=True)
        ]
        self._insert("events", rows, EVENT_COLUMNS, token)

    def insert_window_stats(self, rows: Sequence[WindowStats], *, token: str) -> None:
        """Insert aggregated window updates."""
        stored_at = self._now()
        data = [
            [
                row.region,
                _dt(row.window_start_ms),
                _dt(row.window_end_ms),
                row.event_count,
                row.max_magnitude,
                row.mean_magnitude,
                row.mean_depth_km,
                row.total_energy_joules,
                _dt(row.emitted_at_ms),
                stored_at,
            ]
            for row in rows
        ]
        self._insert("window_stats", data, WINDOW_STATS_COLUMNS, token)

    def insert_alerts(self, rows: Sequence[Alert], *, token: str) -> None:
        """Insert alert updates."""
        stored_at = self._now()
        data = [
            [
                row.event_id,
                row.level.value,
                row.reason,
                row.magnitude,
                row.place,
                _dt(row.event_time_ms),
                _dt(row.updated_at_ms),
                row.tsunami,
                row.in_ecuador_region,
                stored_at,
            ]
            for row in rows
        ]
        self._insert("alerts", data, ALERT_COLUMNS, token)

    def insert_dead_letters(self, rows: Sequence[DeadLetter], *, token: str) -> None:
        """Insert dead-letter envelopes, preserving the raw payload bytes."""
        stored_at = self._now()
        data = [
            [
                row.dlq_id,
                row.stage.value,
                row.error_type,
                row.error_message,
                row.source_topic,
                row.source_partition,
                row.source_offset,
                row.payload,
                _dt(row.failed_at_ms),
                row.attempts,
                stored_at,
            ]
            for row in rows
        ]
        self._insert("dead_letters", data, DEAD_LETTER_COLUMNS, token)

    def _insert(
        self,
        table: str,
        rows: Sequence[Sequence[object]],
        columns: Sequence[str],
        token: str,
    ) -> None:
        if not rows:
            return
        try:
            self._client.insert(
                f"{self._database}.{table}",
                list(rows),
                column_names=list(columns),
                settings={"insert_deduplication_token": token},
            )
        except Exception as error:
            raise classify_clickhouse_error(error) from error

    def _now(self) -> datetime:
        return datetime.fromtimestamp(self._clock.now(), tz=UTC)


def _dt(milliseconds: int) -> datetime:
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)
