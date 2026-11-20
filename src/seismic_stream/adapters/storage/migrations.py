"""Ordered, idempotent ClickHouse DDL migrations recorded in ``schema_migrations``.

The SQL files bundle with the package (``adapters/storage/ddl``) so the wheel and
the tests apply exactly the same statements. A migration is recorded only after
all of its statements succeed; every statement is ``IF NOT EXISTS`` so a crash
mid-file can be retried safely.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from importlib.resources import files
from typing import Any, Final, Protocol, cast

import clickhouse_connect
from clickhouse_connect.driver import exceptions
from clickhouse_connect.driver.client import Client

from seismic_stream.application.retry import PermanentStoreError, TransientStoreError
from seismic_stream.config import Settings

DDL_PACKAGE: Final = "seismic_stream.adapters.storage.ddl"
MIGRATION_FILES: Final = (
    "001_database.sql",
    "002_events.sql",
    "003_window_stats.sql",
    "004_alerts.sql",
    "005_dead_letters.sql",
    "006_views.sql",
)
TRANSIENT_CLICKHOUSE_CODES: Final = frozenset({159, 202, 209, 210, 241, 252})


MigrationClient = Client


class MigrationError(RuntimeError):
    """Raised when a DDL migration cannot be applied."""


@dataclass(frozen=True, slots=True)
class MigrationResult:
    """Applied and already-present migration versions."""

    applied: tuple[str, ...]
    skipped: tuple[str, ...]

    def as_dict(self) -> dict[str, list[str]]:
        """Render the stable bootstrap JSON shape."""
        return {"applied": list(self.applied), "skipped": list(self.skipped)}


def load_migration(filename: str) -> tuple[str, ...]:
    """Return the executable statements of one bundled DDL file."""
    text = files(DDL_PACKAGE).joinpath(filename).read_text(encoding="utf-8")
    without_comments = "\n".join(
        line for line in text.splitlines() if not line.strip().startswith("--")
    )
    return tuple(
        statement.strip() for statement in without_comments.split(";") if statement.strip()
    )


def classify_clickhouse_error(error: BaseException) -> TransientStoreError | PermanentStoreError:
    """Map driver exceptions to the D10 taxonomy used by the sink."""
    if isinstance(error, (TransientStoreError, PermanentStoreError)):
        return error
    if isinstance(
        error,
        (
            OSError,
            TimeoutError,
            exceptions.InterfaceError,
            exceptions.OperationalError,
            exceptions.StreamFailureError,
        ),
    ):
        return TransientStoreError(str(error))
    if isinstance(error, exceptions.Error):
        if error.code in TRANSIENT_CLICKHOUSE_CODES:
            return TransientStoreError(str(error))
        return PermanentStoreError(str(error))
    if isinstance(error, (ValueError, TypeError, OverflowError)):
        return PermanentStoreError(str(error))
    return TransientStoreError(str(error))


class ClickHouseMigrator:
    """Apply the bundled DDL files in order and record their versions."""

    def __init__(self, client: MigrationClient, *, database: str = "seismic") -> None:
        self._client = client
        self._database = database

    @classmethod
    def from_settings(cls, settings: Settings) -> ClickHouseMigrator:
        """Build a migrator connected to the configured ClickHouse server."""
        try:
            client = clickhouse_connect.get_client(
                host=settings.clickhouse_host,
                port=settings.clickhouse_port,
                username=settings.clickhouse_user,
                password=settings.clickhouse_password,
            )
        except Exception as error:  # surface the documented typed taxonomy
            raise classify_clickhouse_error(error) from error
        return cls(client, database=settings.clickhouse_database)

    def apply(self) -> MigrationResult:
        """Create the database, apply pending files and record each version."""
        try:
            self._client.command(f"CREATE DATABASE IF NOT EXISTS {self._database}")
            self._client.command(
                f"CREATE TABLE IF NOT EXISTS {self._database}.schema_migrations "
                "(version String, applied_at DateTime64(3, 'UTC')) "
                "ENGINE = MergeTree ORDER BY version"
            )
            existing = _existing_versions(self._client, self._database)
            applied: list[str] = []
            skipped: list[str] = []
            for filename in MIGRATION_FILES:
                version = filename.removesuffix(".sql")
                if version in existing:
                    skipped.append(version)
                    continue
                for statement in load_migration(filename):
                    self._client.command(statement)
                self._client.insert(
                    f"{self._database}.schema_migrations",
                    [[version]],
                    column_names=["version"],
                )
                applied.append(version)
        except Exception as error:
            raise classify_clickhouse_error(error) from error
        return MigrationResult(applied=tuple(applied), skipped=tuple(skipped))


class StorageMigrator(Protocol):
    """Bootstrap boundary so unit tests can inject a fake migrator."""

    def apply(self) -> MigrationResult:
        """Apply pending migrations."""
        ...


def _existing_versions(client: MigrationClient, database: str) -> set[str]:
    result = client.query(f"SELECT version FROM {database}.schema_migrations")
    rows = cast(Iterable[Sequence[Any]], result.result_rows)
    return {str(row[0]) for row in rows}
