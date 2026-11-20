"""Read-only diagnostics used by the ``doctor`` and ``verify`` commands.

These checks cross the three infrastructures the pipeline depends on (Kafka,
Schema Registry and ClickHouse), so they live in an adapter rather than in the
pure application layer. Every failure is captured as a ``FAIL`` row with a short
reason instead of crashing the CLI, which is what an operator needs when the
stack is only partially healthy.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Final, Literal

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient

from seismic_stream.adapters.kafka.topics import SUBJECTS, TOPIC_SPECS
from seismic_stream.adapters.storage.clickhouse import ClickHouseStore
from seismic_stream.config import Settings

TIMEOUT_SECONDS: Final = 10.0

EXPECTED_TABLES: Final = frozenset(
    {
        "alerts",
        "alerts_latest",
        "data_clock",
        "dead_letters",
        "dead_letters_latest",
        "events",
        "events_latest",
        "schema_migrations",
        "window_stats",
        "window_stats_latest",
    }
)

CheckStatus = Literal["PASS", "FAIL"]


@dataclass(frozen=True, slots=True)
class CheckResult:
    """One doctor check and the human-readable detail shown in the table."""

    check: str
    status: CheckStatus
    detail: str

    @property
    def ok(self) -> bool:
        """Whether this check passed."""
        return self.status == "PASS"


def _pass(check: str, detail: str) -> CheckResult:
    return CheckResult(check=check, status="PASS", detail=detail)


def _fail(check: str, detail: str) -> CheckResult:
    return CheckResult(check=check, status="FAIL", detail=detail)


def _check_kafka(settings: Settings) -> CheckResult:
    try:
        admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
        topics = set(admin.list_topics(timeout=TIMEOUT_SECONDS).topics)
    except Exception as error:
        return _fail("kafka", f"{type(error).__name__}: {error}")
    missing = [spec.name for spec in TOPIC_SPECS if spec.name not in topics]
    if missing:
        return _fail("kafka", f"missing topics: {', '.join(missing)}")
    return _pass("kafka", f"broker reachable, {len(TOPIC_SPECS)}/{len(TOPIC_SPECS)} topics")


def _check_schema_registry(settings: Settings) -> CheckResult:
    try:
        client = SchemaRegistryClient({"url": settings.schema_registry_url})
        subjects = set(client.get_subjects())
    except Exception as error:
        return _fail("schema_registry", f"{type(error).__name__}: {error}")
    missing = [subject for subject in SUBJECTS if subject not in subjects]
    if missing:
        return _fail("schema_registry", f"missing subjects: {', '.join(missing)}")
    return _pass("schema_registry", f"{len(SUBJECTS)}/{len(SUBJECTS)} subjects registered")


def _check_clickhouse(settings: Settings) -> tuple[CheckResult, CheckResult]:
    try:
        store = ClickHouseStore.from_settings(settings)
    except Exception as error:
        detail = f"{type(error).__name__}: {error}"
        return _fail("clickhouse", detail), _fail("clickhouse_tables", "server unavailable")
    try:
        store.ping()
    except Exception as error:
        store.close()
        return (
            _fail("clickhouse", f"{type(error).__name__}: {error}"),
            _fail("clickhouse_tables", "server unavailable"),
        )
    try:
        tables = store.table_names()
    except Exception as error:
        return _pass("clickhouse", "ping ok"), _fail(
            "clickhouse_tables", f"{type(error).__name__}: {error}"
        )
    finally:
        store.close()
    missing = sorted(EXPECTED_TABLES - tables)
    if missing:
        return _pass("clickhouse", "ping ok"), _fail(
            "clickhouse_tables", f"missing tables: {', '.join(missing)}"
        )
    return _pass("clickhouse", "ping ok"), _pass(
        "clickhouse_tables", f"{len(EXPECTED_TABLES)}/{len(EXPECTED_TABLES)} tables"
    )


def run_doctor(settings: Settings) -> tuple[CheckResult, ...]:
    """Run every readiness check and return the rows in display order."""
    clickhouse, tables = _check_clickhouse(settings)
    return (
        _check_kafka(settings),
        _check_schema_registry(settings),
        clickhouse,
        tables,
    )


def stored_event_ids(settings: Settings) -> set[str]:
    """Read the deduplicated event id set from ``events_latest``."""
    store = ClickHouseStore.from_settings(settings)
    try:
        return store.event_ids()
    finally:
        store.close()


def missing_ids(expected: Iterable[str], stored: Iterable[str]) -> list[str]:
    """Return the sorted expected ids that are not stored yet."""
    stored_set = set(stored)
    return sorted(set(expected) - stored_set)
