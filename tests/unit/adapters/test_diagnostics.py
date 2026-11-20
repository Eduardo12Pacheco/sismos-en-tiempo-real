"""Unit tests for the doctor diagnostics adapter."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from seismic_stream.adapters import diagnostics
from seismic_stream.adapters.diagnostics import (
    EXPECTED_TABLES,
    CheckResult,
    missing_ids,
    run_doctor,
    stored_event_ids,
)
from seismic_stream.config import Settings


def _settings() -> Settings:
    return Settings(_env_file=None)


class FakeAdmin:
    def __init__(self, topics: set[str]) -> None:
        self._topics = topics

    def list_topics(self, timeout: float) -> Any:
        return SimpleNamespace(topics=dict.fromkeys(self._topics, object()))


class BrokenAdmin:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("broker unreachable")


class FakeRegistryClient:
    def __init__(self, subjects: set[str]) -> None:
        self._subjects = subjects

    def get_subjects(self) -> list[str]:
        return sorted(self._subjects)


class BrokenRegistryClient:
    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("registry unreachable")


class FakeStore:
    def __init__(self, tables: set[str] | None = None, *, ping_error: Exception | None = None):
        self.tables = EXPECTED_TABLES if tables is None else tables
        self.ping_error = ping_error
        self.closed = False

    def ping(self) -> None:
        if self.ping_error is not None:
            raise self.ping_error

    def table_names(self) -> set[str]:
        return set(self.tables)

    def event_ids(self) -> set[str]:
        return {"a"}

    def close(self) -> None:
        self.closed = True


def test_all_checks_pass_on_a_ready_stack(monkeypatch) -> None:
    monkeypatch.setattr(diagnostics, "AdminClient", lambda _cfg: FakeAdmin(_all_topics()))
    monkeypatch.setattr(
        diagnostics, "SchemaRegistryClient", lambda _cfg: FakeRegistryClient(_all_subjects())
    )
    store = FakeStore()
    monkeypatch.setattr(diagnostics.ClickHouseStore, "from_settings", lambda _s: store)

    checks = run_doctor(_settings())

    assert [check.check for check in checks] == [
        "kafka",
        "schema_registry",
        "clickhouse",
        "clickhouse_tables",
    ]
    assert all(check.ok for check in checks)
    assert store.closed is True


def test_kafka_check_reports_missing_topics(monkeypatch) -> None:
    monkeypatch.setattr(diagnostics, "AdminClient", lambda _cfg: FakeAdmin(set()))

    check = diagnostics._check_kafka(_settings())

    assert check.status == "FAIL"
    assert "missing topics" in check.detail


def test_kafka_check_captures_connection_errors(monkeypatch) -> None:
    monkeypatch.setattr(diagnostics, "AdminClient", BrokenAdmin)

    check = diagnostics._check_kafka(_settings())

    assert check.status == "FAIL"
    assert "broker unreachable" in check.detail


def test_schema_registry_check_reports_missing_subjects(monkeypatch) -> None:
    monkeypatch.setattr(diagnostics, "SchemaRegistryClient", lambda _cfg: FakeRegistryClient(set()))

    check = diagnostics._check_schema_registry(_settings())

    assert check.status == "FAIL"
    assert "missing subjects" in check.detail


def test_schema_registry_check_captures_connection_errors(monkeypatch) -> None:
    monkeypatch.setattr(diagnostics, "SchemaRegistryClient", BrokenRegistryClient)

    check = diagnostics._check_schema_registry(_settings())

    assert check.status == "FAIL"
    assert "registry unreachable" in check.detail


def test_clickhouse_connection_failure_fails_both_rows(monkeypatch) -> None:
    def broken(_settings: Settings) -> FakeStore:
        raise RuntimeError("client error")

    monkeypatch.setattr(diagnostics.ClickHouseStore, "from_settings", broken)

    server, tables = diagnostics._check_clickhouse(_settings())

    assert server.status == "FAIL"
    assert tables.status == "FAIL"


def test_clickhouse_ping_failure_closes_the_client(monkeypatch) -> None:
    store = FakeStore(ping_error=RuntimeError("ping failed"))
    monkeypatch.setattr(diagnostics.ClickHouseStore, "from_settings", lambda _s: store)

    server, tables = diagnostics._check_clickhouse(_settings())

    assert server.status == "FAIL"
    assert tables.status == "FAIL"
    assert store.closed is True


def test_clickhouse_missing_tables_are_reported_but_ping_passes(monkeypatch) -> None:
    store = FakeStore(tables={"events"})
    monkeypatch.setattr(diagnostics.ClickHouseStore, "from_settings", lambda _s: store)

    server, tables = diagnostics._check_clickhouse(_settings())

    assert server.status == "PASS"
    assert tables.status == "FAIL"
    assert "missing tables" in tables.detail


def test_clickhouse_table_query_failure_is_reported(monkeypatch) -> None:
    class FlakyStore(FakeStore):
        def table_names(self) -> set[str]:
            raise RuntimeError("query failed")

    store = FlakyStore()
    monkeypatch.setattr(diagnostics.ClickHouseStore, "from_settings", lambda _s: store)

    server, tables = diagnostics._check_clickhouse(_settings())

    assert server.status == "PASS"
    assert tables.status == "FAIL"
    assert "query failed" in tables.detail


def test_stored_event_ids_reads_and_closes_the_store(monkeypatch) -> None:
    store = FakeStore()
    monkeypatch.setattr(diagnostics.ClickHouseStore, "from_settings", lambda _s: store)

    assert stored_event_ids(_settings()) == {"a"}
    assert store.closed is True


def test_missing_ids_are_sorted_and_deduplicated() -> None:
    assert missing_ids(["b", "a", "b"], ["a"]) == ["b"]


def test_check_result_ok_property() -> None:
    assert CheckResult("kafka", "PASS", "").ok is True
    assert CheckResult("kafka", "FAIL", "").ok is False


def _all_topics() -> set[str]:
    return {spec.name for spec in diagnostics.TOPIC_SPECS}


def _all_subjects() -> set[str]:
    return set(diagnostics.SUBJECTS)
