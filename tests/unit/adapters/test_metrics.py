"""Tests for the metric catalog and the Prometheus adapter."""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from seismic_stream.adapters.observability.metrics import (
    METRIC_CATALOG,
    SERVICE_METRICS,
    PrometheusMetrics,
)
from seismic_stream.application.ports import Metrics


def _text(registry: CollectorRegistry) -> str:
    return generate_latest(registry).decode()


def _exercise(metrics: PrometheusMetrics) -> None:
    metrics.set_build_info("sink", "0.1.0")
    metrics.record_ingest("replay", "published")
    metrics.record_ingest("replay", "published")
    metrics.record_ingest("replay", "invalid")
    metrics.record_backpressure_wait()
    metrics.observe_replay_schedule_lag(0.25)
    metrics.set_replay_progress(0.5)
    metrics.record_processor_event("new")
    metrics.record_processor_event("revision")
    metrics.record_processor_event("duplicate")
    metrics.record_processor_alert("WARNING")
    metrics.record_window_update()
    metrics.record_processor_skipped("undecodable")
    metrics.record_sink_rows("events", 3)
    metrics.record_sink_batch("events", "ok")
    metrics.observe_sink_insert_seconds("events", 0.125)
    metrics.record_sink_retry()
    metrics.set_sink_paused(True)
    metrics.observe_end_to_end_seconds(1.5)
    metrics.record_dlq_message("sink_decode", "DecodeError")
    metrics.set_consumer_lag("seismic-sink", "seismic.events.v1", 4)
    metrics.set_topic_end_offset("seismic.events.v1", 42)


def test_metric_catalog_entries_are_complete_and_unique() -> None:
    names = [spec.name for spec in METRIC_CATALOG]

    assert len(names) == len(set(names))
    assert all(spec.help_text.strip() for spec in METRIC_CATALOG)
    assert all(isinstance(spec.labels, tuple) for spec in METRIC_CATALOG)
    assert {spec.type for spec in METRIC_CATALOG} <= {"counter", "gauge", "histogram", "info"}


@pytest.mark.parametrize("name", [spec.name for spec in METRIC_CATALOG])
def test_every_catalog_metric_is_exported(name: str) -> None:
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry)
    _exercise(metrics)

    assert f"# HELP {name} " in _text(registry)


def test_prometheus_metrics_implements_the_port() -> None:
    assert isinstance(PrometheusMetrics(CollectorRegistry()), Metrics)


def test_counter_gauge_and_histogram_values_are_exported() -> None:
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry)
    _exercise(metrics)
    text = _text(registry)

    assert 'seismic_build_info{service="sink",version="0.1.0"} 1.0' in text
    assert 'seismic_ingest_records_total{outcome="published",source="replay"} 2.0' in text
    assert 'seismic_ingest_records_total{outcome="invalid",source="replay"} 1.0' in text
    assert "seismic_ingest_backpressure_waits_total 1.0" in text
    assert "seismic_replay_schedule_lag_seconds 0.25" in text
    assert "seismic_replay_progress_ratio 0.5" in text
    assert 'seismic_processor_events_total{outcome="new"} 1.0' in text
    assert 'seismic_processor_alerts_total{level="WARNING"} 1.0' in text
    assert "seismic_processor_window_updates_total 1.0" in text
    assert 'seismic_processor_skipped_total{reason="undecodable"} 1.0' in text
    assert 'seismic_sink_rows_total{table="events"} 3.0' in text
    assert 'seismic_sink_batches_total{outcome="ok",table="events"} 1.0' in text
    assert 'seismic_sink_insert_seconds_count{table="events"} 1.0' in text
    assert 'seismic_sink_insert_seconds_sum{table="events"} 0.125' in text
    assert "seismic_sink_retries_total 1.0" in text
    assert "seismic_sink_paused 1.0" in text
    assert "seismic_sink_end_to_end_seconds_count 1.0" in text
    assert 'seismic_dlq_messages_total{error_type="DecodeError",stage="sink_decode"} 1.0' in text
    assert (
        'seismic_consumer_lag_messages{group="seismic-sink",topic="seismic.events.v1"} 4.0' in text
    )
    assert 'seismic_topic_end_offset{topic="seismic.events.v1"} 42.0' in text


def test_sink_pause_gauge_can_be_cleared() -> None:
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(registry)
    metrics.set_sink_paused(True)
    metrics.set_sink_paused(False)

    assert "seismic_sink_paused 0.0" in _text(registry)


@pytest.mark.parametrize("service", sorted(SERVICE_METRICS))
def test_each_service_registers_exactly_its_catalog_subset(service: str) -> None:
    metrics = PrometheusMetrics(CollectorRegistry(), service=service)

    assert metrics.exported_names == SERVICE_METRICS[service]
    assert metrics.exported_names <= {spec.name for spec in METRIC_CATALOG}


def test_unknown_service_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown service"):
        PrometheusMetrics(CollectorRegistry(), service="grafana")
