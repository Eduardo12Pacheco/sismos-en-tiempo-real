"""Prometheus implementation of the observability port.

``METRIC_CATALOG`` is the single source of truth for metric names, types, labels
and help text. Documentation and tests read it, so a new metric cannot be added
without its description.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram

MetricType = Literal["counter", "gauge", "histogram"]
Collector = Counter | Gauge | Histogram


@dataclass(frozen=True, slots=True)
class MetricSpec:
    """Declaration of one metric exposed by the pipeline."""

    name: str
    type: MetricType
    labels: tuple[str, ...]
    help_text: str


METRIC_CATALOG: tuple[MetricSpec, ...] = (
    MetricSpec(
        "seismic_build_info",
        "gauge",
        ("service", "version"),
        "Build information for the running service.",
    ),
    MetricSpec(
        "seismic_ingest_records_total",
        "counter",
        ("source", "outcome"),
        "Source records read by ingest, by source and outcome.",
    ),
    MetricSpec(
        "seismic_ingest_backpressure_waits_total",
        "counter",
        (),
        "Producer queue-full waits during ingest.",
    ),
    MetricSpec(
        "seismic_replay_schedule_lag_seconds",
        "gauge",
        (),
        "How far behind schedule the replay pacer is running, in seconds.",
    ),
    MetricSpec(
        "seismic_replay_progress_ratio",
        "gauge",
        (),
        "Replay progress as a fraction of the source records.",
    ),
    MetricSpec(
        "seismic_processor_events_total",
        "counter",
        ("outcome",),
        "Processor decisions, by outcome (new, revision, duplicate).",
    ),
    MetricSpec(
        "seismic_processor_alerts_total",
        "counter",
        ("level",),
        "Alerts emitted by the processor, by level.",
    ),
    MetricSpec(
        "seismic_processor_window_updates_total",
        "counter",
        (),
        "Window statistics updates emitted by the processor.",
    ),
    MetricSpec(
        "seismic_processor_skipped_total",
        "counter",
        ("reason",),
        "Processor records skipped, by reason.",
    ),
    MetricSpec(
        "seismic_sink_rows_total",
        "counter",
        ("table",),
        "Rows stored by the sink, by ClickHouse table.",
    ),
    MetricSpec(
        "seismic_sink_batches_total",
        "counter",
        ("table", "outcome"),
        "Sink batches processed, by table and outcome.",
    ),
    MetricSpec(
        "seismic_sink_insert_seconds",
        "histogram",
        ("table",),
        "Duration of ClickHouse inserts, by table.",
    ),
    MetricSpec(
        "seismic_sink_retries_total",
        "counter",
        (),
        "Transient sink retries.",
    ),
    MetricSpec(
        "seismic_sink_paused",
        "gauge",
        (),
        "Whether the sink has paused its consumer assignment (1 = paused).",
    ),
    MetricSpec(
        "seismic_sink_end_to_end_seconds",
        "histogram",
        (),
        "End-to-end latency from ingest timestamp to storage, in seconds.",
    ),
    MetricSpec(
        "seismic_dlq_messages_total",
        "counter",
        ("stage", "error_type"),
        "Dead-letter messages, by stage and error type.",
    ),
    MetricSpec(
        "seismic_consumer_lag_messages",
        "gauge",
        ("group", "topic"),
        "Consumer lag in messages, by consumer group and topic.",
    ),
    MetricSpec(
        "seismic_topic_end_offset",
        "gauge",
        ("topic",),
        "Current end offset of a topic.",
    ),
)


class PrometheusMetrics:
    """Export the catalog to one per-process :class:`CollectorRegistry`."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self._registry = registry
        self._collectors: dict[str, Collector] = {}
        for spec in METRIC_CATALOG:
            match spec.type:
                case "counter":
                    collector: Collector = Counter(
                        spec.name, spec.help_text, spec.labels, registry=registry
                    )
                case "gauge":
                    collector = Gauge(spec.name, spec.help_text, spec.labels, registry=registry)
                case "histogram":
                    collector = Histogram(spec.name, spec.help_text, spec.labels, registry=registry)
            self._collectors[spec.name] = collector

    @property
    def registry(self) -> CollectorRegistry:
        """Registry this adapter populates and the health server exports."""
        return self._registry

    def _counter(self, name: str, labels: Mapping[str, str] | None = None) -> Counter:
        collector = self._collectors[name]
        if not isinstance(collector, Counter):
            raise TypeError(f"{name} is not a counter")
        return collector.labels(**labels) if labels else collector

    def _gauge(self, name: str, labels: Mapping[str, str] | None = None) -> Gauge:
        collector = self._collectors[name]
        if not isinstance(collector, Gauge):
            raise TypeError(f"{name} is not a gauge")
        return collector.labels(**labels) if labels else collector

    def _histogram(self, name: str, labels: Mapping[str, str] | None = None) -> Histogram:
        collector = self._collectors[name]
        if not isinstance(collector, Histogram):
            raise TypeError(f"{name} is not a histogram")
        return collector.labels(**labels) if labels else collector

    def set_build_info(self, service: str, version: str) -> None:
        self._gauge("seismic_build_info", {"service": service, "version": version}).set(1)

    def record_ingest(self, source: str, outcome: str) -> None:
        self._counter("seismic_ingest_records_total", {"source": source, "outcome": outcome}).inc()

    def record_backpressure_wait(self) -> None:
        self._counter("seismic_ingest_backpressure_waits_total").inc()

    def observe_replay_schedule_lag(self, seconds: float) -> None:
        self._gauge("seismic_replay_schedule_lag_seconds").set(seconds)

    def set_replay_progress(self, ratio: float) -> None:
        self._gauge("seismic_replay_progress_ratio").set(ratio)

    def record_processor_event(self, outcome: str) -> None:
        self._counter("seismic_processor_events_total", {"outcome": outcome}).inc()

    def record_processor_alert(self, level: str) -> None:
        self._counter("seismic_processor_alerts_total", {"level": level}).inc()

    def record_window_update(self) -> None:
        self._counter("seismic_processor_window_updates_total").inc()

    def record_processor_skipped(self, reason: str) -> None:
        self._counter("seismic_processor_skipped_total", {"reason": reason}).inc()

    def record_sink_rows(self, table: str, count: int) -> None:
        self._counter("seismic_sink_rows_total", {"table": table}).inc(count)

    def record_sink_batch(self, table: str, outcome: str) -> None:
        self._counter("seismic_sink_batches_total", {"table": table, "outcome": outcome}).inc()

    def observe_sink_insert_seconds(self, table: str, seconds: float) -> None:
        self._histogram("seismic_sink_insert_seconds", {"table": table}).observe(seconds)

    def record_sink_retry(self) -> None:
        self._counter("seismic_sink_retries_total").inc()

    def set_sink_paused(self, paused: bool) -> None:
        self._gauge("seismic_sink_paused").set(1 if paused else 0)

    def observe_end_to_end_seconds(self, seconds: float) -> None:
        self._histogram("seismic_sink_end_to_end_seconds").observe(seconds)

    def record_dlq_message(self, stage: str, error_type: str) -> None:
        self._counter(
            "seismic_dlq_messages_total", {"stage": stage, "error_type": error_type}
        ).inc()

    def set_consumer_lag(self, group: str, topic: str, lag: int) -> None:
        self._gauge("seismic_consumer_lag_messages", {"group": group, "topic": topic}).set(lag)

    def set_topic_end_offset(self, topic: str, offset: int) -> None:
        self._gauge("seismic_topic_end_offset", {"topic": topic}).set(offset)
