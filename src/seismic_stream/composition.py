"""Composition root: wires adapters to application use cases (grows per task).

Only this module and the CLI know about concrete infrastructure. Tests inject
fakes, while the CLI calls the builders here to obtain a ready-to-run service.
"""

from __future__ import annotations

import random
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from prometheus_client import CollectorRegistry
from quixstreams import Application

from seismic_stream import __version__
from seismic_stream.adapters.clock import SystemClock
from seismic_stream.adapters.kafka.admin import KafkaLagReader
from seismic_stream.adapters.kafka.codecs import (
    AlertCodec,
    DeadLetterCodec,
    EventCodec,
    WindowStatsCodec,
)
from seismic_stream.adapters.kafka.consumer import KafkaBatchConsumer
from seismic_stream.adapters.kafka.producer import (
    KafkaDeadLetterPublisher,
    KafkaEventPublisher,
    ResilientProducer,
    producer_config,
)
from seismic_stream.adapters.kafka.topics import (
    AGGREGATES_TOPIC,
    ALERTS_TOPIC,
    DLQ_TOPIC,
    EVENTS_TOPIC,
)
from seismic_stream.adapters.observability.metrics import PrometheusMetrics
from seismic_stream.adapters.sources.chaos import ChaosPolicy, ChaoticSource
from seismic_stream.adapters.sources.replay import ReplaySource
from seismic_stream.adapters.sources.usgs_live import UsgsLiveSource, default_user_agent
from seismic_stream.adapters.storage.clickhouse import ClickHouseStore
from seismic_stream.adapters.stream.topology import build_application, build_topology
from seismic_stream.application.ingest import (
    IngestService,
    InMemoryDeadLetterPublisher,
    InMemoryEventPublisher,
)
from seismic_stream.application.monitoring import LagMonitor
from seismic_stream.application.pacing import ReplayPacer
from seismic_stream.application.ports import BatchConsumer, Clock, EventSource
from seismic_stream.application.retry import RetryPolicy
from seismic_stream.application.sinking import SinkService
from seismic_stream.config import Settings

SOURCE_NAMES = ("replay", "usgs")


@dataclass(slots=True)
class IngestSession:
    """Everything a running ingest process owns, plus its cleanup hook."""

    service: IngestService
    metrics: PrometheusMetrics
    _closer: Callable[[], None] | None = field(default=None, repr=False)

    def shutdown(self) -> None:
        """Release adapter resources such as the live HTTP client."""
        if self._closer is not None:
            self._closer()
            self._closer = None


def build_metrics(service: str) -> PrometheusMetrics:
    """Create one per-process registry with the build-info gauge set."""
    metrics = PrometheusMetrics(CollectorRegistry())
    metrics.set_build_info(service, __version__)
    return metrics


def build_source(
    settings: Settings,
    *,
    source_name: str,
    replay_file: Path,
    chaos: bool,
    seed: int,
    stop: threading.Event,
    clock: Clock,
) -> tuple[EventSource, Callable[[], None] | None]:
    """Build the configured source (optionally wrapped by the chaos decorator)."""
    if source_name not in SOURCE_NAMES:
        raise ValueError(f"unknown ingest source {source_name!r}")

    closer: Callable[[], None] | None = None
    source: EventSource
    if source_name == "usgs":
        client = httpx.Client(timeout=httpx.Timeout(30.0))
        source = UsgsLiveSource(
            client,
            clock=clock,
            rng=random.Random(seed),
            user_agent=default_user_agent(),
            stop=stop,
            feed=settings.live_feed,
            poll_interval_s=settings.live_poll_seconds,
        )
        closer = client.close
    else:
        source = ReplaySource(replay_file)

    if chaos:
        source = ChaoticSource(
            source,
            ChaosPolicy(
                seed=seed,
                malformed_rate=settings.chaos_malformed_rate,
                duplicate_rate=settings.chaos_duplicate_rate,
                revision_rate=settings.chaos_revision_rate,
            ),
        )
    return source, closer


def build_ingest_session(
    settings: Settings,
    *,
    source_name: str,
    replay_file: Path,
    speed: float | None,
    chaos: bool,
    seed: int,
    limit: int | None,
    dry_run: bool,
    stop: threading.Event,
    clock: Clock | None = None,
) -> IngestSession:
    """Wire the ingest use case with replay/live sources and Kafka or memory sinks."""
    wall_clock = clock or SystemClock()
    metrics = build_metrics("ingest")
    source, closer = build_source(
        settings,
        source_name=source_name,
        replay_file=replay_file,
        chaos=chaos,
        seed=seed,
        stop=stop,
        clock=wall_clock,
    )

    total_records = len(source) if isinstance(source, ReplaySource) else None
    if dry_run:
        events: InMemoryEventPublisher | KafkaEventPublisher = InMemoryEventPublisher()
        dead_letters: InMemoryDeadLetterPublisher | KafkaDeadLetterPublisher = (
            InMemoryDeadLetterPublisher()
        )
    else:
        registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        producer = ResilientProducer(
            Producer(
                producer_config(
                    settings.kafka_bootstrap,
                    settings.producer_queue_max_messages,
                )
            ),
            metrics,
        )
        events = KafkaEventPublisher(producer, EventCodec(registry))
        dead_letters = KafkaDeadLetterPublisher(producer, DeadLetterCodec(registry))

    service = IngestService(
        source=source,
        pacer=ReplayPacer(wall_clock, speed),
        events=events,
        dead_letters=dead_letters,
        metrics=metrics,
        clock=wall_clock,
        source_name=source_name,
        include_event_types=settings.include_event_types,
        limit=limit,
        total_records=total_records,
    )
    return IngestSession(service=service, metrics=metrics, _closer=closer)


@dataclass(slots=True)
class ProcessorSession:
    """Quix application plus the observability registry serving its metrics."""

    application: Application
    metrics: PrometheusMetrics


def build_processor_session(settings: Settings) -> ProcessorSession:
    """Wire the processor application and topology with the Kafka adapters."""
    metrics = build_metrics("processor")
    application = build_application(settings, metrics)
    build_topology(application, settings, metrics)
    return ProcessorSession(application=application, metrics=metrics)


@dataclass(slots=True)
class SinkSession:
    """Sink service plus the adapters it owns and their cleanup hook."""

    service: SinkService
    metrics: PrometheusMetrics
    consumer: BatchConsumer
    _closer: Callable[[], None] | None = field(default=None, repr=False)

    def shutdown(self) -> None:
        """Close the consumer and flush any queued dead letters."""
        self.consumer.close()
        if self._closer is not None:
            self._closer()
            self._closer = None


def build_sink_session(
    settings: Settings,
    *,
    consumer: BatchConsumer | None = None,
    store: ClickHouseStore | None = None,
) -> SinkSession:
    """Wire the sink use case, the Kafka consumer and the ClickHouse repository."""
    metrics = build_metrics("sink")
    kafka_consumer = consumer or KafkaBatchConsumer.from_config(
        settings.kafka_bootstrap, "seismic-sink"
    )
    kafka_consumer.subscribe((EVENTS_TOPIC, AGGREGATES_TOPIC, ALERTS_TOPIC, DLQ_TOPIC))
    clickhouse = store or ClickHouseStore.from_settings(settings)

    registry = SchemaRegistryClient({"url": settings.schema_registry_url})
    producer = ResilientProducer(
        Producer(producer_config(settings.kafka_bootstrap, settings.producer_queue_max_messages)),
        metrics,
    )
    dead_letters = KafkaDeadLetterPublisher(producer, DeadLetterCodec(registry))
    service = SinkService(
        consumer=kafka_consumer,
        store=clickhouse,
        dead_letters=dead_letters,
        metrics=metrics,
        clock=SystemClock(),
        retry_policy=RetryPolicy(
            base_seconds=settings.retry_base_seconds,
            max_seconds=settings.retry_max_seconds,
        ),
        codecs={
            EVENTS_TOPIC: EventCodec(registry),
            AGGREGATES_TOPIC: WindowStatsCodec(registry),
            ALERTS_TOPIC: AlertCodec(registry),
            DLQ_TOPIC: DeadLetterCodec(registry),
        },
        batch_size=settings.sink_batch_size,
        batch_max_wait_seconds=settings.sink_batch_max_wait_seconds,
    )
    return SinkSession(
        service=service,
        metrics=metrics,
        consumer=kafka_consumer,
        _closer=producer.flush,
    )


MONITOR_TOPICS = (EVENTS_TOPIC, AGGREGATES_TOPIC, ALERTS_TOPIC, DLQ_TOPIC)


@dataclass(slots=True)
class MonitorSession:
    """Lag monitor plus the reader and metrics registry it owns."""

    monitor: LagMonitor
    metrics: PrometheusMetrics
    reader: KafkaLagReader

    def shutdown(self) -> None:
        """Release the probe consumer."""
        self.reader.close()


def build_monitor_session(settings: Settings) -> MonitorSession:
    """Wire the lag monitor with the Kafka offset reader (S14)."""
    logger = structlog.get_logger("seismic.monitor")
    metrics = build_metrics("monitor")
    reader = KafkaLagReader(settings.kafka_bootstrap, MONITOR_TOPICS)
    monitor = LagMonitor(
        reader,
        metrics,
        groups=settings.monitor_groups,
        topics=MONITOR_TOPICS,
        interval_seconds=settings.monitor_interval_seconds,
        clock=SystemClock(),
        on_error=lambda error: logger.warning("lag_poll_failed", error=str(error)),
    )
    return MonitorSession(monitor=monitor, metrics=metrics, reader=reader)
