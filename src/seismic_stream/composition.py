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
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from prometheus_client import CollectorRegistry

from seismic_stream import __version__
from seismic_stream.adapters.clock import SystemClock
from seismic_stream.adapters.kafka.codecs import DeadLetterCodec, EventCodec
from seismic_stream.adapters.kafka.producer import (
    KafkaDeadLetterPublisher,
    KafkaEventPublisher,
    ResilientProducer,
    producer_config,
)
from seismic_stream.adapters.observability.metrics import PrometheusMetrics
from seismic_stream.adapters.sources.chaos import ChaosPolicy, ChaoticSource
from seismic_stream.adapters.sources.replay import ReplaySource
from seismic_stream.adapters.sources.usgs_live import UsgsLiveSource, default_user_agent
from seismic_stream.application.ingest import (
    IngestService,
    InMemoryDeadLetterPublisher,
    InMemoryEventPublisher,
)
from seismic_stream.application.pacing import ReplayPacer
from seismic_stream.application.ports import Clock, EventSource
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
