"""Quix Streams adapter: stateful dedup, event-time windows and alerts.

The topology is intentionally thin: every decision with business meaning lives
in :mod:`seismic_stream.application.processing`, :mod:`seismic_stream.domain` or
the Avro codecs, so this module only wires Quix callbacks to those helpers.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from quixstreams import Application
from quixstreams.dataframe import StreamingDataFrame
from quixstreams.dataframe.windows.aggregations import Count, Max, Mean, Sum
from quixstreams.models import Row
from quixstreams.models.serializers import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)
from quixstreams.models.serializers.avro import AvroDeserializer, AvroSerializer
from quixstreams.state import State

from seismic_stream.adapters.kafka.codecs import (
    ALERT_SCHEMA_FILE,
    WINDOW_STATS_SCHEMA_FILE,
    alert_to_dict,
    event_from_dict,
    load_schema,
)
from seismic_stream.adapters.kafka.topics import (
    AGGREGATES_TOPIC,
    ALERTS_TOPIC,
    EVENTS_TOPIC,
)
from seismic_stream.application.ports import Metrics
from seismic_stream.application.processing import (
    DedupDecision,
    Deduplicator,
    shape_window_stats,
    to_window_input,
)
from seismic_stream.config import Settings
from seismic_stream.domain.alerts import AlertPolicy

PROCESSOR_CONSUMER_GROUP = "seismic-processor"
CONSUMER_ERROR_REASON = "undecodable"
PROCESSING_ERROR_REASON = "processing_error"
DEDUP_MARKER = "dedup_decision"

ConsumerErrorCallback = Callable[[Exception, Any, logging.Logger], bool]
ProcessingErrorCallback = Callable[[Exception, Row | None, logging.Logger], bool]

logger = logging.getLogger(__name__)


def consumer_error_handler(metrics: Metrics) -> ConsumerErrorCallback:
    """Build the callback that logs, counts and skips undecodable records."""

    def handle(error: Exception, message: Any, log: logging.Logger) -> bool:
        log.exception(
            "skipping an undecodable processor record: %s",
            error,
            extra={"reason": CONSUMER_ERROR_REASON},
        )
        metrics.record_processor_skipped(CONSUMER_ERROR_REASON)
        return True

    return handle


def processing_error_handler(metrics: Metrics) -> ProcessingErrorCallback:
    """Build the callback that suppresses only known per-row data errors."""

    def handle(error: Exception, row: Row | None, log: logging.Logger) -> bool:
        log.exception(
            "skipping a processor row after a processing error: %s",
            error,
            extra={
                "topic": row.topic if row is not None else None,
                "partition": row.partition if row is not None else None,
                "offset": row.offset if row is not None else None,
            },
        )
        metrics.record_processor_skipped(PROCESSING_ERROR_REASON)
        return isinstance(error, (KeyError, ValueError))

    return handle


def _registry_config(settings: Settings) -> SchemaRegistryClientConfig:
    return SchemaRegistryClientConfig(url=settings.schema_registry_url)


def _serializer(schema_filename: str, settings: Settings) -> AvroSerializer:
    return AvroSerializer(
        json.loads(load_schema(schema_filename)),
        schema_registry_client_config=_registry_config(settings),
        schema_registry_serialization_config=SchemaRegistrySerializationConfig(
            auto_register_schemas=False,
            use_latest_version=True,
        ),
    )


def build_application(settings: Settings, metrics: Metrics) -> Application:
    """Create the Quix application with the planned consumer and error policy."""
    return Application(
        broker_address=settings.kafka_bootstrap,
        consumer_group=PROCESSOR_CONSUMER_GROUP,
        auto_offset_reset="earliest",
        state_dir=Path(settings.state_dir),
        processing_guarantee="at-least-once",
        on_consumer_error=consumer_error_handler(metrics),
        on_processing_error=processing_error_handler(metrics),
        # Keep the root structlog/stdlib bridge authoritative: Quix must not
        # install its own non-JSON handler (S15).
        loglevel=None,
    )


def build_topology(app: Application, settings: Settings, metrics: Metrics) -> StreamingDataFrame:
    """Wire the events stream into dedup, alerts and event-time window branches."""
    events_topic = app.topic(
        EVENTS_TOPIC,
        value_deserializer=AvroDeserializer(
            schema_registry_client_config=_registry_config(settings)
        ),
        timestamp_extractor=lambda value, headers, timestamp, timestamp_type: int(
            value["event_time_ms"]
        ),
    )
    alerts_topic = app.topic(
        ALERTS_TOPIC,
        value_serializer=_serializer(ALERT_SCHEMA_FILE, settings),
    )
    aggregates_topic = app.topic(
        AGGREGATES_TOPIC,
        value_serializer=_serializer(WINDOW_STATS_SCHEMA_FILE, settings),
    )

    policy = AlertPolicy(
        warning_mag=settings.alert_warning_mag,
        watch_mag=settings.alert_watch_mag,
        ecuador_mag=settings.alert_ecuador_mag,
    )
    window_ms = settings.window_minutes * 60_000
    grace_ms = settings.window_grace_minutes * 60_000

    def dedup_step(value: dict[str, Any], state: State[Any, Any]) -> dict[str, Any]:
        decision, new_state = Deduplicator.decide(
            state.get("updated_at_ms"), value["updated_at_ms"]
        )
        state.set("updated_at_ms", new_state)
        metrics.record_processor_event(decision.value)
        return {**value, DEDUP_MARKER: decision.value}

    def is_new(value: dict[str, Any]) -> bool:
        return bool(value[DEDUP_MARKER] == DedupDecision.NEW.value)

    def is_alertable(value: dict[str, Any]) -> bool:
        return value[DEDUP_MARKER] in (
            DedupDecision.NEW.value,
            DedupDecision.REVISION.value,
        )

    def alert_step(value: dict[str, Any]) -> dict[str, Any] | None:
        alert = policy.evaluate(event_from_dict(value))
        if alert is None:
            return None
        metrics.record_processor_alert(alert.level.value)
        return alert_to_dict(alert)

    def window_stats_step(
        value: dict[str, Any], key: Any, timestamp: int, headers: Any
    ) -> dict[str, Any]:
        region = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        stats = shape_window_stats(region, value)
        metrics.record_window_update()
        return stats

    def window_key(stats: Mapping[str, Any]) -> bytes:
        return f"{stats['region']}|{stats['window_start_ms']}".encode()

    deduped = app.dataframe(events_topic).apply(dedup_step, stateful=True)

    deduped.filter(is_alertable).apply(alert_step).filter(lambda alert: alert is not None).to_topic(
        alerts_topic, key=lambda alert: str(alert["event_id"]).encode()
    )

    windows = (
        deduped.filter(is_new)
        .apply(to_window_input)
        .group_by(lambda value: value["region"], name="by_region")
        .tumbling_window(duration_ms=window_ms, grace_ms=grace_ms)
        .agg(
            event_count=Count(),
            max_magnitude=Max("magnitude"),
            mean_magnitude=Mean("magnitude"),
            mean_depth_km=Mean("depth_km"),
            total_energy_joules=Sum("energy_joules"),
        )
        .current()
    )
    return windows.apply(window_stats_step, metadata=True).to_topic(
        aggregates_topic, key=window_key
    )
