"""Avro codecs mapping domain objects to and from Schema Registry payloads.

Schemas are versioned files bundled inside the package and loaded through
``importlib.resources`` so the installed wheel and the tests read the same bytes.
Services serialize with ``auto.register.schemas=false``: only ``bootstrap`` is
allowed to register contracts (D4).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from importlib.resources import files
from typing import Any, Final, cast

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from seismic_stream.adapters.kafka.topics import (
    AGGREGATES_TOPIC,
    ALERTS_TOPIC,
    DLQ_TOPIC,
    EVENTS_TOPIC,
)
from seismic_stream.domain.models import Alert, AlertLevel, DeadLetter, DlqStage, Event, WindowStats

SCHEMA_PACKAGE: Final = "seismic_stream.adapters.kafka.schemas"
EVENT_SCHEMA_FILE: Final = "earthquake_event.v1.avsc"
WINDOW_STATS_SCHEMA_FILE: Final = "window_stats.v1.avsc"
ALERT_SCHEMA_FILE: Final = "alert.v1.avsc"
DEAD_LETTER_SCHEMA_FILE: Final = "dead_letter.v1.avsc"

SOURCE_HEADER: Final = "source"
INGESTED_AT_HEADER: Final = "ingested_at_ms"

SERIALIZER_CONF: Final[dict[str, Any]] = {
    "auto.register.schemas": False,
    "use.latest.version": True,
}

type ToDict[T] = Callable[[T], dict[str, Any]]
type FromDict[T] = Callable[[Mapping[str, Any]], T]


class CodecError(ValueError):
    """Raised when an Avro record cannot be mapped to a domain object."""


def encode_headers(headers: Mapping[str, str]) -> list[tuple[str, bytes]]:
    """Encode string headers for librdkafka, preserving insertion order."""
    return [(name, value.encode("utf-8")) for name, value in headers.items()]


def decode_headers(
    headers: Iterable[tuple[str, bytes | str]] | None,
) -> dict[str, str]:
    """Decode a consumed Kafka message's headers into plain strings."""
    if headers is None:
        return {}
    decoded: dict[str, str] = {}
    for name, value in headers:
        decoded[name] = value.decode("utf-8") if isinstance(value, bytes) else value
    return decoded


def event_headers(event: Event, ingested_at_ms: int) -> dict[str, str]:
    """Build the headers attached to an ``earthquake_event`` message."""
    return {
        SOURCE_HEADER: event.source,
        INGESTED_AT_HEADER: str(ingested_at_ms),
    }


def load_schema(filename: str) -> str:
    """Return the UTF-8 text of a bundled ``.avsc`` file."""
    return files(SCHEMA_PACKAGE).joinpath(filename).read_text(encoding="utf-8")


def _str(record: Mapping[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str):
        raise CodecError(f"{field} must be a string")
    return value


def _optional_str(record: Mapping[str, Any], field: str) -> str | None:
    value = record.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise CodecError(f"{field} must be a string or null")
    return value


def _int(record: Mapping[str, Any], field: str) -> int:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise CodecError(f"{field} must be an integer")
    return value


def _optional_int(record: Mapping[str, Any], field: str) -> int | None:
    value = record.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise CodecError(f"{field} must be an integer or null")
    return value


def _float(record: Mapping[str, Any], field: str) -> float:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise CodecError(f"{field} must be a number")
    return float(value)


def _bool(record: Mapping[str, Any], field: str) -> bool:
    value = record.get(field)
    if not isinstance(value, bool):
        raise CodecError(f"{field} must be a boolean")
    return value


def _bytes(record: Mapping[str, Any], field: str) -> bytes:
    value = record.get(field)
    if not isinstance(value, bytes | bytearray):
        raise CodecError(f"{field} must be bytes")
    return bytes(value)


def event_to_dict(event: Event) -> dict[str, Any]:
    """Map a domain :class:`Event` to the ``earthquake_event`` Avro record."""
    return {
        "event_id": event.event_id,
        "source": event.source,
        "event_type": event.event_type,
        "event_time_ms": event.event_time_ms,
        "updated_at_ms": event.updated_at_ms,
        "magnitude": event.magnitude,
        "magnitude_type": event.magnitude_type,
        "place": event.place,
        "region": event.region,
        "latitude": event.latitude,
        "longitude": event.longitude,
        "depth_km": event.depth_km,
        "status": event.status,
        "tsunami": event.tsunami,
        "significance": event.significance,
        "network": event.network,
        "detail_url": event.detail_url,
        "in_ecuador_region": event.in_ecuador_region,
    }


def event_from_dict(record: Mapping[str, Any]) -> Event:
    """Rebuild a domain :class:`Event` from an ``earthquake_event`` record."""
    return Event(
        event_id=_str(record, "event_id"),
        source=_str(record, "source"),
        event_type=_str(record, "event_type"),
        event_time_ms=_int(record, "event_time_ms"),
        updated_at_ms=_int(record, "updated_at_ms"),
        magnitude=_float(record, "magnitude"),
        magnitude_type=_optional_str(record, "magnitude_type"),
        place=_str(record, "place"),
        region=_str(record, "region"),
        latitude=_float(record, "latitude"),
        longitude=_float(record, "longitude"),
        depth_km=_float(record, "depth_km"),
        status=_str(record, "status"),
        tsunami=_bool(record, "tsunami"),
        significance=_int(record, "significance"),
        network=_optional_str(record, "network"),
        detail_url=_optional_str(record, "detail_url"),
        in_ecuador_region=_bool(record, "in_ecuador_region"),
    )


def window_stats_to_dict(stats: WindowStats) -> dict[str, Any]:
    """Map :class:`WindowStats` to the ``window_stats`` Avro record."""
    return {
        "region": stats.region,
        "window_start_ms": stats.window_start_ms,
        "window_end_ms": stats.window_end_ms,
        "event_count": stats.event_count,
        "max_magnitude": stats.max_magnitude,
        "mean_magnitude": stats.mean_magnitude,
        "mean_depth_km": stats.mean_depth_km,
        "total_energy_joules": stats.total_energy_joules,
        "emitted_at_ms": stats.emitted_at_ms,
    }


def window_stats_from_dict(record: Mapping[str, Any]) -> WindowStats:
    """Rebuild :class:`WindowStats` from a ``window_stats`` record."""
    return WindowStats(
        region=_str(record, "region"),
        window_start_ms=_int(record, "window_start_ms"),
        window_end_ms=_int(record, "window_end_ms"),
        event_count=_int(record, "event_count"),
        max_magnitude=_float(record, "max_magnitude"),
        mean_magnitude=_float(record, "mean_magnitude"),
        mean_depth_km=_float(record, "mean_depth_km"),
        total_energy_joules=_float(record, "total_energy_joules"),
        emitted_at_ms=_int(record, "emitted_at_ms"),
    )


def alert_to_dict(alert: Alert) -> dict[str, Any]:
    """Map :class:`Alert` to the ``alert`` Avro record."""
    return {
        "event_id": alert.event_id,
        "level": alert.level.value,
        "reason": alert.reason,
        "magnitude": alert.magnitude,
        "place": alert.place,
        "event_time_ms": alert.event_time_ms,
        "updated_at_ms": alert.updated_at_ms,
        "tsunami": alert.tsunami,
        "in_ecuador_region": alert.in_ecuador_region,
    }


def alert_from_dict(record: Mapping[str, Any]) -> Alert:
    """Rebuild :class:`Alert` from an ``alert`` record."""
    try:
        level = AlertLevel(_str(record, "level"))
    except ValueError as error:
        raise CodecError(f"unknown alert level: {error}") from error
    return Alert(
        event_id=_str(record, "event_id"),
        level=level,
        reason=_str(record, "reason"),
        magnitude=_float(record, "magnitude"),
        place=_str(record, "place"),
        event_time_ms=_int(record, "event_time_ms"),
        updated_at_ms=_int(record, "updated_at_ms"),
        tsunami=_bool(record, "tsunami"),
        in_ecuador_region=_bool(record, "in_ecuador_region"),
    )


def dead_letter_to_dict(letter: DeadLetter) -> dict[str, Any]:
    """Map :class:`DeadLetter` to the ``dead_letter`` Avro record."""
    return {
        "dlq_id": letter.dlq_id,
        "stage": letter.stage.value,
        "error_type": letter.error_type,
        "error_message": letter.error_message,
        "source_topic": letter.source_topic,
        "source_partition": letter.source_partition,
        "source_offset": letter.source_offset,
        "payload": letter.payload,
        "failed_at_ms": letter.failed_at_ms,
        "attempts": letter.attempts,
    }


def dead_letter_from_dict(record: Mapping[str, Any]) -> DeadLetter:
    """Rebuild :class:`DeadLetter` from a ``dead_letter`` record."""
    try:
        stage = DlqStage(_str(record, "stage"))
    except ValueError as error:
        raise CodecError(f"unknown dead-letter stage: {error}") from error
    return DeadLetter(
        dlq_id=_str(record, "dlq_id"),
        stage=stage,
        error_type=_str(record, "error_type"),
        error_message=_str(record, "error_message"),
        source_topic=_optional_str(record, "source_topic"),
        source_partition=_optional_int(record, "source_partition"),
        source_offset=_optional_int(record, "source_offset"),
        payload=_bytes(record, "payload"),
        failed_at_ms=_int(record, "failed_at_ms"),
        attempts=_int(record, "attempts"),
    )


class AvroCodec[T]:
    """Serialize/deserialize one domain type with a bundled writer schema."""

    def __init__(
        self,
        registry: SchemaRegistryClient,
        *,
        topic: str,
        schema_filename: str,
        to_dict: ToDict[T],
        from_dict: FromDict[T],
    ) -> None:
        self._topic = topic
        self._schema_str = load_schema(schema_filename)
        self._serializer = AvroSerializer(
            registry,
            self._schema_str,
            to_dict=lambda value, context: to_dict(cast(T, value)),
            conf=dict(SERIALIZER_CONF),
        )
        self._deserializer = AvroDeserializer(
            registry,
            self._schema_str,
            from_dict=lambda record, context: from_dict(cast(Mapping[str, Any], record)),
        )

    @property
    def schema_str(self) -> str:
        """Reader/writer schema text bundled with the application."""
        return self._schema_str

    @property
    def topic(self) -> str:
        """Topic whose ``<topic>-value`` subject the codec targets."""
        return self._topic

    def serialize(self, value: T) -> bytes:
        """Serialize ``value`` using the latest registered subject version."""
        context = SerializationContext(self._topic, MessageField.VALUE)
        return cast(bytes, self._serializer(value, context))

    def deserialize(self, payload: bytes) -> T:
        """Deserialize a Schema Registry framed payload into a domain object."""
        context = SerializationContext(self._topic, MessageField.VALUE)
        return cast(T, self._deserializer(payload, context))


class EventCodec(AvroCodec[Event]):
    """Codec for ``seismic.events.v1`` values."""

    def __init__(self, registry: SchemaRegistryClient) -> None:
        super().__init__(
            registry,
            topic=EVENTS_TOPIC,
            schema_filename=EVENT_SCHEMA_FILE,
            to_dict=event_to_dict,
            from_dict=event_from_dict,
        )


class WindowStatsCodec(AvroCodec[WindowStats]):
    """Codec for ``seismic.aggregates.v1`` values."""

    def __init__(self, registry: SchemaRegistryClient) -> None:
        super().__init__(
            registry,
            topic=AGGREGATES_TOPIC,
            schema_filename=WINDOW_STATS_SCHEMA_FILE,
            to_dict=window_stats_to_dict,
            from_dict=window_stats_from_dict,
        )


class AlertCodec(AvroCodec[Alert]):
    """Codec for ``seismic.alerts.v1`` values."""

    def __init__(self, registry: SchemaRegistryClient) -> None:
        super().__init__(
            registry,
            topic=ALERTS_TOPIC,
            schema_filename=ALERT_SCHEMA_FILE,
            to_dict=alert_to_dict,
            from_dict=alert_from_dict,
        )


class DeadLetterCodec(AvroCodec[DeadLetter]):
    """Codec for ``seismic.dlq.v1`` values."""

    def __init__(self, registry: SchemaRegistryClient) -> None:
        super().__init__(
            registry,
            topic=DLQ_TOPIC,
            schema_filename=DEAD_LETTER_SCHEMA_FILE,
            to_dict=dead_letter_to_dict,
            from_dict=dead_letter_from_dict,
        )
