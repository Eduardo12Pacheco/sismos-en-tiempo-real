"""Resilient Kafka producer and the event/dead-letter publisher adapters.

``ResilientProducer`` owns the two failure modes that matter for ingest:
librdkafka's bounded producer queue (``BufferError``, handled by polling and
retrying instead of dropping the record) and asynchronous delivery failures
(collected through the delivery callback and raised from ``flush`` so a run can
never report success after losing a message).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from seismic_stream.adapters.kafka import codecs
from seismic_stream.adapters.kafka.topics import DLQ_TOPIC, EVENTS_TOPIC
from seismic_stream.application.ports import Metrics
from seismic_stream.domain.models import DeadLetter, Event

BACKPRESSURE_POLL_SECONDS = 0.2


class DeliveryError(RuntimeError):
    """Raised when at least one produced message could not be delivered."""


class ProducerLike(Protocol):
    """Subset of ``confluent_kafka.Producer`` used by the resilient wrapper."""

    def produce(self, **kwargs: Any) -> None:
        """Queue one message."""

    def poll(self, timeout: float = 0) -> int:
        """Serve delivery callbacks."""

    def flush(self, timeout: float = -1) -> int:
        """Wait for queued messages and return how many remain."""


class Serializer[T](Protocol):
    """Typed boundary implemented by the Avro codecs."""

    def serialize(self, value: T) -> bytes:
        """Serialize one value."""


def producer_config(bootstrap_servers: str, queue_max_messages: int) -> dict[str, Any]:
    """Return the idempotent, durable producer settings fixed by S5."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 20,
        "compression.type": "zstd",
        "queue.buffering.max.messages": queue_max_messages,
    }


class ResilientProducer:
    """Wrap a librdkafka producer with backpressure and delivery tracking."""

    def __init__(
        self,
        producer: ProducerLike,
        metrics: Metrics,
        *,
        backpressure_poll_seconds: float = BACKPRESSURE_POLL_SECONDS,
    ) -> None:
        self._producer = producer
        self._metrics = metrics
        self._backpressure_poll_seconds = backpressure_poll_seconds
        self._delivery_errors: list[DeliveryError] = []

    def produce(
        self,
        *,
        topic: str,
        key: str,
        payload: bytes,
        timestamp_ms: int,
        headers: Mapping[str, str],
    ) -> None:
        """Queue one message, waiting for queue space while the buffer is full."""
        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=payload,
                    timestamp=timestamp_ms,
                    headers=codecs.encode_headers(headers),
                    on_delivery=self._on_delivery,
                )
                return
            except BufferError:
                self._metrics.record_backpressure_wait()
                self._producer.poll(self._backpressure_poll_seconds)

    def flush(self, timeout: float | None = None) -> None:
        """Wait for deliveries and raise if any message was lost."""
        remaining = self._producer.flush(-1.0 if timeout is None else timeout)
        errors = tuple(self._delivery_errors)
        if remaining or errors:
            details = "; ".join(str(error) for error in errors)
            reason = f"{remaining} message(s) were not delivered within the timeout"
            raise DeliveryError(f"{reason}: {details}" if details else reason)

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is not None:
            self._delivery_errors.append(DeliveryError(str(error)))


class KafkaEventPublisher:
    """Serialize domain events to the events topic with stable keys and headers."""

    def __init__(self, producer: ResilientProducer, codec: Serializer[Event]) -> None:
        self._producer = producer
        self._codec = codec

    def publish(self, event: Event, headers: Mapping[str, str]) -> None:
        """Publish one event; the Kafka key is always its ``event_id``."""
        self._producer.produce(
            topic=EVENTS_TOPIC,
            key=event.event_id,
            payload=self._codec.serialize(event),
            timestamp_ms=event.event_time_ms,
            headers=headers,
        )

    def flush(self, timeout: float | None = None) -> None:
        """Flush the shared producer."""
        self._producer.flush(timeout)


class KafkaDeadLetterPublisher:
    """Serialize dead letters to the DLQ topic keyed by their deterministic id."""

    def __init__(self, producer: ResilientProducer, codec: Serializer[DeadLetter]) -> None:
        self._producer = producer
        self._codec = codec

    def publish(self, letter: DeadLetter) -> None:
        """Publish one dead letter with the deterministic ``dlq_id`` key."""
        self._producer.produce(
            topic=DLQ_TOPIC,
            key=letter.dlq_id,
            payload=self._codec.serialize(letter),
            timestamp_ms=letter.failed_at_ms,
            headers={},
        )

    def flush(self, timeout: float | None = None) -> None:
        """Flush the shared producer."""
        self._producer.flush(timeout)
