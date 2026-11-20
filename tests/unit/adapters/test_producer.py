"""Unit tests for the resilient producer and the Kafka publishers.

A fake librdkafka producer lets us drive ``BufferError`` backpressure and
delivery failures deterministically, with no broker involved.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from seismic_stream.adapters.kafka import codecs
from seismic_stream.adapters.kafka.producer import (
    DeliveryError,
    KafkaDeadLetterPublisher,
    KafkaEventPublisher,
    ResilientProducer,
    producer_config,
)
from seismic_stream.adapters.kafka.topics import DLQ_TOPIC, EVENTS_TOPIC
from seismic_stream.domain.models import DeadLetter, DlqStage
from tests.support.factories import make_event


class FakeMetrics:
    def __init__(self) -> None:
        self.backpressure_waits = 0

    def record_backpressure_wait(self) -> None:
        self.backpressure_waits += 1


class FakeDeliveryError:
    def __str__(self) -> str:
        return "delivery failed: broker unavailable"


class FakeProducer:
    """Stands in for ``confluent_kafka.Producer`` and records every call."""

    def __init__(self, *, buffer_errors: int = 0, flush_remaining: int = 0) -> None:
        self.buffer_errors = buffer_errors
        self.flush_remaining = flush_remaining
        self.calls: list[dict[str, Any]] = []
        self.polls: list[float] = []
        self.flushes: list[float] = []
        self.callback: Any = None

    def produce(self, **kwargs: Any) -> None:
        if self.buffer_errors > 0:
            self.buffer_errors -= 1
            raise BufferError("queue full")
        self.calls.append(kwargs)
        self.callback = kwargs.get("on_delivery")

    def poll(self, timeout: float = 0) -> int:
        self.polls.append(timeout)
        return 0

    def flush(self, timeout: float = -1) -> int:
        self.flushes.append(timeout)
        return self.flush_remaining

    def fail_delivery(self) -> None:
        assert self.callback is not None
        self.callback(FakeDeliveryError(), object())


class FakeCodec:
    def __init__(self, payload: bytes = b"encoded") -> None:
        self.payload = payload
        self.values: list[Any] = []

    def serialize(self, value: Any) -> bytes:
        self.values.append(value)
        return self.payload


def _producer(
    fake: FakeProducer | None = None, metrics: FakeMetrics | None = None
) -> tuple[ResilientProducer, FakeProducer, FakeMetrics]:
    raw = fake or FakeProducer()
    port = metrics or FakeMetrics()
    return ResilientProducer(raw, port), raw, port


def test_producer_config_sets_idempotence_and_durability() -> None:
    config = producer_config("redpanda:9092", queue_max_messages=123)

    assert config["bootstrap.servers"] == "redpanda:9092"
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["linger.ms"] == 20
    assert config["compression.type"] == "zstd"
    assert config["queue.buffering.max.messages"] == 123


def test_produce_forwards_topic_key_value_timestamp_and_encoded_headers() -> None:
    resilient, raw, _ = _producer()

    resilient.produce(
        topic=EVENTS_TOPIC,
        key="nc75468637",
        payload=b"payload",
        timestamp_ms=1_730_000_000_000,
        headers={"source": "usgs", "ingested_at_ms": "1730000000500"},
    )

    assert len(raw.calls) == 1
    call = raw.calls[0]
    assert call["topic"] == EVENTS_TOPIC
    assert call["key"] == b"nc75468637"
    assert call["value"] == b"payload"
    assert call["timestamp"] == 1_730_000_000_000
    assert call["headers"] == [
        ("source", b"usgs"),
        ("ingested_at_ms", b"1730000000500"),
    ]


def test_buffer_errors_are_retried_without_losing_the_message() -> None:
    raw = FakeProducer(buffer_errors=3)
    metrics = FakeMetrics()
    resilient = ResilientProducer(raw, metrics)

    resilient.produce(
        topic=EVENTS_TOPIC,
        key="nc1",
        payload=b"payload",
        timestamp_ms=1,
        headers={},
    )

    assert metrics.backpressure_waits == 3
    assert raw.polls == [0.2, 0.2, 0.2]
    assert len(raw.calls) == 1
    assert raw.calls[0]["key"] == b"nc1"


def test_delivery_errors_surface_from_flush() -> None:
    resilient, raw, _ = _producer()
    resilient.produce(topic=EVENTS_TOPIC, key="nc1", payload=b"p", timestamp_ms=1, headers={})
    raw.fail_delivery()

    with pytest.raises(DeliveryError, match="broker unavailable"):
        resilient.flush(timeout=5.0)
    assert raw.flushes == [5.0]


def test_flush_waits_indefinitely_by_default_and_accepts_timeout() -> None:
    resilient, raw, _ = _producer()

    resilient.flush()
    resilient.flush(2.5)

    assert raw.flushes == [-1.0, 2.5]


def test_flush_reports_messages_left_in_the_queue() -> None:
    resilient, raw, _ = _producer(fake=FakeProducer(flush_remaining=2))

    with pytest.raises(DeliveryError, match="2 message"):
        resilient.flush(timeout=0.1)
    assert raw.flushes == [0.1]


def test_event_publisher_serializes_with_event_key_and_time() -> None:
    resilient, raw, _ = _producer()
    serializer = FakeCodec()
    publisher = KafkaEventPublisher(resilient, serializer)
    event = make_event(event_time_ms=1_730_000_123_000)

    publisher.publish(event, {"source": "usgs", "ingested_at_ms": "1730000124000"})
    publisher.flush(3.0)

    assert serializer.values == [event]
    call = raw.calls[0]
    assert call["topic"] == EVENTS_TOPIC
    assert call["key"] == event.event_id.encode("utf-8")
    assert call["timestamp"] == event.event_time_ms
    assert call["value"] == b"encoded"
    assert call["headers"] == [
        ("source", b"usgs"),
        ("ingested_at_ms", b"1730000124000"),
    ]
    assert raw.flushes == [3.0]


def test_dead_letter_publisher_uses_the_dlq_topic_and_dlq_id_key() -> None:
    resilient, raw, _ = _producer()
    serializer = FakeCodec(b"letter")
    publisher = KafkaDeadLetterPublisher(resilient, serializer)
    letter = DeadLetter(
        dlq_id="0123456789abcdef0123456789abcdef",
        stage=DlqStage.INGEST_VALIDATION,
        error_type="missing",
        error_message="bad",
        source_topic=None,
        source_partition=None,
        source_offset=None,
        payload=b"{}",
        failed_at_ms=1_730_000_000_777,
        attempts=1,
    )

    publisher.publish(letter)
    publisher.flush()

    assert serializer.values == [letter]
    call = raw.calls[0]
    assert call["topic"] == DLQ_TOPIC
    assert call["key"] == letter.dlq_id.encode("utf-8")
    assert call["timestamp"] == letter.failed_at_ms
    assert call["value"] == b"letter"
    assert call["headers"] == []


def test_header_helpers_round_trip_strings_and_bytes() -> None:
    encoded = codecs.encode_headers({"source": "usgs", "ingested_at_ms": "123"})

    assert encoded == [("source", b"usgs"), ("ingested_at_ms", b"123")]
    assert codecs.decode_headers(encoded) == {"source": "usgs", "ingested_at_ms": "123"}
    assert codecs.decode_headers(None) == {}
    assert codecs.decode_headers([("source", b"usgs")]) == {"source": "usgs"}


def test_event_headers_helper_uses_the_clock_value() -> None:
    event = make_event()

    headers = codecs.event_headers(event, ingested_at_ms=1_730_000_000_999)

    assert headers == {"source": "usgs", "ingested_at_ms": "1730000000999"}
    assert codecs.INGESTED_AT_HEADER == "ingested_at_ms"
    assert codecs.SOURCE_HEADER == "source"
    assert isinstance(headers, Mapping)
