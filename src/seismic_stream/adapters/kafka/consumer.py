"""Kafka batch consumer used by the sink.

The consumer owns the transport details that the sink must not know: it decodes
message headers, drains up to a fixed number of records per call, pauses and
resumes every assigned partition, and commits explicit next offsets. Manual
commits (``enable.auto.commit=false``) are what make at-least-once delivery plus
the sink's idempotent writes equal "effectively-once" (ADR 0005).
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from typing import Any, Final

from confluent_kafka import Consumer, KafkaException, TopicPartition

from seismic_stream.adapters.kafka.codecs import decode_headers
from seismic_stream.application.ports import ConsumedRecord

AUTO_OFFSET_RESET: Final = "earliest"
CONSUMER_CLIENT_ID: Final = "seismic-sink"


def consumer_config(bootstrap_servers: str, group_id: str) -> dict[str, Any]:
    """Return confluent-kafka consumer settings with manual commits enabled."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "client.id": CONSUMER_CLIENT_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": AUTO_OFFSET_RESET,
        "enable.partition.eof": False,
    }


class KafkaBatchConsumer:
    """Batched, pausable consumer over the four pipeline topics (D9)."""

    def __init__(self, consumer: Consumer) -> None:
        self._consumer = consumer
        self._paused = False

    @classmethod
    def from_config(cls, bootstrap_servers: str, group_id: str) -> KafkaBatchConsumer:
        """Build a consumer from the shared configuration."""
        return cls(Consumer(consumer_config(bootstrap_servers, group_id)))

    def subscribe(self, topics: Iterable[str]) -> None:
        """Subscribe to the sink's topics."""
        self._consumer.subscribe(list(topics))

    def poll_batch(self, max_records: int, max_wait_seconds: float) -> list[ConsumedRecord]:
        """Drain up to ``max_records`` messages, waiting at most the given time."""
        records: list[ConsumedRecord] = []
        deadline = time.monotonic() + max_wait_seconds
        while len(records) < max_records:
            remaining = max(0.0, deadline - time.monotonic())
            message = self._consumer.poll(remaining)
            if message is None:
                break
            error = message.error()
            if error is not None:
                raise KafkaException(error)
            value = message.value()
            records.append(
                ConsumedRecord(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key=message.key(),
                    value=value if isinstance(value, bytes) else b"",
                    headers=decode_headers(message.headers()),
                    timestamp_ms=_timestamp_ms(message.timestamp()),
                )
            )
            if remaining == 0:
                break
        return records

    def pause(self) -> None:
        """Pause every assigned partition (idempotent)."""
        assignment = self._consumer.assignment()
        if assignment:
            self._consumer.pause(assignment)
        self._paused = True

    def resume(self) -> None:
        """Resume the partitions paused by :meth:`pause`."""
        assignment = self._consumer.assignment()
        if assignment:
            self._consumer.resume(assignment)
        self._paused = False

    def commit(self, offsets: Mapping[tuple[str, int], int]) -> None:
        """Commit next offsets synchronously, keyed by ``(topic, partition)``."""
        if not offsets:
            return
        partitions = [
            TopicPartition(topic, partition, offset)
            for (topic, partition), offset in offsets.items()
        ]
        self._consumer.commit(offsets=partitions, asynchronous=False)

    def close(self) -> None:
        """Close the consumer, committing nothing implicitly."""
        self._consumer.close()


def _timestamp_ms(timestamp: tuple[int, int] | None) -> int | None:
    if timestamp is None:
        return None
    value = timestamp[1]
    return int(value) if value >= 0 else None
