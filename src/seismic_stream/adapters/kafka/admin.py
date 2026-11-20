"""Kafka topic administration.

Topics are created with one partition (D5) because event-time order must be
preserved end to end. Creation is idempotent: an existing topic is reported as
``unchanged`` and never altered.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from confluent_kafka import (
    Consumer,
    ConsumerGroupTopicPartitions,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic

from seismic_stream.adapters.kafka.topics import TopicSpec

DEFAULT_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True, slots=True)
class TopicAdminResult:
    """Outcome of one ``ensure_topics`` call, split for idempotent reporting."""

    created: tuple[str, ...]
    unchanged: tuple[str, ...]

    def as_dict(self) -> dict[str, list[str]]:
        """Render the result for the bootstrap JSON summary."""
        return {"created": list(self.created), "unchanged": list(self.unchanged)}


def _already_exists(error: KafkaException) -> bool:
    kafka_error = error.args[0] if error.args else None
    if not isinstance(kafka_error, KafkaError):
        return False
    return bool(kafka_error.code() == KafkaError._TOPIC_ALREADY_EXISTS)


class TopicAdmin:
    """Create missing topics and leave existing ones untouched."""

    def __init__(
        self,
        bootstrap_servers: str,
        admin: AdminClient | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._admin = admin or AdminClient(
            {"bootstrap.servers": bootstrap_servers, "client.id": "seismic-bootstrap"}
        )
        self._timeout = timeout

    def ensure_topics(self, specs: Iterable[TopicSpec]) -> TopicAdminResult:
        """Create every missing topic; return created and unchanged names."""
        spec_list = tuple(specs)
        existing = set(self._admin.list_topics(timeout=self._timeout).topics)
        created: list[str] = []
        unchanged: list[str] = []
        new_topics: list[NewTopic] = []
        for spec in spec_list:
            if spec.name in existing:
                unchanged.append(spec.name)
                continue
            new_topics.append(
                NewTopic(
                    topic=spec.name,
                    num_partitions=1,
                    replication_factor=1,
                    config={"retention.ms": str(spec.retention_ms)},
                )
            )

        if new_topics:
            futures = self._admin.create_topics(new_topics, request_timeout=self._timeout)
            for name, future in futures.items():
                try:
                    future.result(timeout=self._timeout)
                except KafkaException as error:
                    if not _already_exists(error):
                        raise
                    unchanged.append(name)
                else:
                    created.append(name)

        order = {spec.name: index for index, spec in enumerate(spec_list)}
        return TopicAdminResult(
            created=tuple(sorted(created, key=order.__getitem__)),
            unchanged=tuple(sorted(unchanged, key=order.__getitem__)),
        )


class KafkaLagReader:
    """Read committed and end offsets through the Kafka admin and consumer APIs.

    ``AdminClient.list_consumer_group_offsets`` reports the last committed offset
    per partition (``-1001`` when a group never committed), while
    ``Consumer.get_watermark_offsets`` reports the log end. The monitor combines
    both; this adapter only exposes the raw offset maps.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topics: Iterable[str],
        *,
        admin: AdminClient | None = None,
        consumer: Consumer | None = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        self._topics = tuple(topics)
        self._timeout = timeout
        self._admin = admin or AdminClient(
            {"bootstrap.servers": bootstrap_servers, "client.id": "seismic-monitor"}
        )
        self._consumer = consumer or Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "seismic-monitor-probe",
                "client.id": "seismic-monitor",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self._partitions: tuple[tuple[str, int], ...] | None = None

    def _topic_partitions(self) -> tuple[tuple[str, int], ...]:
        if self._partitions is None:
            metadata = self._consumer.list_topics(timeout=self._timeout)
            partitions: list[tuple[str, int]] = []
            for topic in self._topics:
                description = metadata.topics.get(topic)
                if description is None or description.error is not None:
                    continue
                partitions.extend(
                    (topic, partition) for partition in sorted(description.partitions)
                )
            self._partitions = tuple(partitions)
        return self._partitions

    def end_offsets(self) -> dict[tuple[str, int], int]:
        """Return the log end offset per partition of the configured topics."""
        offsets: dict[tuple[str, int], int] = {}
        for topic, partition in self._topic_partitions():
            _low, high = self._consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=self._timeout,
            )
            offsets[(topic, partition)] = max(0, int(high))
        return offsets

    def consumer_offsets(self, group: str) -> dict[tuple[str, int], int]:
        """Return the last committed offset per partition for ``group``."""
        partitions = self._topic_partitions()
        if not partitions:
            return {}
        request = ConsumerGroupTopicPartitions(
            group,
            [TopicPartition(topic, partition) for topic, partition in partitions],
        )
        futures = self._admin.list_consumer_group_offsets(
            [request],
            request_timeout=self._timeout,
        )
        result = futures[group].result(timeout=self._timeout)
        return {
            (partition.topic, partition.partition): int(partition.offset)
            for partition in result.topic_partitions
            if partition.error is None
        }

    def close(self) -> None:
        """Release the probe consumer (the admin client has no close)."""
        self._consumer.close()
