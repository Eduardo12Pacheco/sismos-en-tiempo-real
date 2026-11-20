"""Unit tests for the Kafka lag reader adapter (S14)."""

from __future__ import annotations

from types import SimpleNamespace

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

from seismic_stream.adapters.kafka.admin import KafkaLagReader


class FakeFuture:
    def __init__(self, result: ConsumerGroupTopicPartitions) -> None:
        self._result = result

    def result(self, timeout: float | None = None) -> ConsumerGroupTopicPartitions:
        return self._result


class FakeAdmin:
    def __init__(self, results: dict[str, ConsumerGroupTopicPartitions]) -> None:
        self.results = results
        self.requests: list[list[ConsumerGroupTopicPartitions]] = []

    def list_consumer_group_offsets(
        self, request: list[ConsumerGroupTopicPartitions], **kwargs: object
    ) -> dict[str, FakeFuture]:
        self.requests.append(request)
        return {item.group_id: FakeFuture(self.results[item.group_id]) for item in request}


class FakeConsumer:
    def __init__(
        self,
        partitions: dict[str, list[int]],
        watermarks: dict[tuple[str, int], tuple[int, int]],
    ) -> None:
        self.partitions = partitions
        self.watermarks = watermarks
        self.closed = False
        self.listed = 0

    def list_topics(self, timeout: float | None = None) -> SimpleNamespace:
        self.listed += 1
        topics = {
            name: SimpleNamespace(
                partitions=dict.fromkeys(numbers, SimpleNamespace()),
                error=None,
            )
            for name, numbers in self.partitions.items()
        }
        return SimpleNamespace(topics=topics)

    def get_watermark_offsets(
        self, partition: TopicPartition, timeout: float | None = None, cached: bool = False
    ) -> tuple[int, int]:
        return self.watermarks[(partition.topic, partition.partition)]

    def close(self) -> None:
        self.closed = True


def test_end_offsets_sum_every_partition_of_every_configured_topic() -> None:
    consumer = FakeConsumer(
        {"seismic.events.v1": [0, 1], "seismic.alerts.v1": [0]},
        {
            ("seismic.events.v1", 0): (0, 100),
            ("seismic.events.v1", 1): (5, 30),
            ("seismic.alerts.v1", 0): (0, 0),
        },
    )
    reader = KafkaLagReader(
        "redpanda:9092",
        ("seismic.events.v1", "seismic.alerts.v1"),
        admin=FakeAdmin({}),
        consumer=consumer,
    )

    assert reader.end_offsets() == {
        ("seismic.events.v1", 0): 100,
        ("seismic.events.v1", 1): 30,
        ("seismic.alerts.v1", 0): 0,
    }
    # Partition metadata is cached: one metadata refresh for both calls.
    reader.end_offsets()
    assert consumer.listed == 1


def test_consumer_offsets_skip_partitions_without_a_committed_offset() -> None:
    consumer = FakeConsumer({"seismic.events.v1": [0, 1]}, {})
    result = ConsumerGroupTopicPartitions(
        "seismic-sink",
        [
            TopicPartition("seismic.events.v1", 0, 41),
            TopicPartition("seismic.events.v1", 1, -1001),
        ],
    )
    admin = FakeAdmin({"seismic-sink": result})
    reader = KafkaLagReader(
        "redpanda:9092",
        ("seismic.events.v1",),
        admin=admin,
        consumer=consumer,
    )

    offsets = reader.consumer_offsets("seismic-sink")

    assert offsets[("seismic.events.v1", 0)] == 41
    # The -1001 sentinel means "no committed offset"; the monitor treats it as 0.
    assert offsets[("seismic.events.v1", 1)] == -1001
    assert len(admin.requests) == 1
    assert [tp.partition for tp in admin.requests[0][0].topic_partitions] == [0, 1]


def test_missing_topics_are_ignored_until_bootstrap_creates_them() -> None:
    consumer = FakeConsumer({"seismic.events.v1": [0]}, {("seismic.events.v1", 0): (0, 4)})
    reader = KafkaLagReader(
        "redpanda:9092",
        ("seismic.events.v1", "seismic.alerts.v1"),
        admin=FakeAdmin({}),
        consumer=consumer,
    )

    assert reader.end_offsets() == {("seismic.events.v1", 0): 4}


def test_close_releases_the_consumer() -> None:
    consumer = FakeConsumer({}, {})
    reader = KafkaLagReader("redpanda:9092", (), admin=FakeAdmin({}), consumer=consumer)

    reader.close()

    assert consumer.closed is True
