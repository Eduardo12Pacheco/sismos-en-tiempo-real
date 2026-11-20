"""Unit tests for the bootstrap orchestration with in-memory admins."""

from __future__ import annotations

from seismic_stream.adapters.kafka.admin import TopicAdminResult
from seismic_stream.adapters.kafka.bootstrap import run
from seismic_stream.adapters.kafka.topics import SUBJECTS, TOPIC_SPECS, TopicSpec
from seismic_stream.config import Settings


class FakeTopicAdmin:
    def __init__(self) -> None:
        self.existing: set[str] = set()
        self.received: tuple[TopicSpec, ...] = ()

    def ensure_topics(self, specs: object) -> TopicAdminResult:
        self.received = tuple(specs)  # type: ignore[arg-type]
        created = tuple(spec.name for spec in self.received if spec.name not in self.existing)
        unchanged = tuple(spec.name for spec in self.received if spec.name in self.existing)
        self.existing.update(created)
        return TopicAdminResult(created=created, unchanged=unchanged)


class FakeRegistryAdmin:
    def __init__(self) -> None:
        self.subjects: dict[str, str] = {}

    def ensure_subject(self, subject: str, schema: str) -> str:
        if subject in self.subjects:
            assert self.subjects[subject] == schema
            return "unchanged"
        self.subjects[subject] = schema
        return "created"

    def set_compatibility(self, subject: str, level: str = "BACKWARD") -> str:
        assert level == "BACKWARD"
        return "unchanged"


def _settings() -> Settings:
    return Settings(
        kafka_bootstrap="redpanda:9092",
        schema_registry_url="http://redpanda:8081",
        _env_file=None,
    )


def test_first_run_creates_every_topic_and_subject() -> None:
    topics = FakeTopicAdmin()
    registry = FakeRegistryAdmin()

    result = run(_settings(), topics=topics, registry=registry)

    assert topics.received == TOPIC_SPECS
    assert result.topics.created == tuple(spec.name for spec in TOPIC_SPECS)
    assert result.topics.unchanged == ()
    assert result.as_dict()["subjects"] == {
        "created": list(SUBJECTS),
        "unchanged": [],
    }
    assert result.as_dict()["clickhouse"] == "skipped"
    assert set(registry.subjects) == set(SUBJECTS)


def test_second_run_reports_everything_unchanged() -> None:
    topics = FakeTopicAdmin()
    registry = FakeRegistryAdmin()
    run(_settings(), topics=topics, registry=registry)

    result = run(_settings(), topics=topics, registry=registry)

    assert result.topics.created == ()
    assert result.topics.unchanged == tuple(spec.name for spec in TOPIC_SPECS)
    assert result.as_dict()["subjects"] == {"created": [], "unchanged": list(SUBJECTS)}
