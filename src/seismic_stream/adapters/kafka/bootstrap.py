"""Idempotent bootstrap for topics, Avro subjects and compatibility.

``bootstrap`` is the only component allowed to register schemas (D4). Running it
twice must be safe: the second run reports every resource as ``unchanged``.
ClickHouse DDL is applied by the storage adapter added with the sink; until then
the summary records that step as skipped.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Final, Protocol

from confluent_kafka.schema_registry import SchemaRegistryClient

from seismic_stream.adapters.kafka import codecs
from seismic_stream.adapters.kafka.admin import TopicAdmin, TopicAdminResult
from seismic_stream.adapters.kafka.registry import COMPATIBILITY_LEVEL, SchemaRegistryAdmin
from seismic_stream.adapters.kafka.topics import (
    AGGREGATES_TOPIC,
    ALERTS_TOPIC,
    DLQ_TOPIC,
    EVENTS_TOPIC,
    SUBJECTS,
    TOPIC_SPECS,
    TopicSpec,
)
from seismic_stream.config import Settings

SUBJECT_SCHEMAS: Final[Mapping[str, str]] = {
    f"{EVENTS_TOPIC}-value": codecs.EVENT_SCHEMA_FILE,
    f"{AGGREGATES_TOPIC}-value": codecs.WINDOW_STATS_SCHEMA_FILE,
    f"{ALERTS_TOPIC}-value": codecs.ALERT_SCHEMA_FILE,
    f"{DLQ_TOPIC}-value": codecs.DEAD_LETTER_SCHEMA_FILE,
}


class TopicEnsurer(Protocol):
    """Adapter boundary used by :func:`run` (a fake in unit tests)."""

    def ensure_topics(self, specs: Iterable[TopicSpec]) -> TopicAdminResult:
        """Create missing topics and report the outcome."""
        ...


class SubjectEnsurer(Protocol):
    """Adapter boundary for schema registration and compatibility."""

    def ensure_subject(self, subject: str, schema: str) -> str:
        """Register or verify one subject."""
        ...

    def set_compatibility(self, subject: str, level: str = COMPATIBILITY_LEVEL) -> str:
        """Set the subject compatibility level."""
        ...


@dataclass(frozen=True, slots=True)
class SubjectResult:
    """One subject registration outcome."""

    subject: str
    action: str


@dataclass(frozen=True, slots=True)
class CompatibilityResult:
    """One subject compatibility outcome."""

    subject: str
    level: str
    action: str


@dataclass(frozen=True, slots=True)
class BootstrapResult:
    """Everything the bootstrap command did, ready to print as JSON."""

    topics: TopicAdminResult
    subjects: tuple[SubjectResult, ...]
    compatibility: tuple[CompatibilityResult, ...]
    clickhouse: str = "skipped"

    def as_dict(self) -> dict[str, object]:
        """Render a stable, machine-readable summary."""
        return {
            "topics": self.topics.as_dict(),
            "subjects": {
                "created": [
                    result.subject for result in self.subjects if result.action == "created"
                ],
                "unchanged": [
                    result.subject for result in self.subjects if result.action == "unchanged"
                ],
            },
            "compatibility": {
                "updated": [
                    result.subject for result in self.compatibility if result.action == "updated"
                ],
                "unchanged": [
                    result.subject for result in self.compatibility if result.action == "unchanged"
                ],
            },
            "clickhouse": self.clickhouse,
        }


def run(
    settings: Settings,
    *,
    topics: TopicEnsurer | None = None,
    registry: SubjectEnsurer | None = None,
) -> BootstrapResult:
    """Create topics, register Avro subjects and enforce BACKWARD compatibility."""
    topic_admin = topics or TopicAdmin(settings.kafka_bootstrap)
    registry_admin = registry or SchemaRegistryAdmin(
        SchemaRegistryClient({"url": settings.schema_registry_url})
    )

    topic_result = topic_admin.ensure_topics(TOPIC_SPECS)

    subject_results: list[SubjectResult] = []
    compatibility_results: list[CompatibilityResult] = []
    for subject in SUBJECTS:
        schema = codecs.load_schema(SUBJECT_SCHEMAS[subject])
        subject_results.append(
            SubjectResult(subject=subject, action=registry_admin.ensure_subject(subject, schema))
        )
        compatibility_results.append(
            CompatibilityResult(
                subject=subject,
                level=COMPATIBILITY_LEVEL,
                action=registry_admin.set_compatibility(subject, COMPATIBILITY_LEVEL),
            )
        )

    return BootstrapResult(
        topics=topic_result,
        subjects=tuple(subject_results),
        compatibility=tuple(compatibility_results),
        clickhouse=_ensure_storage(),
    )


def _ensure_storage() -> str:
    """Hook for the ClickHouse DDL step; the sink adapter supplies it in T9."""
    return "skipped"
