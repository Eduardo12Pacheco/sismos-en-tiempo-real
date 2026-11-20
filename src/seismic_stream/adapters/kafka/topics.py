"""Topic names, retention policies and Schema Registry subjects.

The plan fixes four single-partition topics (D5). Keeping their names and
retention in one module avoids string drift between the admin, codecs,
bootstrap and tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

SINK_CONSUMER_GROUP: Final = "seismic-sink"
PROCESSOR_CONSUMER_GROUP: Final = "seismic-processor"

EVENTS_TOPIC: Final = "seismic.events.v1"
AGGREGATES_TOPIC: Final = "seismic.aggregates.v1"
ALERTS_TOPIC: Final = "seismic.alerts.v1"
DLQ_TOPIC: Final = "seismic.dlq.v1"

RETENTION_7_DAYS_MS: Final = 604_800_000
RETENTION_30_DAYS_MS: Final = 2_592_000_000


@dataclass(frozen=True, slots=True)
class TopicSpec:
    """One topic to create with a retention policy in milliseconds."""

    name: str
    retention_ms: int


TOPIC_SPECS: Final = (
    TopicSpec(EVENTS_TOPIC, RETENTION_7_DAYS_MS),
    TopicSpec(AGGREGATES_TOPIC, RETENTION_7_DAYS_MS),
    TopicSpec(ALERTS_TOPIC, RETENTION_30_DAYS_MS),
    TopicSpec(DLQ_TOPIC, RETENTION_30_DAYS_MS),
)

SUBJECTS: Final = tuple(f"{spec.name}-value" for spec in TOPIC_SPECS)
