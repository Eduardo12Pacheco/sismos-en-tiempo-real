"""Deterministic chaos decorator for replay sources.

The decorator never removes or reorders source records: after each record it may
append an exact duplicate, a malformed copy, or a revision of the same event.
All decisions come from a seeded ``random.Random`` instance, and
:meth:`ChaosPolicy.plan` walks the very same code path so tests can derive the
expected injection counts without duplicating the logic.
"""

from __future__ import annotations

import random
from collections.abc import Iterable, Iterator, Mapping
from copy import deepcopy
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Final

from seismic_stream.adapters.sources.replay import event_time_ms_from_feature
from seismic_stream.application.ports import EventSource, SourceRecord
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.normalize import feature_to_event

REVISION_DELAY_MS: Final = 60_000
REVISION_MAGNITUDE_STEP: Final = 0.1
MAX_MAGNITUDE: Final = 10.0
BAD_MAGNITUDE: Final = 99.0


class MalformedKind(StrEnum):
    """Ways a malformed copy can break USGS feature validation."""

    MISSING_MAG = "missing_mag"
    BAD_COORDINATES = "bad_coordinates"
    MAG_OUT_OF_RANGE = "mag_out_of_range"
    NON_NUMERIC_TIME = "non_numeric_time"
    EMPTY_ID = "empty_id"


MALFORMED_KINDS: Final[tuple[MalformedKind, ...]] = (
    MalformedKind.MISSING_MAG,
    MalformedKind.BAD_COORDINATES,
    MalformedKind.MAG_OUT_OF_RANGE,
    MalformedKind.NON_NUMERIC_TIME,
    MalformedKind.EMPTY_ID,
)


class InjectionKind(StrEnum):
    """Kinds of records the decorator may append."""

    MALFORMED = "malformed"
    DUPLICATE = "duplicate"
    REVISION = "revision"


@dataclass(frozen=True, slots=True)
class ChaosPlan:
    """Counts and identifiers produced by a deterministic chaos policy."""

    malformed: int = 0
    duplicates: int = 0
    revisions: int = 0
    revised_ids: frozenset[str] = field(default_factory=frozenset)

    @property
    def injected(self) -> int:
        """Total number of appended records."""
        return self.malformed + self.duplicates + self.revisions


@dataclass(frozen=True, slots=True)
class ChaosPolicy:
    """Seeded injection probabilities and the plan derived from them."""

    seed: int = 42
    malformed_rate: float = 0.02
    duplicate_rate: float = 0.05
    revision_rate: float = 0.02

    def __post_init__(self) -> None:
        for name in ("malformed_rate", "duplicate_rate", "revision_rate"):
            value = getattr(self, name)
            if not 0.0 <= value <= 1.0:
                raise ValueError(f"{name} must be within [0, 1], got {value!r}")

    def plan(self, records: Iterable[SourceRecord]) -> ChaosPlan:
        """Walk ``records`` with the same algorithm as :meth:`ChaoticSource.records`."""
        malformed = 0
        duplicates = 0
        revisions = 0
        revised_ids: set[str] = set()

        for _, injections in _walk(records, self):
            for kind, record in injections:
                if kind is InjectionKind.MALFORMED:
                    malformed += 1
                elif kind is InjectionKind.DUPLICATE:
                    duplicates += 1
                else:
                    revisions += 1
                    event_id = record.feature.get("id")
                    if isinstance(event_id, str):
                        revised_ids.add(event_id)

        return ChaosPlan(
            malformed=malformed,
            duplicates=duplicates,
            revisions=revisions,
            revised_ids=frozenset(revised_ids),
        )


class ChaoticSource:
    """Decorator adding deterministic malformed copies, duplicates and revisions."""

    def __init__(self, inner: EventSource, policy: ChaosPolicy) -> None:
        self._inner = inner
        self._policy = policy

    @property
    def policy(self) -> ChaosPolicy:
        """Policy applied by this decorator."""
        return self._policy

    def records(self) -> Iterator[SourceRecord]:
        """Yield every source record followed by its injected variants."""
        for record, injections in _walk(self._inner.records(), self._policy):
            yield record
            for _, injected in injections:
                yield injected


Injection = tuple[InjectionKind, SourceRecord]


def _walk(
    records: Iterable[SourceRecord],
    policy: ChaosPolicy,
) -> Iterator[tuple[SourceRecord, list[Injection]]]:
    rng = random.Random(policy.seed)
    malformed_index = 0

    for record in records:
        injections: list[Injection] = []

        if rng.random() < policy.malformed_rate:
            kind = MALFORMED_KINDS[malformed_index % len(MALFORMED_KINDS)]
            malformed_index += 1
            injections.append((InjectionKind.MALFORMED, _malformed_record(record, kind)))

        if rng.random() < policy.duplicate_rate:
            injections.append((InjectionKind.DUPLICATE, _duplicate_record(record)))

        wants_revision = rng.random() < policy.revision_rate
        if wants_revision:
            revision = _revision_record(record)
            if revision is not None:
                injections.append((InjectionKind.REVISION, revision))

        yield record, injections


def _duplicate_record(record: SourceRecord) -> SourceRecord:
    return SourceRecord(
        feature=deepcopy(dict(record.feature)),
        event_time_ms=record.event_time_ms,
        injected=True,
    )


def _malformed_record(record: SourceRecord, kind: MalformedKind) -> SourceRecord:
    feature = _mutable_feature(record.feature)
    properties = _mutable_properties(feature)

    if kind is MalformedKind.MISSING_MAG:
        properties.pop("mag", None)
    elif kind is MalformedKind.BAD_COORDINATES:
        feature["geometry"] = {"type": "Point", "coordinates": _two_coordinates(record.feature)}
    elif kind is MalformedKind.MAG_OUT_OF_RANGE:
        properties["mag"] = BAD_MAGNITUDE
    elif kind is MalformedKind.NON_NUMERIC_TIME:
        properties["time"] = "yesterday"
    else:
        feature["id"] = ""

    return SourceRecord(
        feature=feature,
        event_time_ms=event_time_ms_from_feature(feature),
        injected=True,
    )


def _revision_record(record: SourceRecord) -> SourceRecord | None:
    try:
        event = feature_to_event(record.feature)
    except EventValidationError:
        return None
    if event.event_type != "earthquake":
        return None

    feature = _mutable_feature(record.feature)
    properties = _mutable_properties(feature)
    properties["updated"] = event.updated_at_ms + REVISION_DELAY_MS
    properties["mag"] = round(
        min(event.magnitude + REVISION_MAGNITUDE_STEP, MAX_MAGNITUDE),
        2,
    )
    return SourceRecord(
        feature=feature,
        event_time_ms=record.event_time_ms,
        injected=True,
    )


def _mutable_feature(feature: Mapping[str, Any]) -> dict[str, Any]:
    return deepcopy(dict(feature))


def _mutable_properties(feature: dict[str, Any]) -> dict[str, Any]:
    properties = feature.get("properties")
    if isinstance(properties, dict):
        return properties
    empty: dict[str, Any] = {}
    feature["properties"] = empty
    return empty


def _two_coordinates(feature: Mapping[str, Any]) -> list[Any]:
    geometry = feature.get("geometry")
    if isinstance(geometry, Mapping):
        coordinates = geometry.get("coordinates")
        if isinstance(coordinates, list) and len(coordinates) >= 2:
            return [coordinates[0], coordinates[1]]
    return [0.0, 0.0]
