"""Chaos decorator contract: deterministic, additive fault injection."""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

import pytest

from seismic_stream.adapters.sources.chaos import (
    ChaosPlan,
    ChaosPolicy,
    ChaoticSource,
    MalformedKind,
)
from seismic_stream.application.ports import SourceRecord
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.normalize import feature_to_event
from seismic_stream.domain.versioning import supersedes
from tests.support.factories import make_feature
from tests.support.fakes import FakeSource

BASE_TIME_MS = 1_730_000_000_000


def _source(records: Iterable[SourceRecord]) -> FakeSource:
    return FakeSource(records)


def _records(features: Iterable[dict[str, Any]]) -> list[SourceRecord]:
    return [
        SourceRecord(feature=feature, event_time_ms=feature["properties"].get("time"))
        for feature in features
    ]


def _earthquake(index: int, event_type: str = "earthquake") -> dict[str, Any]:
    return make_feature(
        id=f"ev{index:03d}",
        properties={
            "time": BASE_TIME_MS + index * 1_000,
            "updated": BASE_TIME_MS + index * 1_000 + 60_000,
            "mag": 4.0 + index / 100,
            "type": event_type,
        },
    )


def _output(source: ChaoticSource) -> list[SourceRecord]:
    return list(source.records())


def test_zero_rates_yield_the_clean_stream() -> None:
    records = _records([_earthquake(index) for index in range(5)])
    policy = ChaosPolicy(seed=1, malformed_rate=0, duplicate_rate=0, revision_rate=0)

    source = ChaoticSource(_source(records), policy)

    assert _output(source) == records
    assert policy.plan(records) == ChaosPlan(malformed=0, duplicates=0, revisions=0)


def test_all_rates_inject_after_each_source_record() -> None:
    records = _records([_earthquake(index) for index in range(6)])
    policy = ChaosPolicy(seed=7, malformed_rate=1, duplicate_rate=1, revision_rate=1)

    output = _output(ChaoticSource(_source(records), policy))
    plan = policy.plan(records)

    assert len(output) == 24
    assert plan == ChaosPlan(
        malformed=6,
        duplicates=6,
        revisions=6,
        revised_ids=frozenset(f"ev{index:03d}" for index in range(6)),
    )
    # Source, malformed copy, duplicate, revision: the documented order.
    for index, source_record in enumerate(records):
        group = output[index * 4 : index * 4 + 4]
        assert group[0].injected is False
        assert all(record.injected for record in group[1:])
        assert group[0].feature == source_record.feature
        assert group[2].feature == source_record.feature
        assert group[2] == source_record


def test_malformed_kinds_rotate_and_always_fail_validation() -> None:
    records = _records([_earthquake(index) for index in range(5)])
    policy = ChaosPolicy(seed=3, malformed_rate=1, duplicate_rate=0, revision_rate=0)

    output = _output(ChaoticSource(_source(records), policy))
    malformed = [record for record in output if record.injected]

    assert len(malformed) == 5
    for record in malformed:
        with pytest.raises(EventValidationError):
            feature_to_event(record.feature)

    expected: list[MalformedKind] = [
        MalformedKind.MISSING_MAG,
        MalformedKind.BAD_COORDINATES,
        MalformedKind.MAG_OUT_OF_RANGE,
        MalformedKind.NON_NUMERIC_TIME,
        MalformedKind.EMPTY_ID,
    ]
    for record, kind in zip(malformed, expected, strict=True):
        properties = record.feature["properties"]
        if kind is MalformedKind.MISSING_MAG:
            assert "mag" not in properties
        elif kind is MalformedKind.BAD_COORDINATES:
            assert len(record.feature["geometry"]["coordinates"]) == 2
        elif kind is MalformedKind.MAG_OUT_OF_RANGE:
            assert properties["mag"] == 99.0
        elif kind is MalformedKind.NON_NUMERIC_TIME:
            assert properties["time"] == "yesterday"
            assert record.event_time_ms is None
        elif kind is MalformedKind.EMPTY_ID:
            assert record.feature["id"] == ""


def test_revisions_are_newer_and_bump_magnitude() -> None:
    records = _records([_earthquake(index) for index in range(4)])
    policy = ChaosPolicy(seed=11, malformed_rate=0, duplicate_rate=0, revision_rate=1)

    output = _output(ChaoticSource(_source(records), policy))
    plan = policy.plan(records)

    assert plan.revisions == 4
    assert plan.revised_ids == frozenset(f"ev{index:03d}" for index in range(4))
    for index, source_record in enumerate(records):
        revision = output[index * 2 + 1]
        original = feature_to_event(source_record.feature)
        revised = feature_to_event(revision.feature)
        assert revised.event_id == original.event_id
        assert supersedes(revised.updated_at_ms, original.updated_at_ms)
        assert revised.magnitude == pytest.approx(round(original.magnitude + 0.1, 2))


def test_revision_magnitude_is_capped_at_ten() -> None:
    feature = _earthquake(0)
    feature["properties"]["mag"] = 9.97
    records = _records([feature])
    policy = ChaosPolicy(seed=2, malformed_rate=0, duplicate_rate=0, revision_rate=1)

    output = _output(ChaoticSource(_source(records), policy))

    assert output[1].feature["properties"]["mag"] == 10.0


def test_revisions_only_target_valid_earthquakes() -> None:
    invalid = make_feature(id="invalid", properties={"time": BASE_TIME_MS})
    del invalid["properties"]["mag"]
    features = [
        _earthquake(0),
        _earthquake(1, event_type="quarry blast"),
        invalid,
        _earthquake(2),
    ]
    records = _records(features)
    policy = ChaosPolicy(seed=5, malformed_rate=0, duplicate_rate=0, revision_rate=1)

    plan = policy.plan(records)
    output = _output(ChaoticSource(_source(records), policy))

    assert plan.revisions == 2
    assert plan.revised_ids == frozenset({"ev000", "ev002"})
    assert [record.injected for record in output] == [
        False,
        True,
        False,
        False,
        False,
        True,
    ]


def test_same_seed_produces_identical_plan_and_output() -> None:
    features = [_earthquake(index) for index in range(50)]
    policy = ChaosPolicy(seed=42, malformed_rate=0.1, duplicate_rate=0.2, revision_rate=0.15)

    first = _output(ChaoticSource(_source(_records(features)), policy))
    second = _output(ChaoticSource(_source(_records(features)), policy))

    assert policy.plan(_records(features)) == policy.plan(_records(features))

    def fingerprint(record: SourceRecord) -> tuple[dict[str, Any], int | None, bool]:
        return (record.feature, record.event_time_ms, record.injected)

    assert [fingerprint(record) for record in first] == [fingerprint(record) for record in second]


def test_different_seeds_produce_different_injection_patterns() -> None:
    features = [_earthquake(index) for index in range(50)]
    records = _records(features)

    first = _output(
        ChaoticSource(
            _source(records),
            ChaosPolicy(seed=1, malformed_rate=0.1, duplicate_rate=0.2, revision_rate=0.1),
        )
    )
    second = _output(
        ChaoticSource(
            _source(records),
            ChaosPolicy(seed=2, malformed_rate=0.1, duplicate_rate=0.2, revision_rate=0.1),
        )
    )

    assert [record.injected for record in first] != [record.injected for record in second]


def test_chaos_only_adds_records_to_the_clean_stream() -> None:
    features = [_earthquake(index) for index in range(30)]
    records = _records(features)
    policy = ChaosPolicy(seed=9, malformed_rate=0.2, duplicate_rate=0.3, revision_rate=0.2)

    output = _output(ChaoticSource(_source(records), policy))
    kept = [record for record in output if not record.injected]

    assert [record.feature for record in kept] == [record.feature for record in records]
    assert [record.event_time_ms for record in kept] == [record.event_time_ms for record in records]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("malformed_rate", -0.01),
        ("duplicate_rate", 1.01),
        ("revision_rate", 2.0),
    ],
)
def test_rates_outside_unit_interval_are_rejected(field: str, value: float) -> None:
    kwargs: dict[str, float] = {field: value}

    with pytest.raises(ValueError, match=field):
        ChaosPolicy(seed=1, **kwargs)


def test_plan_matches_injected_counts_from_the_same_source() -> None:
    features = [_earthquake(index) for index in range(40)]
    records = _records(features)
    policy = ChaosPolicy(seed=17, malformed_rate=0.25, duplicate_rate=0.5, revision_rate=0.3)

    plan = policy.plan(records)
    output = _output(ChaoticSource(_source(records), policy))

    originals = {json.dumps(feature, sort_keys=True) for feature in features}
    malformed = 0
    duplicates = 0
    revisions = 0
    for record in output:
        if not record.injected:
            continue
        if _raises_validation(record.feature):
            malformed += 1
        elif json.dumps(record.feature, sort_keys=True) in originals:
            duplicates += 1
        else:
            revisions += 1
    assert (malformed, duplicates, revisions) == (
        plan.malformed,
        plan.duplicates,
        plan.revisions,
    )
    assert len(output) == len(records) + plan.injected


def _raises_validation(feature: dict[str, Any]) -> bool:
    try:
        feature_to_event(feature)
    except EventValidationError:
        return True
    return False
