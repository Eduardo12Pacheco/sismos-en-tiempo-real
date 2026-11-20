"""Property-based invariants for replay pacing and chaos injection."""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from seismic_stream.adapters.sources.chaos import ChaosPolicy, ChaoticSource
from seismic_stream.application.pacing import ReplayPacer
from seismic_stream.application.ports import SourceRecord
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.normalize import feature_to_event
from seismic_stream.domain.versioning import supersedes
from tests.support.factories import make_feature
from tests.support.fakes import FakeClock, FakeSource

SPEEDS = st.floats(min_value=0.1, max_value=1_000.0, allow_nan=False, allow_infinity=False)
DELTAS = st.lists(st.integers(min_value=0, max_value=1_000_000), min_size=0, max_size=30)
RATES = st.floats(min_value=0.0, max_value=1.0, allow_nan=False, allow_infinity=False)
TIMES = st.integers(min_value=1, max_value=10**12)
EVENT_TYPES = st.sampled_from(["earthquake", "quarry blast", "ice quake", "explosion"])
MAGNITUDES = st.floats(min_value=-2.0, max_value=9.9, allow_nan=False, allow_infinity=False)


@st.composite
def feature_lists(draw: st.DrawFn) -> list[dict[str, object]]:
    """Arbitrary lists of valid, uniquely identified USGS-like features."""
    count = draw(st.integers(min_value=1, max_value=10))
    features: list[dict[str, object]] = []
    for index in range(count):
        event_time_ms = draw(TIMES)
        features.append(
            make_feature(
                id=f"ev{index:03d}",
                properties={
                    "time": event_time_ms,
                    "updated": event_time_ms + draw(st.integers(min_value=0, max_value=10**6)),
                    "mag": draw(MAGNITUDES),
                    "type": draw(EVENT_TYPES),
                },
            )
        )
    return features


@st.composite
def source_records(draw: st.DrawFn) -> list[SourceRecord]:
    """Arbitrary records backed by valid features."""
    features = draw(feature_lists())
    return [
        SourceRecord(feature=feature, event_time_ms=feature["properties"]["time"])  # type: ignore[index]
        for feature in features
    ]


@given(
    start=st.integers(min_value=0, max_value=10**12),
    deltas=DELTAS,
    speed=SPEEDS,
)
def test_total_sleep_equals_compressed_event_span(
    start: int, deltas: list[int], speed: float
) -> None:
    times = [start]
    for delta in deltas:
        times.append(times[-1] + delta)

    clock = FakeClock(now=123.456)
    pacer = ReplayPacer(clock, speed=speed)
    for event_time_ms in times:
        pacer.wait(event_time_ms)

    expected = (times[-1] - times[0]) / 1000 / speed
    assert clock.total_slept == pytest.approx(expected, abs=1e-6)
    assert clock.sleeps == [sleep for sleep in clock.sleeps if sleep >= 0]


@given(
    start=st.integers(min_value=0, max_value=10**12),
    deltas=DELTAS,
)
def test_speed_none_never_sleeps(start: int, deltas: list[int]) -> None:
    clock = FakeClock()
    pacer = ReplayPacer(clock, speed=None)

    times = [start]
    for delta in deltas:
        times.append(times[-1] + delta)
    for event_time_ms in times:
        pacer.wait(event_time_ms)

    assert clock.sleeps == []
    assert pacer.schedule_lag_seconds == 0.0


@given(records=source_records(), seed=st.integers(min_value=0, max_value=2**32 - 1))
def test_same_seed_reproduces_plan_and_stream(records: list[SourceRecord], seed: int) -> None:
    policy = ChaosPolicy(
        seed=seed,
        malformed_rate=0.2,
        duplicate_rate=0.3,
        revision_rate=0.2,
    )

    first_plan = policy.plan(records)
    second_plan = policy.plan(records)
    first = _fingerprint(list(ChaoticSource(FakeSource(records), policy).records()))
    second = _fingerprint(list(ChaoticSource(FakeSource(records), policy).records()))

    assert first_plan == second_plan
    assert first == second


@given(
    records=source_records(),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
    malformed_rate=RATES,
    duplicate_rate=RATES,
    revision_rate=RATES,
)
def test_chaos_only_adds_records(
    records: list[SourceRecord],
    seed: int,
    malformed_rate: float,
    duplicate_rate: float,
    revision_rate: float,
) -> None:
    policy = ChaosPolicy(
        seed=seed,
        malformed_rate=malformed_rate,
        duplicate_rate=duplicate_rate,
        revision_rate=revision_rate,
    )
    plan = policy.plan(records)
    output = list(ChaoticSource(FakeSource(records), policy).records())

    assert len(output) == len(records) + plan.injected
    clean = [record for record in output if not record.injected]
    assert _fingerprint(clean) == _fingerprint(records)


@given(
    records=source_records(),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
    malformed_rate=RATES,
    duplicate_rate=RATES,
    revision_rate=RATES,
)
def test_only_malformed_injections_fail_validation(
    records: list[SourceRecord],
    seed: int,
    malformed_rate: float,
    duplicate_rate: float,
    revision_rate: float,
) -> None:
    policy = ChaosPolicy(
        seed=seed,
        malformed_rate=malformed_rate,
        duplicate_rate=duplicate_rate,
        revision_rate=revision_rate,
    )
    plan = policy.plan(records)
    output = list(ChaoticSource(FakeSource(records), policy).records())

    invalid = [record for record in output if _invalid(record.feature)]
    assert len(invalid) == plan.malformed
    assert all(record.injected for record in invalid)


@given(records=source_records(), seed=st.integers(min_value=0, max_value=2**32 - 1))
def test_duplicates_equal_their_source(records: list[SourceRecord], seed: int) -> None:
    policy = ChaosPolicy(seed=seed, malformed_rate=0, duplicate_rate=1, revision_rate=0)

    output = list(ChaoticSource(FakeSource(records), policy).records())

    assert len(output) == 2 * len(records)
    for source_record, duplicate in zip(output[::2], output[1::2], strict=True):
        assert duplicate == source_record
        assert duplicate.feature == source_record.feature
        assert duplicate.injected is True
        assert source_record.injected is False


@given(records=source_records(), seed=st.integers(min_value=0, max_value=2**32 - 1))
def test_revisions_supersede_their_source(records: list[SourceRecord], seed: int) -> None:
    policy = ChaosPolicy(seed=seed, malformed_rate=0, duplicate_rate=0, revision_rate=1)

    plan = policy.plan(records)
    output = list(ChaoticSource(FakeSource(records), policy).records())

    sources = {
        event.event_id: event
        for event in (feature_to_event(record.feature) for record in records)
        if event.event_type == "earthquake"
    }
    revisions = [record for record in output if record.injected]
    assert len(revisions) == plan.revisions
    assert plan.revised_ids == frozenset(sources)

    for revision in revisions:
        event = feature_to_event(revision.feature)
        original = sources[event.event_id]
        assert supersedes(event.updated_at_ms, original.updated_at_ms)
        assert event.event_time_ms == original.event_time_ms


def _fingerprint(records: list[SourceRecord]) -> list[tuple[dict[str, object], int | None, bool]]:
    return [(record.feature, record.event_time_ms, record.injected) for record in records]


def _invalid(feature: dict[str, object]) -> bool:
    try:
        feature_to_event(feature)  # type: ignore[arg-type]
    except EventValidationError:
        return True
    return False
