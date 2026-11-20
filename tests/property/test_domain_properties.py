"""Property-based invariants for the domain layer."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from seismic_stream.domain.alerts import AlertPolicy
from seismic_stream.domain.energy import radiated_energy_joules
from seismic_stream.domain.geography import region_from_place
from seismic_stream.domain.models import AlertLevel, Event
from seismic_stream.domain.versioning import supersedes
from tests.support.factories import make_event

MAGNITUDES = st.floats(min_value=-3.0, max_value=10.0, allow_nan=False, allow_infinity=False).map(
    lambda value: round(value, 6)
)


@st.composite
def ordered_magnitudes(draw: st.DrawFn) -> tuple[float, float]:
    low = draw(MAGNITUDES)
    high = draw(
        st.floats(min_value=low, max_value=10.0, allow_nan=False, allow_infinity=False).map(
            lambda value: round(value, 6)
        )
    )
    return low, high


@given(st.none() | st.text())
def test_region_from_place_never_returns_empty(place: str | None) -> None:
    region = region_from_place(place)

    assert region
    assert region == region.strip()


@given(ordered_magnitudes())
def test_energy_is_strictly_increasing(pair: tuple[float, float]) -> None:
    low, high = pair
    if low < high:
        assert radiated_energy_joules(low) < radiated_energy_joules(high)


@given(st.floats(min_value=-3.0, max_value=9.0, allow_nan=False, allow_infinity=False))
def test_energy_ratio_for_one_magnitude_unit(magnitude: float) -> None:
    ratio = radiated_energy_joules(magnitude + 1.0) / radiated_energy_joules(magnitude)

    assert abs(ratio - 10**1.5) <= 1e-9 * 10**1.5


@given(st.integers(), st.integers())
def test_supersedes_is_irreflexive_and_antisymmetric(first: int, second: int) -> None:
    assert supersedes(first, first) is False
    if supersedes(first, second):
        assert supersedes(second, first) is False


def _alert_rank(level: AlertLevel | None) -> int:
    if level is None:
        return 0
    return 2 if level is AlertLevel.WARNING else 1


@st.composite
def monotonic_event_pairs(draw: st.DrawFn) -> tuple[Event, Event]:
    low, high = draw(ordered_magnitudes())
    in_ecuador_region = draw(st.booleans())
    return (
        make_event(magnitude=low, tsunami=False, in_ecuador_region=in_ecuador_region),
        make_event(magnitude=high, tsunami=False, in_ecuador_region=in_ecuador_region),
    )


@given(monotonic_event_pairs())
def test_alert_level_never_decreases_with_magnitude(pair: tuple[Event, Event]) -> None:
    low_event, high_event = pair
    policy = AlertPolicy()

    low_alert = policy.evaluate(low_event)
    high_alert = policy.evaluate(high_event)

    assert _alert_rank(high_alert.level if high_alert else None) >= _alert_rank(
        low_alert.level if low_alert else None
    )
