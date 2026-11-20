"""Property-based invariants for the domain layer."""

from __future__ import annotations

import contextlib

from hypothesis import given
from hypothesis import strategies as st

from seismic_stream.domain.alerts import AlertPolicy
from seismic_stream.domain.energy import radiated_energy_joules
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.geography import in_ecuador_region, region_from_place
from seismic_stream.domain.models import AlertLevel, Event
from seismic_stream.domain.normalize import feature_to_event
from seismic_stream.domain.versioning import supersedes
from tests.support.factories import make_event, make_feature

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


JSON_SCALARS = (
    st.none()
    | st.booleans()
    | st.integers()
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(max_size=40)
)
JSON_VALUES = st.recursive(
    JSON_SCALARS,
    lambda children: (
        st.lists(children, max_size=4) | st.dictionaries(st.text(max_size=10), children, max_size=4)
    ),
    max_leaves=20,
)


@given(JSON_VALUES)
def test_acl_never_leaks_a_non_domain_error(value: object) -> None:
    with contextlib.suppress(EventValidationError):
        feature_to_event(value)  # type: ignore[arg-type]


VALID_IDS = st.from_regex(r"[A-Za-z0-9_.-]{1,64}", fullmatch=True)
LONGITUDES = st.floats(min_value=-180.0, max_value=180.0, allow_nan=False, allow_infinity=False)
LATITUDES = st.floats(min_value=-90.0, max_value=90.0, allow_nan=False, allow_infinity=False)
DEPTHS = st.floats(min_value=-10.0, max_value=1000.0, allow_nan=False, allow_infinity=False)
OPTIONAL_TEXT = st.none() | st.text(max_size=20)
EVENT_TYPES = st.text(max_size=20)
PLACES = st.text(max_size=40)


@st.composite
def valid_feature_cases(draw: st.DrawFn) -> tuple[dict[str, object], dict[str, object]]:
    feature_id = draw(VALID_IDS)
    magnitude = draw(MAGNITUDES)
    event_time_ms = draw(st.integers(min_value=1, max_value=2**52))
    updated_at_ms = draw(st.none() | st.integers(min_value=0, max_value=2**53))
    longitude = draw(LONGITUDES)
    latitude = draw(LATITUDES)
    depth_km = draw(DEPTHS)
    properties = {
        "mag": magnitude,
        "time": event_time_ms,
        "updated": updated_at_ms,
        "type": draw(EVENT_TYPES),
        "place": draw(PLACES),
        "status": draw(st.text(max_size=20)),
        "tsunami": draw(st.integers(min_value=0, max_value=1)),
        "sig": draw(st.integers(min_value=-(2**31), max_value=2**31 - 1)),
        "net": draw(OPTIONAL_TEXT),
        "detail": draw(OPTIONAL_TEXT),
        "magType": draw(OPTIONAL_TEXT),
    }
    feature = make_feature(
        id=feature_id,
        properties=properties,
        geometry={"coordinates": [longitude, latitude, depth_km]},
    )
    expected_updated = (
        event_time_ms if updated_at_ms is None or updated_at_ms < event_time_ms else updated_at_ms
    )
    return feature, {
        "event_id": feature_id,
        "event_type": properties["type"],
        "event_time_ms": event_time_ms,
        "updated_at_ms": expected_updated,
        "magnitude": float(magnitude),
        "magnitude_type": properties["magType"],
        "place": properties["place"],
        "latitude": latitude,
        "longitude": longitude,
        "depth_km": depth_km,
        "status": properties["status"],
        "tsunami": properties["tsunami"] == 1,
        "significance": properties["sig"],
        "network": properties["net"],
        "detail_url": properties["detail"],
    }


@given(valid_feature_cases())
def test_valid_feature_round_trips_every_field(
    case: tuple[dict[str, object], dict[str, object]],
) -> None:
    feature, expected = case

    event = feature_to_event(feature)  # type: ignore[arg-type]

    for field_name, expected_value in expected.items():
        assert getattr(event, field_name) == expected_value
    assert event.region == region_from_place(expected["place"])  # type: ignore[arg-type]
    assert event.in_ecuador_region is in_ecuador_region(
        expected["latitude"],
        expected["longitude"],  # type: ignore[arg-type]
    )
