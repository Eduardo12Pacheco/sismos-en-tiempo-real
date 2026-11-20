"""Tests for the alert policy."""

from __future__ import annotations

from seismic_stream.domain.alerts import AlertPolicy
from seismic_stream.domain.models import AlertLevel
from tests.support.factories import make_event


def test_warning_by_magnitude() -> None:
    alert = AlertPolicy().evaluate(make_event(magnitude=6.0, tsunami=False))

    assert alert is not None
    assert alert.level is AlertLevel.WARNING
    assert alert.reason == "magnitude>=6.0"


def test_warning_by_tsunami_flag() -> None:
    alert = AlertPolicy().evaluate(make_event(magnitude=2.0, tsunami=True))

    assert alert is not None
    assert alert.level is AlertLevel.WARNING
    assert alert.reason == "tsunami flag set"


def test_watch_by_magnitude() -> None:
    alert = AlertPolicy().evaluate(make_event(magnitude=5.0, tsunami=False))

    assert alert is not None
    assert alert.level is AlertLevel.WATCH
    assert alert.reason == "magnitude>=5.0"


def test_watch_by_ecuador_region() -> None:
    alert = AlertPolicy().evaluate(make_event(magnitude=4.0, tsunami=False, in_ecuador_region=True))

    assert alert is not None
    assert alert.level is AlertLevel.WATCH
    assert alert.reason == "ecuador region magnitude>=4.0"


def test_ecuador_below_threshold_is_ignored() -> None:
    assert (
        AlertPolicy().evaluate(make_event(magnitude=3.9, tsunami=False, in_ecuador_region=True))
        is None
    )


def test_small_non_ecuador_event_is_ignored() -> None:
    assert AlertPolicy().evaluate(make_event(magnitude=4.9, tsunami=False)) is None


def test_alert_copies_the_event_context() -> None:
    event = make_event(
        event_id="us7000abcd",
        magnitude=6.4,
        place="100 km SSE of Perryville, Alaska",
        event_time_ms=1_730_000_111_000,
        updated_at_ms=1_730_000_222_000,
        tsunami=True,
        in_ecuador_region=False,
    )

    alert = AlertPolicy().evaluate(event)

    assert alert is not None
    assert alert.event_id == event.event_id
    assert alert.magnitude == event.magnitude
    assert alert.place == event.place
    assert alert.event_time_ms == event.event_time_ms
    assert alert.updated_at_ms == event.updated_at_ms
    assert alert.tsunami is True
    assert alert.in_ecuador_region is False


def test_custom_thresholds_are_reflected_in_reasons() -> None:
    policy = AlertPolicy(warning_mag=7.0, watch_mag=6.0, ecuador_mag=3.5)

    warning = policy.evaluate(make_event(magnitude=7.0))
    watch = policy.evaluate(make_event(magnitude=6.0))
    ecuador = policy.evaluate(make_event(magnitude=3.5, in_ecuador_region=True))

    assert warning is not None
    assert warning.reason == "magnitude>=7.0"
    assert watch is not None
    assert watch.reason == "magnitude>=6.0"
    assert ecuador is not None
    assert ecuador.reason == "ecuador region magnitude>=3.5"
