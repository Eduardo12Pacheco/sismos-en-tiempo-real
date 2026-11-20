"""Replay pacer contract: anchors, compression and schedule lag."""

from __future__ import annotations

import math

import pytest

from seismic_stream.application.pacing import ReplayPacer
from tests.support.fakes import FakeClock


def test_first_record_anchors_without_sleeping() -> None:
    clock = FakeClock(now=100.0)
    pacer = ReplayPacer(clock, speed=10.0)
    assert pacer.speed == 10.0

    pacer.wait(1_000)

    assert clock.sleeps == []
    assert pacer.schedule_lag_seconds == 0.0


def test_sleeps_compress_event_time_by_speed() -> None:
    clock = FakeClock()
    pacer = ReplayPacer(clock, speed=2.0)

    for event_time_ms in (1_000, 1_000, 3_000, 7_000):
        pacer.wait(event_time_ms)

    assert clock.sleeps == pytest.approx([1.0, 2.0])
    assert clock.total_slept == pytest.approx(3.0)


@pytest.mark.parametrize("speed", [0.0, -1.0, -0.001, math.nan, math.inf])
def test_invalid_speed_is_rejected(speed: float) -> None:
    with pytest.raises(ValueError, match="speed"):
        ReplayPacer(FakeClock(), speed=speed)


def test_speed_none_never_sleeps() -> None:
    clock = FakeClock()
    pacer = ReplayPacer(clock, speed=None)
    assert pacer.speed is None

    for event_time_ms in (1_000, 10_000, 1_000_000):
        pacer.wait(event_time_ms)

    assert clock.sleeps == []
    assert clock.total_slept == 0.0
    assert pacer.schedule_lag_seconds == 0.0


def test_records_without_event_time_are_not_paced() -> None:
    clock = FakeClock()
    pacer = ReplayPacer(clock, speed=1.0)

    pacer.wait(None)
    pacer.wait(5_000)
    pacer.wait(None)
    pacer.wait(6_000)

    assert clock.total_slept == pytest.approx(1.0)


def test_non_increasing_event_time_never_sleeps_negative() -> None:
    clock = FakeClock()
    pacer = ReplayPacer(clock, speed=1.0)

    pacer.wait(5_000)
    pacer.wait(1_000)

    assert clock.sleeps == []
    assert pacer.schedule_lag_seconds == pytest.approx(4.0)


def test_schedule_lag_reports_how_far_behind_the_pacer_is() -> None:
    clock = FakeClock()
    pacer = ReplayPacer(clock, speed=1.0)

    pacer.wait(0)
    clock.advance(5.0)
    pacer.wait(2_000)

    assert clock.sleeps == []
    assert pacer.schedule_lag_seconds == pytest.approx(3.0)


def test_on_time_replay_has_no_schedule_lag() -> None:
    clock = FakeClock()
    pacer = ReplayPacer(clock, speed=1_440.0)

    for event_time_ms in range(0, 86_400_001, 3_600_000):
        pacer.wait(event_time_ms)

    assert pacer.schedule_lag_seconds == pytest.approx(0.0)
    assert clock.total_slept == pytest.approx(86_400_000 / 1000 / 1_440)
