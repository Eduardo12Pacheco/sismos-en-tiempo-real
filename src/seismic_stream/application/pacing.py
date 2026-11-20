"""Event-time replay pacing with configurable time compression.

The pacer anchors on the first record that carries an event timestamp and then
targets ``anchor_wall + (event_ms - anchor_event_ms) / 1000 / speed`` for every
following record. With a truthful clock the scheduled sleeps telescope exactly
to ``(last_event_ms - first_event_ms) / 1000 / speed``, which is the invariant
property tests assert.
"""

from __future__ import annotations

import math

from seismic_stream.application.ports import Clock


class ReplayPacer:
    """Convert event time into wall-clock sleeps at a fixed compression speed.

    ``speed`` is a multiplier over real time (``1440`` replays one day of event
    time per minute); ``None`` means "as fast as possible" and never sleeps.
    """

    def __init__(self, clock: Clock, speed: float | None) -> None:
        if speed is not None and (not math.isfinite(speed) or speed <= 0):
            raise ValueError(f"replay speed must be a finite number > 0, got {speed!r}")
        self._clock = clock
        self._speed = speed
        self._anchor_wall: float | None = None
        self._anchor_event_ms: int | None = None
        self._schedule_lag_seconds = 0.0

    @property
    def speed(self) -> float | None:
        """Configured compression speed, or ``None`` for unpaced replay."""
        return self._speed

    @property
    def schedule_lag_seconds(self) -> float:
        """Seconds by which the last scheduled record was late (never negative)."""
        return self._schedule_lag_seconds

    def wait(self, event_time_ms: int | None) -> None:
        """Sleep until the record at ``event_time_ms`` should be emitted.

        Records without an event timestamp are not paced. The first timed
        record only anchors the schedule; later records sleep for the remaining
        compressed delay, clamping at zero so the pacer never sleeps a negative
        duration.
        """
        if event_time_ms is None:
            return

        if self._anchor_event_ms is None:
            self._anchor_wall = self._clock.now()
            self._anchor_event_ms = event_time_ms
            self._schedule_lag_seconds = 0.0
            return

        if self._speed is None:
            return

        assert self._anchor_wall is not None
        target_wall = (
            self._anchor_wall + (event_time_ms - self._anchor_event_ms) / 1000.0 / self._speed
        )
        now = self._clock.now()
        delay = target_wall - now
        if delay > 0:
            self._clock.sleep(delay)
            now = self._clock.now()
        self._schedule_lag_seconds = max(0.0, now - target_wall)
