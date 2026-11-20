"""Domain alert policy (thresholds are configurable by the composition root)."""

from __future__ import annotations

from dataclasses import dataclass

from seismic_stream.domain.models import Alert, AlertLevel, Event


@dataclass(frozen=True, slots=True)
class AlertPolicy:
    """Map an event to a WATCH/WARNING alert, or to no alert at all."""

    warning_mag: float = 6.0
    watch_mag: float = 5.0
    ecuador_mag: float = 4.0

    def evaluate(self, event: Event) -> Alert | None:
        """Return the highest-severity alert the event qualifies for."""
        if event.magnitude >= self.warning_mag:
            return self._alert(event, AlertLevel.WARNING, f"magnitude>={self.warning_mag:.1f}")
        if event.tsunami:
            return self._alert(event, AlertLevel.WARNING, "tsunami flag set")
        if event.magnitude >= self.watch_mag:
            return self._alert(event, AlertLevel.WATCH, f"magnitude>={self.watch_mag:.1f}")
        if event.in_ecuador_region and event.magnitude >= self.ecuador_mag:
            return self._alert(
                event,
                AlertLevel.WATCH,
                f"ecuador region magnitude>={self.ecuador_mag:.1f}",
            )
        return None

    def _alert(self, event: Event, level: AlertLevel, reason: str) -> Alert:
        return Alert(
            event_id=event.event_id,
            level=level,
            reason=reason,
            magnitude=event.magnitude,
            place=event.place,
            event_time_ms=event.event_time_ms,
            updated_at_ms=event.updated_at_ms,
            tsunami=event.tsunami,
            in_ecuador_region=event.in_ecuador_region,
        )
