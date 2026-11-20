"""Deterministic fakes shared by unit and property tests."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from seismic_stream.application.ports import SourceRecord


class FakeClock:
    """Manual clock: ``sleep`` advances time and records every request.

    A negative sleep raises immediately so tests catch a pacer that would try to
    move backwards instead of clamping at zero.
    """

    def __init__(self, now: float = 0.0) -> None:
        self._now = now
        self.sleeps: list[float] = []

    def now(self) -> float:
        """Return the current fake wall time in seconds."""
        return self._now

    def sleep(self, seconds: float) -> None:
        """Advance the fake clock by ``seconds`` and remember the request."""
        if seconds < 0:
            raise AssertionError(f"negative sleep requested: {seconds}")
        self.sleeps.append(seconds)
        self._now += seconds

    def advance(self, seconds: float) -> None:
        """Move the fake clock forward without recording a sleep."""
        self._now += seconds

    @property
    def total_slept(self) -> float:
        """Total seconds requested through :meth:`sleep`."""
        return sum(self.sleeps)


class FakeSource:
    """In-memory :class:`EventSource` used to test decorators and consumers."""

    def __init__(self, records: Iterable[SourceRecord]) -> None:
        self._records = list(records)

    def __len__(self) -> int:
        """Number of records held in memory."""
        return len(self._records)

    def records(self) -> Iterator[SourceRecord]:
        """Yield the stored records in insertion order."""
        yield from self._records
