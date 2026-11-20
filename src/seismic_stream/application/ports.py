"""Application ports shared by the ingest and replay use cases.

Every port is a :class:`typing.Protocol` so adapters depend on behaviour instead
of concrete classes, and the pure application layer never imports
infrastructure libraries.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol


class SourceError(Exception):
    """Raised when an event source cannot be read or yields unusable data."""


@dataclass(frozen=True, slots=True)
class SourceRecord:
    """One raw feature read from a source, with its event timestamp when known.

    ``injected`` marks records added by the chaos decorator. It is excluded from
    equality so an exact duplicate still compares equal to its source record.
    """

    feature: Mapping[str, Any]
    event_time_ms: int | None
    injected: bool = field(default=False, compare=False)


class Clock(Protocol):
    """Wall-clock abstraction so replay pacing is deterministic in tests."""

    def now(self) -> float:
        """Return the current wall time in seconds."""
        ...

    def sleep(self, seconds: float) -> None:
        """Block for ``seconds`` (never negative)."""
        ...


class EventSource(Protocol):
    """Stream of raw source records, oldest first."""

    def records(self) -> Iterator[SourceRecord]:
        """Yield records lazily."""
        ...
