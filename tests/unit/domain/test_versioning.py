"""Tests for event-version ordering."""

from __future__ import annotations

import pytest

from seismic_stream.domain.versioning import supersedes
from tests.support.factories import make_event


@pytest.mark.parametrize(
    ("candidate", "existing", "expected"),
    [
        (2, 1, True),
        (1, 1, False),
        (0, 1, False),
        (1_000, 999, True),
    ],
)
def test_supersedes_is_strictly_greater_than(candidate: int, existing: int, expected: bool) -> None:
    assert supersedes(candidate, existing) is expected


def test_event_version_exposes_updated_at_ms() -> None:
    event = make_event(updated_at_ms=1_730_000_060_000)
    assert event.version == event.updated_at_ms
