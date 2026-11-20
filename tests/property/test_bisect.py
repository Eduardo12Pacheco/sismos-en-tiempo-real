"""Hypothesis properties for the poison-row bisection algorithm."""

from __future__ import annotations

from collections.abc import Sequence

from hypothesis import given
from hypothesis import strategies as st

from seismic_stream.application.retry import PermanentStoreError
from seismic_stream.application.sinking import bisect_write


@given(
    rows=st.lists(st.integers(min_value=0, max_value=10_000), max_size=64),
    poison=st.sets(st.integers(min_value=0, max_value=10_000), max_size=4),
)
def test_bisection_stores_exactly_the_non_poison_rows(rows: list[int], poison: set[int]) -> None:
    successful_writes: list[int] = []

    def write(chunk: Sequence[int], suffix: str) -> None:
        if any(row in poison for row in chunk):
            raise PermanentStoreError("rejected")
        successful_writes.extend(chunk)

    result = bisect_write(rows, write)

    expected_rejected = [row for row in rows if row in poison]
    expected_stored = [row for row in rows if row not in poison]
    assert result.rejected == tuple(expected_rejected)
    assert result.stored == tuple(expected_stored)
    assert successful_writes == expected_stored


def test_empty_batch_is_a_noop() -> None:
    calls: list[Sequence[int]] = []

    result = bisect_write([], lambda chunk, suffix: calls.append(chunk))

    assert result.stored == ()
    assert result.rejected == ()
    assert calls == []
