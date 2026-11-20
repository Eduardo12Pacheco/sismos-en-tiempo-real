"""Property tests for the exponential backoff used by the live source."""

from __future__ import annotations

import random

import pytest
from hypothesis import given
from hypothesis import strategies as st

from seismic_stream.adapters.sources.usgs_live import (
    BASE_BACKOFF_SECONDS,
    MAX_BACKOFF_SECONDS,
    backoff_cap,
    jittered_backoff,
)


@given(st.integers(min_value=0, max_value=64))
def test_backoff_cap_is_bounded_and_non_decreasing(attempt: int) -> None:
    cap = backoff_cap(attempt)
    next_cap = backoff_cap(attempt + 1)

    assert 0 <= cap <= MAX_BACKOFF_SECONDS
    assert 0 <= next_cap <= MAX_BACKOFF_SECONDS
    assert next_cap >= cap


@given(st.integers(min_value=0, max_value=64))
def test_first_attempt_starts_at_the_base_delay(attempt: int) -> None:
    assert backoff_cap(0) == BASE_BACKOFF_SECONDS


@given(
    st.integers(min_value=0, max_value=64),
    st.integers(min_value=0, max_value=2**32 - 1),
)
def test_full_jitter_never_exceeds_the_cap(attempt: int, seed: int) -> None:
    delay = jittered_backoff(attempt, random.Random(seed))

    assert 0 <= delay <= backoff_cap(attempt) <= MAX_BACKOFF_SECONDS


@given(
    st.integers(min_value=0, max_value=64),
    st.floats(min_value=0, max_value=899, allow_nan=False),
    st.integers(min_value=0, max_value=2**32 - 1),
)
def test_retry_after_is_a_floor(attempt: int, retry_after: float, seed: int) -> None:
    delay = jittered_backoff(attempt, random.Random(seed), retry_after=retry_after)

    assert delay >= retry_after


def test_negative_attempt_is_rejected() -> None:
    with pytest.raises(ValueError, match="attempt"):
        backoff_cap(-1)
