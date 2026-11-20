"""Store error taxonomy and the sink's jittered retry policy.

The application layer never imports a database driver: the storage adapter maps
driver exceptions to :class:`TransientStoreError` or :class:`PermanentStoreError`
following the policy in ADR 0006, and the sink owns the retry/bisection policy.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class StoreError(Exception):
    """Base class for errors raised by an event store adapter."""


class TransientStoreError(StoreError):
    """A failure that may succeed after pausing the consumer and retrying."""


class PermanentStoreError(StoreError):
    """A failure caused by the rows themselves (poison rows), not the service."""


class RandomSource(Protocol):
    """Minimal RNG surface used for deterministic full-jitter tests."""

    def uniform(self, low: float, high: float) -> float:
        """Return a number in ``[low, high]``."""
        ...


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Capped exponential backoff with full jitter (D10, S11).

    The nominal delay for attempt ``n`` is ``min(max_seconds, base_seconds * 2**n)``;
    the actual sleep is drawn uniformly from ``[0, nominal]`` so retries from
    many clients do not synchronise.
    """

    base_seconds: float = 0.5
    max_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.base_seconds < 0:
            raise ValueError("base_seconds must be zero or positive")
        if self.max_seconds <= 0:
            raise ValueError("max_seconds must be greater than zero")

    def nominal_delay(self, attempt: int) -> float:
        """Return the un-jittered cap for a zero-based retry attempt."""
        if attempt < 0:
            raise ValueError("attempt must be zero or positive")
        return min(self.max_seconds, self.base_seconds * float(2**attempt))

    def delay(self, attempt: int, rng: RandomSource) -> float:
        """Return a full-jitter delay in ``[0, nominal_delay(attempt)]``."""
        return rng.uniform(0.0, self.nominal_delay(attempt))
