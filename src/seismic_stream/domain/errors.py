"""Domain-level errors and their validation taxonomy."""

from __future__ import annotations

from typing import Literal

ValidationReason = Literal["missing", "wrong_type", "out_of_range", "malformed"]


class DomainError(Exception):
    """Base class for every error raised by the pure domain layer."""


class EventValidationError(DomainError):
    """Raised when a raw source feature cannot become a domain event.

    ``field`` names the offending input (``properties.mag``, ``geometry``...),
    ``reason`` is a stable machine-readable category and ``feature_id`` is the
    best-effort source identifier so operators can find the DLQ entry.
    """

    def __init__(
        self,
        field: str,
        reason: ValidationReason,
        feature_id: str | None = None,
    ) -> None:
        self.field = field
        self.reason = reason
        self.feature_id = feature_id
        super().__init__(self._render())

    def _render(self) -> str:
        identifier = self.feature_id if self.feature_id is not None else "<unknown>"
        return f"invalid event feature {identifier!r}: field {self.field!r} ({self.reason})"
