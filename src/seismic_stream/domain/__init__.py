"""Pure domain layer: models, validation and derivations.

Nothing in this package performs I/O or imports infrastructure libraries; the
import-linter contract in ``pyproject.toml`` enforces that rule.
"""

from __future__ import annotations

from seismic_stream.domain.alerts import AlertPolicy
from seismic_stream.domain.energy import radiated_energy_joules
from seismic_stream.domain.errors import DomainError, EventValidationError, ValidationReason
from seismic_stream.domain.geography import in_ecuador_region, region_from_place
from seismic_stream.domain.models import (
    Alert,
    AlertLevel,
    DeadLetter,
    DlqStage,
    Event,
    WindowStats,
    dlq_id,
)
from seismic_stream.domain.normalize import feature_to_event
from seismic_stream.domain.versioning import supersedes

__all__ = [
    "Alert",
    "AlertLevel",
    "AlertPolicy",
    "DeadLetter",
    "DlqStage",
    "DomainError",
    "Event",
    "EventValidationError",
    "ValidationReason",
    "WindowStats",
    "dlq_id",
    "feature_to_event",
    "in_ecuador_region",
    "radiated_energy_joules",
    "region_from_place",
    "supersedes",
]
