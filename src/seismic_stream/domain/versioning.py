"""Event-version ordering rules."""

from __future__ import annotations


def supersedes(candidate_updated_ms: int, existing_updated_ms: int) -> bool:
    """Return whether a candidate revision is strictly newer than the existing one."""
    return candidate_updated_ms > existing_updated_ms
