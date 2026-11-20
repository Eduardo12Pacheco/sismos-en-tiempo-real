"""Geographic derivations kept free of infrastructure dependencies."""

from __future__ import annotations

UNKNOWN_REGION = "Unknown"

ECUADOR_LATITUDE_MIN = -5.0
ECUADOR_LATITUDE_MAX = 2.0
ECUADOR_LONGITUDE_MIN = -92.0
ECUADOR_LONGITUDE_MAX = -75.0


def region_from_place(place: str | None) -> str:
    """Derive the region as the text after the last ``", "`` in a place string.

    A missing, empty or whitespace-only place maps to ``"Unknown"``. The result
    is always a non-empty, stripped string.
    """
    if place is None:
        return UNKNOWN_REGION

    text = place.strip()
    if not text:
        return UNKNOWN_REGION

    _, separator, suffix = place.rpartition(", ")
    if not separator:
        return text
    candidate = suffix.strip()
    return candidate or UNKNOWN_REGION


def in_ecuador_region(latitude: float, longitude: float) -> bool:
    """Return whether a coordinate falls in the inclusive Ecuador bounding box."""
    return (
        ECUADOR_LATITUDE_MIN <= latitude <= ECUADOR_LATITUDE_MAX
        and ECUADOR_LONGITUDE_MIN <= longitude <= ECUADOR_LONGITUDE_MAX
    )
