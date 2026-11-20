"""Builders for domain-level test data."""

from __future__ import annotations

from typing import Any, Final

from seismic_stream.domain.models import Event

FEATURE_ID: Final = "nc75468637"
BASE_TIME_MS: Final = 1_730_000_000_000

#: Sentinel used in overrides to remove a key instead of setting it.
REMOVE: Final = object()


def make_feature(**overrides: Any) -> dict[str, Any]:
    """Return a valid USGS-like Feature that can be adjusted per test.

    Top-level keys replace values; ``properties`` and ``geometry`` overrides are
    merged into the nested mapping, and ``REMOVE`` deletes a key at either level.
    """
    feature: dict[str, Any] = {
        "id": FEATURE_ID,
        "properties": {
            "mag": 4.5,
            "time": BASE_TIME_MS,
            "updated": BASE_TIME_MS + 60_000,
            "type": "earthquake",
            "place": "6 km WNW of Cobb, CA",
            "status": "automatic",
            "tsunami": 0,
            "sig": 42,
            "net": "nc",
            "detail": f"https://example.test/detail/{FEATURE_ID}",
            "magType": "ml",
        },
        "geometry": {"type": "Point", "coordinates": [-122.7, 38.8, 8.0]},
    }
    for key, value in overrides.items():
        if value is REMOVE:
            feature.pop(key, None)
        elif key in ("properties", "geometry") and isinstance(value, dict):
            nested = feature[key]
            assert isinstance(nested, dict)
            for nested_key, nested_value in value.items():
                if nested_value is REMOVE:
                    nested.pop(nested_key, None)
                else:
                    nested[nested_key] = nested_value
        else:
            feature[key] = value
    return feature


def make_event(**overrides: Any) -> Event:
    """Return a valid domain Event with sensible defaults."""
    data: dict[str, Any] = {
        "event_id": FEATURE_ID,
        "source": "usgs",
        "event_type": "earthquake",
        "event_time_ms": BASE_TIME_MS,
        "updated_at_ms": BASE_TIME_MS + 60_000,
        "magnitude": 4.5,
        "magnitude_type": "ml",
        "place": "6 km WNW of Cobb, CA",
        "region": "CA",
        "latitude": 38.8,
        "longitude": -122.7,
        "depth_km": 8.0,
        "status": "automatic",
        "tsunami": False,
        "significance": 42,
        "network": "nc",
        "detail_url": f"https://example.test/detail/{FEATURE_ID}",
        "in_ecuador_region": False,
    }
    data.update(overrides)
    return Event(**data)
