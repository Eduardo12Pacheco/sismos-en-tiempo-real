"""Anti-corruption layer translating USGS GeoJSON features into domain events.

The public entry point never leaks implementation exceptions: every malformed
or unexpected input is reported as an :class:`EventValidationError` so callers
can route the raw payload to the dead-letter channel.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from typing import Any, TypeGuard

from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.geography import in_ecuador_region, region_from_place
from seismic_stream.domain.models import Event

_FEATURE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

_MIN_MAGNITUDE = -3.0
_MAX_MAGNITUDE = 10.0

_MIN_LONGITUDE = -180.0
_MAX_LONGITUDE = 180.0
_MIN_LATITUDE = -90.0
_MAX_LATITUDE = 90.0
_MIN_DEPTH_KM = -10.0
_MAX_DEPTH_KM = 1000.0

_DEFAULT_EVENT_TYPE = "earthquake"
_DEFAULT_PLACE = "Unknown"
_DEFAULT_STATUS = "automatic"


def feature_to_event(feature: Mapping[str, Any], source: str = "usgs") -> Event:
    """Validate a raw USGS Feature and convert it into a domain :class:`Event`."""
    try:
        return _build_event(feature, source)
    except EventValidationError:
        raise
    except Exception as exc:
        raise EventValidationError("feature", "malformed") from exc


def _build_event(feature: Mapping[str, Any], source: str) -> Event:
    if not isinstance(feature, Mapping):
        raise EventValidationError("feature", "wrong_type")

    event_id = _required_id(feature.get("id"))

    properties = feature.get("properties")
    if properties is None:
        raise EventValidationError("properties", "missing", event_id)
    if not isinstance(properties, Mapping):
        raise EventValidationError("properties", "wrong_type", event_id)

    magnitude = _required_magnitude(properties.get("mag"), event_id)
    event_time_ms = _required_time(properties.get("time"), event_id)
    updated_at_ms = _updated_at(properties.get("updated"), event_time_ms, event_id)
    longitude, latitude, depth_km = _coordinates(feature.get("geometry"), event_id)

    event_type = _text(properties, "type", _DEFAULT_EVENT_TYPE, event_id)
    place = _text(properties, "place", _DEFAULT_PLACE, event_id)
    status = _text(properties, "status", _DEFAULT_STATUS, event_id)
    tsunami = _tsunami(properties.get("tsunami"), event_id)
    significance = _significance(properties.get("sig"), event_id)
    magnitude_type = _optional_text(properties, "magType", event_id)
    network = _optional_text(properties, "net", event_id)
    detail_url = _optional_text(properties, "detail", event_id)

    return Event(
        event_id=event_id,
        source=source,
        event_type=event_type,
        event_time_ms=event_time_ms,
        updated_at_ms=updated_at_ms,
        magnitude=magnitude,
        magnitude_type=magnitude_type,
        place=place,
        region=region_from_place(place),
        latitude=latitude,
        longitude=longitude,
        depth_km=depth_km,
        status=status,
        tsunami=tsunami,
        significance=significance,
        network=network,
        detail_url=detail_url,
        in_ecuador_region=in_ecuador_region(latitude, longitude),
    )


def _required_id(raw: Any) -> str:
    if raw is None:
        raise EventValidationError("id", "missing")
    if not isinstance(raw, str):
        raise EventValidationError("id", "wrong_type")
    if _FEATURE_ID_PATTERN.fullmatch(raw) is None:
        raise EventValidationError("id", "malformed", raw)
    return raw


def _required_magnitude(raw: Any, event_id: str) -> float:
    if raw is None:
        raise EventValidationError("properties.mag", "missing", event_id)
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise EventValidationError("properties.mag", "wrong_type", event_id)
    magnitude = float(raw)
    if not math.isfinite(magnitude):
        raise EventValidationError("properties.mag", "out_of_range", event_id)
    if not _MIN_MAGNITUDE <= magnitude <= _MAX_MAGNITUDE:
        raise EventValidationError("properties.mag", "out_of_range", event_id)
    return magnitude


def _required_time(raw: Any, event_id: str) -> int:
    if raw is None:
        raise EventValidationError("properties.time", "missing", event_id)
    if not _is_int(raw):
        raise EventValidationError("properties.time", "wrong_type", event_id)
    if raw <= 0:
        raise EventValidationError("properties.time", "out_of_range", event_id)
    return raw


def _updated_at(raw: Any, event_time_ms: int, event_id: str) -> int:
    if raw is None:
        return event_time_ms
    if not _is_int(raw):
        raise EventValidationError("properties.updated", "wrong_type", event_id)
    if raw < event_time_ms:
        return event_time_ms
    return raw


def _coordinates(raw: Any, event_id: str) -> tuple[float, float, float]:
    if raw is None:
        raise EventValidationError("geometry", "missing", event_id)
    if not isinstance(raw, Mapping):
        raise EventValidationError("geometry", "wrong_type", event_id)
    if raw.get("type") != "Point":
        raise EventValidationError("geometry.type", "malformed", event_id)

    coordinates = raw.get("coordinates")
    if coordinates is None:
        raise EventValidationError("geometry.coordinates", "missing", event_id)
    if not isinstance(coordinates, list):
        raise EventValidationError("geometry.coordinates", "wrong_type", event_id)
    if len(coordinates) < 3:
        raise EventValidationError("geometry.coordinates", "malformed", event_id)

    longitude = _coordinate(coordinates[0], "longitude", _MIN_LONGITUDE, _MAX_LONGITUDE, event_id)
    latitude = _coordinate(coordinates[1], "latitude", _MIN_LATITUDE, _MAX_LATITUDE, event_id)
    depth_km = _coordinate(coordinates[2], "depth_km", _MIN_DEPTH_KM, _MAX_DEPTH_KM, event_id)
    return longitude, latitude, depth_km


def _coordinate(raw: Any, field: str, minimum: float, maximum: float, event_id: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise EventValidationError(field, "wrong_type", event_id)
    value = float(raw)
    if not math.isfinite(value):
        raise EventValidationError(field, "out_of_range", event_id)
    if not minimum <= value <= maximum:
        raise EventValidationError(field, "out_of_range", event_id)
    return value


def _text(properties: Mapping[str, Any], key: str, default: str, event_id: str) -> str:
    raw = properties.get(key)
    if raw is None:
        return default
    if not isinstance(raw, str):
        raise EventValidationError(f"properties.{key}", "wrong_type", event_id)
    return raw


def _optional_text(properties: Mapping[str, Any], key: str, event_id: str) -> str | None:
    raw = properties.get(key)
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise EventValidationError(f"properties.{key}", "wrong_type", event_id)
    return raw


def _tsunami(raw: Any, event_id: str) -> bool:
    if raw is None:
        return False
    if not _is_int(raw):
        raise EventValidationError("properties.tsunami", "wrong_type", event_id)
    if raw not in (0, 1):
        raise EventValidationError("properties.tsunami", "out_of_range", event_id)
    return raw == 1


def _significance(raw: Any, event_id: str) -> int:
    if raw is None:
        return 0
    if not _is_int(raw):
        raise EventValidationError("properties.sig", "wrong_type", event_id)
    return raw


def _is_int(value: object) -> TypeGuard[int]:
    return isinstance(value, int) and not isinstance(value, bool)
