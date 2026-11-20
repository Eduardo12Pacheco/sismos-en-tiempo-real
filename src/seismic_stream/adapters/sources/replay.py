"""Replay event source backed by a recorded USGS GeoJSON sample.

The source accepts a FeatureCollection or newline-delimited Features, keeps
every raw feature intact for the anti-corruption layer, and sorts records by
``(properties.time, id)`` so an unpaced replay is deterministic.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

from seismic_stream.application.ports import SourceError, SourceRecord


def event_time_ms_from_feature(feature: Mapping[str, Any]) -> int | None:
    """Return ``properties.time`` when it is a positive integer, else ``None``."""
    properties = feature.get("properties")
    if not isinstance(properties, Mapping):
        return None
    raw = properties.get("time")
    if isinstance(raw, int) and not isinstance(raw, bool) and raw > 0:
        return raw
    return None


class ReplaySource:
    """Load a recorded sample from disk and yield it in event-time order."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._features = _load_features(self._path)

    def __len__(self) -> int:
        """Number of features in the recorded file."""
        return len(self._features)

    def records(self) -> Iterator[SourceRecord]:
        """Yield one record per feature, lazily, sorted by event time then id."""
        for feature in self._features:
            yield SourceRecord(
                feature=feature,
                event_time_ms=event_time_ms_from_feature(feature),
            )


def _load_features(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise SourceError(f"replay source file {path} does not exist")
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:  # pragma: no cover - defensive: permissions/IO failures
        raise SourceError(f"cannot read replay source file {path}: {exc}") from exc

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        features = _parse_ndjson(path, text)
    else:
        features = _features_from_collection(path, payload)

    validated = _require_feature_objects(path, features)
    return _sorted_features(validated)


def _features_from_collection(path: Path, payload: Any) -> list[Any]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("features"), list):
        raise SourceError(
            f"replay source file {path} is not a GeoJSON FeatureCollection "
            "(expected an object with a 'features' array)"
        )
    return list(payload["features"])


def _parse_ndjson(path: Path, text: str) -> list[Any]:
    features: list[Any] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise SourceError(
                f"replay source file {path} is neither valid JSON nor NDJSON "
                f"(line {line_number}: {exc.msg})"
            ) from exc
        features.append(payload)
    if not features:
        raise SourceError(f"replay source file {path} contains no features")
    return features


def _require_feature_objects(path: Path, features: list[Any]) -> list[dict[str, Any]]:
    validated: list[dict[str, Any]] = []
    for index, feature in enumerate(features):
        if not isinstance(feature, Mapping):
            raise SourceError(f"replay source file {path} feature {index} is not a JSON object")
        validated.append(dict(feature))
    return validated


def _sorted_features(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    timed: list[tuple[int, str, dict[str, Any]]] = []
    untimed: list[dict[str, Any]] = []
    for feature in features:
        event_time_ms = event_time_ms_from_feature(feature)
        if event_time_ms is None:
            untimed.append(feature)
            continue
        feature_id = feature.get("id")
        key = feature_id if isinstance(feature_id, str) else ""
        timed.append((event_time_ms, key, feature))

    timed.sort(key=lambda item: (item[0], item[1]))
    return [feature for _, _, feature in timed] + untimed
