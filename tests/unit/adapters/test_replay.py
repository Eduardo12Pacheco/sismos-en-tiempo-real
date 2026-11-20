"""Replay source contract: loading, ordering, laziness and typed errors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from seismic_stream.adapters.sources.replay import ReplaySource
from seismic_stream.application.ports import SourceError
from tests.support.factories import make_feature

REPO_ROOT = Path(__file__).resolve().parents[3]
SAMPLE = REPO_ROOT / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"


def _write_feature_collection(path: Path, features: list[dict[str, Any]]) -> Path:
    path.write_text(
        json.dumps({"type": "FeatureCollection", "features": features}),
        encoding="utf-8",
    )
    return path


def _write_ndjson(path: Path, features: list[dict[str, Any]]) -> Path:
    path.write_text(
        "\n".join(json.dumps(feature) for feature in features) + "\n",
        encoding="utf-8",
    )
    return path


def _ids(source: ReplaySource) -> list[str]:
    return [str(record.feature["id"]) for record in source.records()]


def test_feature_collection_is_sorted_by_time_then_id(tmp_path: Path) -> None:
    features = [
        make_feature(id="late", properties={"time": 300}),
        make_feature(id="b", properties={"time": 100}),
        make_feature(id="a", properties={"time": 100}),
        make_feature(id="middle", properties={"time": 200}),
    ]
    path = _write_feature_collection(tmp_path / "sample.geojson", features)

    source = ReplaySource(path)

    assert len(source) == 4
    assert _ids(source) == ["a", "b", "middle", "late"]


def test_features_without_time_keep_file_order_at_the_end(tmp_path: Path) -> None:
    features = [
        make_feature(id="no-time-1", properties={"time": "not-a-time"}),
        make_feature(id="timed", properties={"time": 5}),
        make_feature(id="missing-time", properties={"time": None}),
    ]
    path = _write_feature_collection(tmp_path / "sample.geojson", features)

    source = ReplaySource(path)

    assert _ids(source) == ["timed", "no-time-1", "missing-time"]
    assert [record.event_time_ms for record in source.records()] == [5, None, None]


def test_ndjson_features_are_supported(tmp_path: Path) -> None:
    features = [
        make_feature(id="second", properties={"time": 2}),
        make_feature(id="first", properties={"time": 1}),
    ]
    path = _write_ndjson(tmp_path / "sample.ndjson", features)

    source = ReplaySource(path)

    assert _ids(source) == ["first", "second"]


def test_records_are_lazy_and_reiterable(tmp_path: Path) -> None:
    path = _write_feature_collection(
        tmp_path / "sample.geojson",
        [make_feature(id="one", properties={"time": 1})],
    )

    source = ReplaySource(path)
    records = source.records()

    assert iter(records) is records
    assert next(records).feature["id"] == "one"
    # A second call starts a fresh iteration, so consumers can replay the file.
    assert [record.feature["id"] for record in source.records()] == ["one"]


def test_missing_file_raises_source_error(tmp_path: Path) -> None:
    with pytest.raises(SourceError, match="does not exist"):
        ReplaySource(tmp_path / "absent.geojson")


def test_invalid_json_raises_source_error(tmp_path: Path) -> None:
    path = tmp_path / "broken.geojson"
    path.write_text("{not json", encoding="utf-8")

    with pytest.raises(SourceError, match="JSON"):
        ReplaySource(path)


def test_json_without_features_raises_source_error(tmp_path: Path) -> None:
    path = tmp_path / "other.geojson"
    path.write_text(json.dumps({"type": "Feature", "id": "x"}), encoding="utf-8")

    with pytest.raises(SourceError, match="FeatureCollection"):
        ReplaySource(path)


def test_ndjson_line_that_is_not_an_object_raises_source_error(tmp_path: Path) -> None:
    path = tmp_path / "broken.ndjson"
    path.write_text("[1, 2, 3]\n", encoding="utf-8")

    with pytest.raises(SourceError, match="object"):
        ReplaySource(path)


def test_bundled_sample_loads_and_is_time_ordered() -> None:
    source = ReplaySource(SAMPLE)

    records = list(source.records())

    assert len(source) == 1_817
    timestamps = [record.event_time_ms for record in records]
    assert all(timestamp is not None for timestamp in timestamps)
    timed = [timestamp for timestamp in timestamps if timestamp is not None]
    assert timed == sorted(timed)
