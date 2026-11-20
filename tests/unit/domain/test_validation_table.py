"""Table-driven coverage of every validation rule in the ACL."""

from __future__ import annotations

from typing import Any

import pytest

from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.normalize import feature_to_event
from tests.support.factories import REMOVE, make_feature

INVALID_CASES: list[tuple[str, Any, str, str]] = [
    ("feature_not_a_mapping", [], "feature", "wrong_type"),
    ("id_missing", make_feature(id=REMOVE), "id", "missing"),
    ("id_wrong_type", make_feature(id=123), "id", "wrong_type"),
    ("id_malformed", make_feature(id="not a valid id"), "id", "malformed"),
    ("properties_missing", make_feature(properties=REMOVE), "properties", "missing"),
    ("properties_wrong_type", make_feature(properties=[]), "properties", "wrong_type"),
    (
        "magnitude_missing",
        make_feature(properties={"mag": REMOVE}),
        "properties.mag",
        "missing",
    ),
    (
        "magnitude_wrong_type",
        make_feature(properties={"mag": "5.0"}),
        "properties.mag",
        "wrong_type",
    ),
    (
        "magnitude_is_bool",
        make_feature(properties={"mag": True}),
        "properties.mag",
        "wrong_type",
    ),
    (
        "magnitude_not_finite",
        make_feature(properties={"mag": float("nan")}),
        "properties.mag",
        "out_of_range",
    ),
    (
        "magnitude_out_of_range",
        make_feature(properties={"mag": 10.1}),
        "properties.mag",
        "out_of_range",
    ),
    (
        "time_missing",
        make_feature(properties={"time": REMOVE}),
        "properties.time",
        "missing",
    ),
    (
        "time_wrong_type",
        make_feature(properties={"time": "yesterday"}),
        "properties.time",
        "wrong_type",
    ),
    (
        "time_is_bool",
        make_feature(properties={"time": True}),
        "properties.time",
        "wrong_type",
    ),
    (
        "time_out_of_range",
        make_feature(properties={"time": 0}),
        "properties.time",
        "out_of_range",
    ),
    (
        "updated_wrong_type",
        make_feature(properties={"updated": "later"}),
        "properties.updated",
        "wrong_type",
    ),
    ("geometry_missing", make_feature(geometry=REMOVE), "geometry", "missing"),
    ("geometry_wrong_type", make_feature(geometry=[]), "geometry", "wrong_type"),
    (
        "geometry_type_is_not_point",
        make_feature(geometry={"type": "LineString"}),
        "geometry.type",
        "malformed",
    ),
    (
        "coordinates_missing",
        make_feature(geometry={"coordinates": REMOVE}),
        "geometry.coordinates",
        "missing",
    ),
    (
        "coordinates_wrong_type",
        make_feature(geometry={"coordinates": "-122,38"}),
        "geometry.coordinates",
        "wrong_type",
    ),
    (
        "coordinates_too_short",
        make_feature(geometry={"coordinates": [-122.0, 38.8]}),
        "geometry.coordinates",
        "malformed",
    ),
    (
        "longitude_wrong_type",
        make_feature(geometry={"coordinates": ["-122", 38.8, 8.0]}),
        "longitude",
        "wrong_type",
    ),
    (
        "longitude_not_finite",
        make_feature(geometry={"coordinates": [float("inf"), 38.8, 8.0]}),
        "longitude",
        "out_of_range",
    ),
    (
        "longitude_out_of_range",
        make_feature(geometry={"coordinates": [181.0, 38.8, 8.0]}),
        "longitude",
        "out_of_range",
    ),
    (
        "latitude_out_of_range",
        make_feature(geometry={"coordinates": [-122.7, 91.0, 8.0]}),
        "latitude",
        "out_of_range",
    ),
    (
        "depth_out_of_range",
        make_feature(geometry={"coordinates": [-122.7, 38.8, -10.1]}),
        "depth_km",
        "out_of_range",
    ),
    (
        "type_wrong_type",
        make_feature(properties={"type": 5}),
        "properties.type",
        "wrong_type",
    ),
    (
        "place_wrong_type",
        make_feature(properties={"place": 5}),
        "properties.place",
        "wrong_type",
    ),
    (
        "status_wrong_type",
        make_feature(properties={"status": 5}),
        "properties.status",
        "wrong_type",
    ),
    (
        "tsunami_wrong_type",
        make_feature(properties={"tsunami": 0.0}),
        "properties.tsunami",
        "wrong_type",
    ),
    (
        "tsunami_out_of_range",
        make_feature(properties={"tsunami": 2}),
        "properties.tsunami",
        "out_of_range",
    ),
    (
        "significance_wrong_type",
        make_feature(properties={"sig": "42"}),
        "properties.sig",
        "wrong_type",
    ),
    (
        "network_wrong_type",
        make_feature(properties={"net": 5}),
        "properties.net",
        "wrong_type",
    ),
    (
        "detail_wrong_type",
        make_feature(properties={"detail": 5}),
        "properties.detail",
        "wrong_type",
    ),
    (
        "magnitude_type_wrong_type",
        make_feature(properties={"magType": 5}),
        "properties.magType",
        "wrong_type",
    ),
]


@pytest.mark.parametrize(
    ("feature", "field", "reason"),
    [case[1:] for case in INVALID_CASES],
    ids=[case[0] for case in INVALID_CASES],
)
def test_invalid_feature_reports_the_documented_field_and_reason(
    feature: Any, field: str, reason: str
) -> None:
    with pytest.raises(EventValidationError) as excinfo:
        feature_to_event(feature)

    assert excinfo.value.field == field
    assert excinfo.value.reason == reason
