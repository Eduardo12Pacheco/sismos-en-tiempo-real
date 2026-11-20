"""Tests for the USGS feature to Event anti-corruption layer."""

from __future__ import annotations

from typing import Any

import pytest

from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.models import Event
from seismic_stream.domain.normalize import feature_to_event
from tests.support.factories import BASE_TIME_MS, REMOVE, make_event, make_feature


def test_valid_feature_maps_every_field() -> None:
    event = feature_to_event(make_feature())

    assert event == make_event()


def test_place_and_coordinates_drive_region_and_ecuador_flag() -> None:
    feature = make_feature(
        properties={"place": "45 km NNW of Salinas, Ecuador"},
        geometry={"coordinates": [-80.95, -2.2, 30.0]},
    )

    event = feature_to_event(feature)

    assert event.region == "Ecuador"
    assert event.in_ecuador_region is True
    assert event.latitude == pytest.approx(-2.2)
    assert event.longitude == pytest.approx(-80.95)
    assert event.depth_km == pytest.approx(30.0)


def test_missing_optional_properties_fall_back_to_defaults() -> None:
    feature = make_feature(
        properties={
            "updated": REMOVE,
            "type": REMOVE,
            "place": REMOVE,
            "status": REMOVE,
            "tsunami": REMOVE,
            "sig": REMOVE,
            "net": REMOVE,
            "detail": REMOVE,
            "magType": REMOVE,
        }
    )

    event = feature_to_event(feature)

    assert event.event_type == "earthquake"
    assert event.place == "Unknown"
    assert event.region == "Unknown"
    assert event.status == "automatic"
    assert event.tsunami is False
    assert event.significance == 0
    assert event.network is None
    assert event.detail_url is None
    assert event.magnitude_type is None
    assert event.updated_at_ms == event.event_time_ms == BASE_TIME_MS


def test_updated_before_time_is_clamped_to_time() -> None:
    feature = make_feature(properties={"time": BASE_TIME_MS, "updated": BASE_TIME_MS - 1})

    event = feature_to_event(feature)

    assert event.updated_at_ms == BASE_TIME_MS


def test_updated_equal_to_time_is_kept() -> None:
    feature = make_feature(properties={"time": BASE_TIME_MS, "updated": BASE_TIME_MS})

    event = feature_to_event(feature)

    assert event.updated_at_ms == BASE_TIME_MS


def test_tsunami_flag_is_converted_to_bool() -> None:
    assert feature_to_event(make_feature(properties={"tsunami": 1})).tsunami is True
    assert feature_to_event(make_feature(properties={"tsunami": 0})).tsunami is False


def test_source_is_configurable() -> None:
    assert feature_to_event(make_feature(), source="replay").source == "replay"


@pytest.mark.parametrize(
    "feature_id",
    ["a", "nc75468637", "Abc_123.xyz-9", "a" * 64],
)
def test_allowed_feature_ids_are_accepted(feature_id: str) -> None:
    assert feature_to_event(make_feature(id=feature_id)).event_id == feature_id


def test_too_long_feature_id_is_malformed() -> None:
    with pytest.raises(EventValidationError) as excinfo:
        feature_to_event(make_feature(id="a" * 65))

    assert excinfo.value.field == "id"
    assert excinfo.value.reason == "malformed"


@pytest.mark.parametrize("feature", [[], "not-a-feature", 42, None])
def test_non_mapping_feature_is_rejected_with_a_domain_error(feature: Any) -> None:
    with pytest.raises(EventValidationError) as excinfo:
        feature_to_event(feature)

    assert excinfo.value.field == "feature"
    assert excinfo.value.reason == "wrong_type"


def test_unexpected_failure_is_wrapped_in_a_domain_error() -> None:
    class ExplodingMapping(dict[str, Any]):
        def get(self, key: str, default: Any = None) -> Any:
            raise RuntimeError("boom")

    with pytest.raises(EventValidationError) as excinfo:
        feature_to_event(ExplodingMapping())

    assert excinfo.value.field == "feature"
    assert excinfo.value.reason == "malformed"


def test_error_message_is_readable() -> None:
    with pytest.raises(EventValidationError) as excinfo:
        feature_to_event(make_feature(id="evt-1", properties={"mag": 99.0}))

    message = str(excinfo.value)

    assert "evt-1" in message
    assert "properties.mag" in message
    assert "out_of_range" in message


def test_feature_id_is_preserved_in_validation_errors() -> None:
    with pytest.raises(EventValidationError) as excinfo:
        feature_to_event(make_feature(id="evt-42", properties={"mag": REMOVE}))

    assert excinfo.value.feature_id == "evt-42"


def test_event_is_the_public_domain_model() -> None:
    assert isinstance(feature_to_event(make_feature()), Event)
