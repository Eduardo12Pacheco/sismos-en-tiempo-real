"""Tests for geographic derivations."""

from __future__ import annotations

import pytest

from seismic_stream.domain.geography import in_ecuador_region, region_from_place


@pytest.mark.parametrize(
    ("place", "expected"),
    [
        ("6 km WNW of Cobb, CA", "CA"),
        ("45 km NNW of Salinas, Ecuador", "Ecuador"),
        ("Southern East Pacific Rise", "Southern East Pacific Rise"),
        ("  Anchorage, Alaska  ", "Alaska"),
        ("Last, suffix, Ecuador", "Ecuador"),
        ("Foo, ", "Unknown"),
        ("", "Unknown"),
        ("   ", "Unknown"),
        (None, "Unknown"),
    ],
)
def test_region_from_place(place: str | None, expected: str) -> None:
    assert region_from_place(place) == expected


@pytest.mark.parametrize(
    ("latitude", "longitude", "expected"),
    [
        (-5.0, -92.0, True),  # south-west corner, inclusive
        (2.0, -75.0, True),  # north-east corner, inclusive
        (-2.2, -80.9, True),  # Guayaquil
        (-0.18, -78.47, True),  # Quito
        (-0.9, -89.6, True),  # Galapagos
        (-5.0001, -80.0, False),
        (2.0001, -80.0, False),
        (-2.0, -92.0001, False),
        (-2.0, -74.9999, False),
        (38.8, -122.7, False),
    ],
)
def test_in_ecuador_region(latitude: float, longitude: float, expected: bool) -> None:
    assert in_ecuador_region(latitude, longitude) is expected
