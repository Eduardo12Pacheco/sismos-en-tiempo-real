"""Tests for the radiated-energy approximation."""

from __future__ import annotations

import pytest

from seismic_stream.domain.energy import radiated_energy_joules


@pytest.mark.parametrize("magnitude", [-3.0, 0.0, 2.5, 4.5, 6.1, 10.0])
def test_energy_matches_the_gutenberg_richter_formula(magnitude: float) -> None:
    assert radiated_energy_joules(magnitude) == pytest.approx(10 ** (1.5 * magnitude + 4.8))


@pytest.mark.parametrize(("low", "high"), [(-3.0, -2.9), (0.0, 1.0), (4.9, 5.0), (9.0, 10.0)])
def test_energy_is_strictly_increasing(low: float, high: float) -> None:
    assert radiated_energy_joules(low) < radiated_energy_joules(high)


@pytest.mark.parametrize("magnitude", [-3.0, 0.0, 4.5, 9.0])
def test_one_magnitude_unit_multiplies_energy_by_ten_to_the_1_5(
    magnitude: float,
) -> None:
    ratio = radiated_energy_joules(magnitude + 1.0) / radiated_energy_joules(magnitude)
    assert ratio == pytest.approx(10**1.5, rel=1e-9)
