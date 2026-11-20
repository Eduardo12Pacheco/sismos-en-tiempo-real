"""Radiated seismic energy estimation."""

from __future__ import annotations


def radiated_energy_joules(magnitude: float) -> float:
    """Return the Gutenberg-Richter radiated energy for a magnitude.

    ``log10(E) = 1.5 * M + 4.8`` with ``E`` in joules. This is an accepted
    approximation, not a seismological measurement.
    """
    return 10 ** (1.5 * magnitude + 4.8)
