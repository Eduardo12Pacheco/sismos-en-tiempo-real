"""Committed visual assets contracts (S26, T14).

The screenshots and the hero GIF are produced by ``make screenshots`` against a
real Grafana replay and then committed under ``docs/assets/``. These checks keep
the delivered files honest: real images, the documented viewport and the GIF
size budget that keeps the README fast on GitHub.
"""

from __future__ import annotations

import struct
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ASSETS = ROOT / "docs" / "assets"

PNGS = {
    "seismic-live-dark.png": (1440, 900),
    "seismic-live-light.png": (1440, 900),
    "pipeline-health.png": (1440, 900),
}
GIF_MAX_BYTES = 4 * 1024 * 1024


def _png_size(path: Path) -> tuple[int, int]:
    data = path.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n", f"{path.name} is not a PNG"
    width, height = struct.unpack(">II", data[16:24])
    return width, height


def test_screenshots_are_real_and_sized_for_the_readme() -> None:
    for name, expected in PNGS.items():
        path = ASSETS / name
        assert path.is_file(), f"missing screenshot {name}"
        assert path.stat().st_size > 10_000, f"{name} looks empty"
        assert _png_size(path) == expected, f"{name} is not {expected[0]}x{expected[1]}"


def test_hero_gif_is_small_enough_and_animated() -> None:
    path = ASSETS / "hero.gif"
    assert path.is_file(), "missing docs/assets/hero.gif"
    size = path.stat().st_size
    assert 0 < size <= GIF_MAX_BYTES, f"hero.gif is {size} bytes (max {GIF_MAX_BYTES})"
    data = path.read_bytes()
    assert data[:6] in {b"GIF87a", b"GIF89a"}, "hero.gif is not a GIF"
    assert data.count(b"\x21\xf9\x04") >= 2, "hero.gif has fewer than two frames"
