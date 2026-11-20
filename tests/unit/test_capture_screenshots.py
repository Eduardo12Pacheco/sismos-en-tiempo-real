"""Screenshot/GIF capture contracts (S26, D26).

The image assets committed under ``docs/assets/`` are produced by
``scripts/capture_screenshots.py`` against the real Grafana stack. These tests
cover the deterministic helpers: parsing the replay progress metric from the
ingest container and enforcing the ``hero.gif`` size budget before committing.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("PIL.Image", reason="run with the ui dependency group")

from PIL import Image
from scripts import capture_screenshots


def test_parse_metric_returns_labelled_sample() -> None:
    text = """# HELP seismic_replay_progress_ratio progress
# TYPE seismic_replay_progress_ratio gauge
seismic_replay_progress_ratio 0.612500
seismic_ingest_records_total{outcome="published",source="replay"} 1065.0
"""

    assert capture_screenshots.parse_metric(text, "seismic_replay_progress_ratio") == 0.6125
    assert capture_screenshots.parse_metric(text, "missing_metric") is None


def test_build_gif_stays_within_the_size_budget(tmp_path: Path) -> None:
    frames = []
    for index, color in enumerate(((200, 30, 30), (30, 200, 30), (30, 30, 200))):
        frame = tmp_path / f"frame-{index}.png"
        Image.new("RGB", (128, 72), color).save(frame)
        frames.append(frame)
    output = tmp_path / "hero.gif"

    size = capture_screenshots.build_gif(frames, output)

    assert output.is_file()
    assert size == output.stat().st_size
    assert size <= capture_screenshots.MAX_GIF_BYTES
    with Image.open(output) as image:
        assert image.n_frames == 3
        assert image.size == (128, 72)


def test_build_gif_rejects_empty_frame_list(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="frame"):
        capture_screenshots.build_gif([], tmp_path / "empty.gif")
