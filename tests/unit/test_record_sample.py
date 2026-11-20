"""``scripts/record_sample.py`` verification contract (offline)."""

from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "record_sample.py"
SAMPLE = REPO_ROOT / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )


def test_verify_accepts_the_bundled_sample() -> None:
    result = _run("--verify", "--output", str(SAMPLE))

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout


def test_verify_detects_a_checksum_mismatch(tmp_path: Path) -> None:
    sample = tmp_path / "sample.geojson"
    sample.write_text('{"type": "FeatureCollection", "features": []}', encoding="utf-8")
    (tmp_path / "SHA256SUMS").write_text(f"{'0' * 64}  sample.geojson\n", encoding="utf-8")

    result = _run("--verify", "--output", str(sample))

    assert result.returncode == 1
    assert "MISMATCH" in result.stdout


def test_checksum_manifest_names_the_bundled_sample() -> None:
    manifest = SAMPLE.parent / "SHA256SUMS"

    assert manifest.read_text(encoding="utf-8").strip() == (
        f"{hashlib.sha256(SAMPLE.read_bytes()).hexdigest()}  {SAMPLE.name}"
    )
