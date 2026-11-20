"""``replay-info`` command: sample profile and checksum reporting."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from typer.testing import CliRunner

from seismic_stream.cli import app

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE = REPO_ROOT / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"
runner = CliRunner()


def test_replay_info_reports_sample_profile() -> None:
    result = runner.invoke(app, ["replay-info", "--file", str(SAMPLE)])

    assert result.exit_code == 0, result.output
    assert "features: 1817" in result.stdout
    assert "earthquake: 1739" in result.stdout
    assert "checksum: OK" in result.stdout
    assert "2026-11-02T00:02:24" in result.stdout
    assert "2026-11-08T23:50:41" in result.stdout


def test_replay_info_json_is_machine_readable() -> None:
    result = runner.invoke(app, ["replay-info", "--file", str(SAMPLE), "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["features"] == 1817
    assert payload["types"]["earthquake"] == 1739
    assert payload["checksum"] == "ok"
    assert payload["sha256"] == hashlib.sha256(SAMPLE.read_bytes()).hexdigest()
    assert payload["first_event_time"].startswith("2026-11-02T00:02:24")
    assert payload["last_event_time"].startswith("2026-11-08T23:50:41")


def test_replay_info_reports_missing_checksum_manifest(tmp_path: Path) -> None:
    sample = tmp_path / "sample.geojson"
    sample.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "id": "x1",
                        "properties": {"mag": 2.0, "time": 1_730_000_000_000, "type": "earthquake"},
                        "geometry": {"type": "Point", "coordinates": [-70.0, -2.0, 10.0]},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = runner.invoke(app, ["replay-info", "--file", str(sample), "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload["checksum"] == "not_available"
    assert payload["features"] == 1


def test_replay_info_fails_on_checksum_mismatch(tmp_path: Path) -> None:
    sample = tmp_path / "sample.geojson"
    sample.write_text('{"type": "FeatureCollection", "features": []}', encoding="utf-8")
    (tmp_path / "SHA256SUMS").write_text(f"{'0' * 64}  sample.geojson\n", encoding="utf-8")

    result = runner.invoke(app, ["replay-info", "--file", str(sample)])

    assert result.exit_code == 1
    assert "checksum: MISMATCH" in result.stdout


def test_replay_info_missing_file_exits_with_runtime_error(tmp_path: Path) -> None:
    result = runner.invoke(app, ["replay-info", "--file", str(tmp_path / "absent.geojson")])

    assert result.exit_code == 1
    assert "does not exist" in result.output
