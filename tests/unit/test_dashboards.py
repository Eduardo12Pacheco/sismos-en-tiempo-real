"""Dashboard-as-code contracts for the provisioned Grafana dashboards (S16, D15-D17).

These tests are deliberately offline: they validate the committed JSON, the
generator that produces it and the panel rules the dashboards must satisfy.
Grafana's own API and the Playwright smoke test are exercised by the e2e/ui
suite.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from scripts import build_dashboards

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "build_dashboards.py"
DASHBOARD_DIR = REPO_ROOT / "docker" / "grafana" / "dashboards"
DASHBOARD_UIDS = ("seismic-live", "pipeline-health")
CLICKHOUSE_TYPE = "grafana-clickhouse-datasource"
PROMETHEUS_TYPE = "prometheus"
CLICKHOUSE_UIDS = {"clickhouse"}
PROMETHEUS_UIDS = {"prometheus"}
CH_TARGET_KEYS = {
    "refId",
    "datasource",
    "editorType",
    "rawSql",
    "queryType",
    "format",
    "pluginVersion",
}
RAW_TABLE_RE = re.compile(r"(?:FROM|JOIN)\s+seismic\.(\w+)", re.IGNORECASE)
ECUADOR_EMPTY = "No events in the Ecuador region in this data window"


def _load(uid: str) -> dict[str, Any]:
    return json.loads((DASHBOARD_DIR / f"{uid}.json").read_text(encoding="utf-8"))


def _dashboards() -> list[dict[str, Any]]:
    return [_load(uid) for uid in DASHBOARD_UIDS]


def _panels(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    return list(dashboard.get("panels", []))


def _targets(dashboard: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    return [(panel, target) for panel in _panels(dashboard) for target in panel.get("targets", [])]


def _sql(targets: list[tuple[dict[str, Any], dict[str, Any]]]) -> str:
    return "\n".join(
        str(target.get("rawSql", "")) + "\n" + str(target.get("expr", "")) for _, target in targets
    )


def test_generator_check_passes() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--check"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_committed_json_equals_generator_output() -> None:
    generated = build_dashboards.build_dashboards()
    assert set(generated) == set(DASHBOARD_UIDS)
    for uid, dashboard in generated.items():
        committed = (DASHBOARD_DIR / f"{uid}.json").read_text(encoding="utf-8")
        assert build_dashboards.dump_dashboard(dashboard) == committed


@pytest.mark.parametrize("dashboard", _dashboards(), ids=DASHBOARD_UIDS)
def test_panel_ids_are_unique_and_layout_fits_the_grid(dashboard: dict[str, Any]) -> None:
    panels = _panels(dashboard)
    assert panels
    ids = [panel["id"] for panel in panels]
    assert len(ids) == len(set(ids))
    assert all(isinstance(panel_id, int) and panel_id > 0 for panel_id in ids)

    rectangles = []
    for panel in panels:
        grid = panel["gridPos"]
        assert all(isinstance(grid[key], int) for key in ("x", "y", "w", "h"))
        assert grid["w"] > 0
        assert grid["h"] > 0
        assert grid["x"] >= 0
        assert grid["y"] >= 0
        assert grid["x"] + grid["w"] <= 24, panel["title"]
        rectangles.append((panel["title"], grid))

    for index, (left_title, left) in enumerate(rectangles):
        for right_title, right in rectangles[index + 1 :]:
            overlap_x = min(left["x"] + left["w"], right["x"] + right["w"]) - max(
                left["x"], right["x"]
            )
            overlap_y = min(left["y"] + left["h"], right["y"] + right["h"]) - max(
                left["y"], right["y"]
            )
            assert overlap_x <= 0 or overlap_y <= 0, f"{left_title} overlaps {right_title}"


@pytest.mark.parametrize("dashboard", _dashboards(), ids=DASHBOARD_UIDS)
def test_every_panel_is_finished_and_documented(dashboard: dict[str, Any]) -> None:
    for panel in _panels(dashboard):
        title = panel.get("title")
        assert isinstance(title, str)
        assert title.strip(), panel
        description = panel.get("description")
        assert isinstance(description, str)
        assert len(description.strip()) >= 20, title
        targets = panel.get("targets")
        assert isinstance(targets, list)
        assert targets, title
        defaults = panel["fieldConfig"]["defaults"]
        assert isinstance(defaults.get("unit"), str)
        assert defaults["unit"], title
        assert isinstance(defaults.get("noValue"), str)
        assert defaults["noValue"], title
        assert isinstance(panel.get("tags"), list), title


@pytest.mark.parametrize("dashboard", _dashboards(), ids=DASHBOARD_UIDS)
def test_targets_use_provisioned_datasources_and_valid_shapes(
    dashboard: dict[str, Any],
) -> None:
    for panel, target in _targets(dashboard):
        datasource = target.get("datasource")
        assert isinstance(datasource, dict), panel["title"]
        uid = datasource.get("uid")
        assert uid in CLICKHOUSE_UIDS | PROMETHEUS_UIDS, panel["title"]

        if uid in CLICKHOUSE_UIDS:
            assert set(target) >= CH_TARGET_KEYS, panel["title"]
            assert target["editorType"] == "sql"
            assert target["pluginVersion"] == "4.23.5"
            assert isinstance(target["rawSql"], str)
            assert target["rawSql"].strip()
            query_type = target["queryType"]
            assert query_type in {"table", "timeseries"}, panel["title"]
            expected_format = 1 if query_type == "table" else 0
            assert target["format"] == expected_format, panel["title"]
        else:
            assert datasource.get("type") == PROMETHEUS_TYPE
            assert isinstance(target.get("expr"), str)
            assert target["expr"].strip()
            assert target.get("refId"), panel["title"]


@pytest.mark.parametrize("dashboard", _dashboards(), ids=DASHBOARD_UIDS)
def test_clickhouse_queries_read_only_latest_views(dashboard: dict[str, Any]) -> None:
    for panel, target in _targets(dashboard):
        if target["datasource"]["uid"] not in CLICKHOUSE_UIDS:
            continue
        table_names = RAW_TABLE_RE.findall(str(target["rawSql"]))
        assert table_names, panel["title"]
        assert all(name.endswith("_latest") for name in table_names), (panel["title"], table_names)


def test_live_dashboard_has_data_clock_and_minimum_magnitude() -> None:
    dashboard = _load("seismic-live")
    assert dashboard["time"] == {"from": "now-24h", "to": "now"}
    assert dashboard["refresh"] == "5s"
    assert dashboard["timezone"] == "utc"

    variables = {item["name"]: item for item in dashboard["templating"]["list"]}
    assert variables["min_mag"]["type"] == "custom"
    assert variables["min_mag"]["query"] == "0,1,2,3,4,5"
    assert variables["min_mag"]["current"]["value"] == "0"

    titles = {panel["title"] for panel in _panels(dashboard)}
    assert {
        "Events (data clock 24h)",
        "Max magnitude",
        "Alerts",
        "Data clock",
        "Replay progress",
        "Earthquake map",
        "Events per hour (stream windows)",
        "Top regions",
        "Magnitude histogram",
        "Latest alerts",
        "Latest events",
        "Ecuador region watch",
    } <= titles

    for panel in _panels(dashboard):
        if panel["title"] in {"Data clock", "Events per hour (stream windows)"}:
            continue
        for target in panel["targets"]:
            if target["datasource"]["uid"] in CLICKHOUSE_UIDS:
                paragraph = str(target["rawSql"])
                assert "$min_mag" in paragraph, panel["title"]

    sql = _sql(_targets(dashboard))
    assert "dateDiff('second', max(event_time), now())" in sql
    for panel in _panels(dashboard):
        if panel["title"] in {
            "Latest events",
            "Latest alerts",
            "Ecuador region watch",
            "Events per hour (stream windows)",
        }:
            assert any(
                "toIntervalSecond" in str(target["rawSql"]) for target in panel["targets"]
            ), panel["title"]
            assert any(
                "dateDiff('second'" in str(target["rawSql"]) for target in panel["targets"]
            ), panel["title"]

    ecuador = next(
        panel for panel in _panels(dashboard) if panel["title"] == "Ecuador region watch"
    )
    assert ecuador["fieldConfig"]["defaults"]["noValue"] == ECUADOR_EMPTY


def test_live_dashboard_geomap_is_offline_and_complete() -> None:
    dashboard = _load("seismic-live")
    geomap = next(panel for panel in _panels(dashboard) if panel["title"] == "Earthquake map")
    options = geomap["options"]
    assert options["basemap"]["type"] == "geojson"
    assert options["basemap"]["config"]["src"] == "public/maps/countries.geojson"
    assert {layer["type"] for layer in options["layers"]} == {"markers"}
    markers = options["layers"][0]
    assert markers["location"]["latitude"] == "latitude"
    assert markers["location"]["longitude"] == "longitude"
    assert markers["config"]["style"]["size"]["field"] == "magnitude"
    assert markers["config"]["style"]["color"]["field"] == "depth_km"
    assert not {"xyz", "osm", "carto", "esri", "maplibre"} & {
        options["basemap"]["type"],
        *(layer["type"] for layer in options["layers"]),
    }


def test_pipeline_health_covers_every_operational_signal() -> None:
    dashboard = _load("pipeline-health")
    assert dashboard["timezone"] == "utc"
    assert dashboard["refresh"] == "5s"
    sql = _sql(_targets(dashboard))

    required = (
        'up{job=~"processor|sink|monitor|redpanda"}',
        "seismic_dlq_messages_total",
        "seismic_sink_paused",
        "seismic_replay_schedule_lag_seconds",
        "rate(seismic_ingest_records_total[1m])",
        "rate(seismic_sink_rows_total[1m])",
        "seismic_consumer_lag_messages",
        "seismic_sink_end_to_end_seconds_bucket",
        "seismic_sink_insert_seconds_bucket",
        "seismic_ingest_backpressure_waits_total",
        "dead_letters_latest",
        "redpanda_kafka_request_latency_seconds",
    )
    for signal in required:
        assert signal in sql, signal

    assert sql.count("histogram_quantile(0.5,") >= 1
    assert sql.count("histogram_quantile(0.95,") >= 3
    assert "produce" in sql
    assert "consume" in sql


def test_dashboards_link_to_each_other_with_recorded_schema_version() -> None:
    dashboards = {dashboard["uid"]: dashboard for dashboard in _dashboards()}
    assert set(dashboards) == set(DASHBOARD_UIDS)

    for uid, dashboard in dashboards.items():
        assert dashboard["title"] in {"Seismic Live", "Pipeline Health"}
        assert dashboard["tags"]
        assert dashboard["timezone"] == "utc"
        assert dashboard["schemaVersion"] == build_dashboards.SCHEMA_VERSION
        other = next(item for item in DASHBOARD_UIDS if item != uid)
        urls = [link["url"] for link in dashboard["links"]]
        assert any(f"/d/{other}" in url for url in urls), uid
