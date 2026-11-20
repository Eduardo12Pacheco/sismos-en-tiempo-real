#!/usr/bin/env python3
"""Generate the provisioned Grafana dashboards from plain Python definitions.

The dashboards are code (D17): every panel, query and layout lives here, and the
committed JSON under ``docker/grafana/dashboards`` is exactly this script's
output. ``--check`` compares the two without writing, so CI fails on drift.

The SQL follows D16: the newest stored event defines the "data clock", and time
columns are shifted by ``dateDiff('second', max(event_time), now())`` at query
time so a replay of a past week looks live on the dashboard. No stored row is
mutated.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, Final

REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DASHBOARD_DIR: Final = REPO_ROOT / "docker" / "grafana" / "dashboards"
DASHBOARD_UIDS: Final = ("seismic-live", "pipeline-health")
PLUGIN_VERSION: Final = "4.23.5"
CLICKHOUSE_TYPE: Final = "grafana-clickhouse-datasource"
PROMETHEUS_TYPE: Final = "prometheus"

# Grafana stamps every dashboard with the schemaVersion it was saved with. The
# value is read from GET /api/dashboards/uid/<uid> on Grafana 13.3.1 (ADR 0008)
# so the provisioned files match the running instance instead of guessing.
SCHEMA_VERSION: Final = 42

CLICKHOUSE_DATASOURCE: Final = {"type": CLICKHOUSE_TYPE, "uid": "clickhouse"}
PROMETHEUS_DATASOURCE: Final = {"type": PROMETHEUS_TYPE, "uid": "prometheus"}

CLOCK_CTE: Final = """WITH (
    SELECT max(event_time) FROM seismic.events_latest
) AS data_now,
dateDiff('second', data_now, now()) AS shift_seconds"""

ECUADOR_NO_VALUE: Final = "No events in the Ecuador region in this data window"


def clickhouse_target(
    ref_id: str,
    sql: str,
    *,
    query_type: str = "table",
) -> dict[str, Any]:
    """Build one ClickHouse SQL target using the D17 shape."""
    if query_type not in {"table", "timeseries"}:
        raise ValueError(f"unsupported ClickHouse query type: {query_type}")
    return {
        "refId": ref_id,
        "datasource": dict(CLICKHOUSE_DATASOURCE),
        "editorType": "sql",
        "rawSql": sql,
        "queryType": query_type,
        "format": 1 if query_type == "table" else 0,
        "pluginVersion": PLUGIN_VERSION,
    }


def prometheus_target(
    ref_id: str,
    expr: str,
    *,
    legend_format: str = "",
    instant: bool = False,
) -> dict[str, Any]:
    """Build one Prometheus target; stats use instant queries, charts use ranges."""
    return {
        "refId": ref_id,
        "datasource": dict(PROMETHEUS_DATASOURCE),
        "expr": expr,
        "instant": instant,
        "range": not instant,
        "legendFormat": legend_format,
    }


def thresholds(*steps: tuple[str, float | None]) -> dict[str, Any]:
    """Build absolute Grafana thresholds from ``(color, value)`` pairs."""
    return {
        "mode": "absolute",
        "steps": [{"color": color, "value": value} for color, value in steps],
    }


def panel(
    *,
    panel_id: int,
    title: str,
    description: str,
    panel_type: str,
    grid: tuple[int, int, int, int],
    targets: Sequence[dict[str, Any]],
    unit: str,
    no_value: str,
    options: dict[str, Any] | None = None,
    overrides: Sequence[dict[str, Any]] = (),
    tags: Sequence[str] = (),
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one panel with the metadata every finished dashboard needs."""
    x, y, width, height = grid
    field_defaults: dict[str, Any] = {
        "unit": unit,
        "noValue": no_value,
        "decimals": 1,
    }
    if defaults:
        field_defaults.update(defaults)
    return {
        "id": panel_id,
        "title": title,
        "description": description,
        "type": panel_type,
        "gridPos": {"h": height, "w": width, "x": x, "y": y},
        "datasource": targets[0]["datasource"],
        "targets": list(targets),
        "fieldConfig": {"defaults": field_defaults, "overrides": list(overrides)},
        "options": options or {},
        "tags": list(tags),
    }


def stat_options(
    *,
    color_mode: str = "value",
    graph_mode: str = "none",
    text_mode: str = "auto",
) -> dict[str, Any]:
    """Options shared by the stat panels."""
    return {
        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
        "colorMode": color_mode,
        "graphMode": graph_mode,
        "justifyMode": "auto",
        "orientation": "auto",
        "textMode": text_mode,
        "wideLayout": True,
    }


def timeseries_options(*, unit_legend: bool = True) -> dict[str, Any]:
    """Options shared by line/bar time-series panels."""
    return {
        "legend": {
            "displayMode": "list",
            "placement": "bottom",
            "showLegend": unit_legend,
            "calcs": [],
        },
        "tooltip": {"mode": "multi", "sort": "none"},
    }


def table_options() -> dict[str, Any]:
    """Options shared by the tables."""
    return {
        "showHeader": True,
        "cellHeight": "sm",
        "footer": {"countRows": False, "fields": "", "reducer": ["sum"], "show": False},
        "sortBy": [{"desc": True, "displayName": "time"}],
    }


def alert_level_override() -> dict[str, Any]:
    """Color the alert level cell and keep missing values readable."""
    return {
        "matcher": {"id": "byName", "options": "Level"},
        "properties": [
            {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "basic"}},
            {
                "id": "mappings",
                "value": [
                    {
                        "type": "value",
                        "options": {
                            "WATCH": {"color": "orange", "index": 0},
                            "WARNING": {"color": "red", "index": 1},
                        },
                    }
                ],
            },
        ],
    }


def usgs_link_override() -> dict[str, Any]:
    """Turn the ``detail_url`` column into a USGS data link."""
    return {
        "matcher": {"id": "byName", "options": "detail_url"},
        "properties": [
            {
                "id": "links",
                "value": [
                    {
                        "title": "Open USGS event",
                        "url": "${__data.fields.detail_url}",
                        "targetBlank": True,
                    }
                ],
            }
        ],
    }


def seismic_live_dashboard() -> dict[str, Any]:
    """The operator-facing Seismic Live dashboard (S16)."""
    panels = [
        panel(
            panel_id=1,
            title="Events (data clock 24h)",
            description=(
                "Earthquakes stored in the last 24 hours of the data clock, where the "
                "newest stored event counts as now."
            ),
            panel_type="stat",
            grid=(0, 0, 4, 4),
            targets=[
                clickhouse_target(
                    "A",
                    """SELECT count() AS "Events"
FROM seismic.events_latest
WHERE event_time >= (SELECT max(event_time) FROM seismic.events_latest)
        - INTERVAL 24 HOUR
  AND magnitude >= $min_mag""",
                )
            ],
            unit="short",
            no_value="No events stored yet",
            options=stat_options(),
            tags=["expect_data", "seismic"],
            defaults={"thresholds": thresholds(("green", None), ("blue", 1))},
        ),
        panel(
            panel_id=2,
            title="Max magnitude",
            description="Largest magnitude stored in the last 24 hours of the data clock.",
            panel_type="stat",
            grid=(4, 0, 4, 4),
            targets=[
                clickhouse_target(
                    "A",
                    """SELECT round(max(magnitude), 1) AS "Max magnitude"
FROM seismic.events_latest
WHERE event_time >= (SELECT max(event_time) FROM seismic.events_latest)
        - INTERVAL 24 HOUR
  AND magnitude >= $min_mag""",
                )
            ],
            unit="short",
            no_value="No events stored yet",
            options=stat_options(),
            tags=["expect_data", "seismic"],
            defaults={"thresholds": thresholds(("green", None), ("orange", 5), ("red", 6))},
        ),
        panel(
            panel_id=3,
            title="Alerts",
            description="Alerts emitted for events in the last 24 hours of the data clock.",
            panel_type="stat",
            grid=(8, 0, 4, 4),
            targets=[
                clickhouse_target(
                    "A",
                    """SELECT count() AS "Alerts"
FROM seismic.alerts_latest
WHERE event_time >= (SELECT max(event_time) FROM seismic.events_latest)
        - INTERVAL 24 HOUR
  AND magnitude >= $min_mag""",
                )
            ],
            unit="short",
            no_value="No alerts in this data window",
            options=stat_options(),
            tags=["expect_data", "seismic"],
            defaults={"thresholds": thresholds(("green", None), ("orange", 1), ("red", 5))},
        ),
        panel(
            panel_id=4,
            title="Data clock",
            description=(
                "Real newest event time and the shift added at query time so the replay "
                "of a past week renders as if it were happening now (D16)."
            ),
            panel_type="stat",
            grid=(12, 0, 6, 4),
            targets=[
                clickhouse_target(
                    "A",
                    """SELECT
    toUnixTimestamp64Milli(max(event_time)) AS "Newest event",
    dateDiff('second', max(event_time), now()) AS "Shift (s)"
FROM seismic.events_latest""",
                )
            ],
            unit="dateTimeAsIso",
            no_value="No events stored yet",
            options=stat_options(text_mode="value_and_name"),
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "Shift (s)"},
                    "properties": [
                        {"id": "unit", "value": "none"},
                        {"id": "decimals", "value": 0},
                    ],
                }
            ],
            tags=["expect_data", "seismic"],
        ),
        panel(
            panel_id=5,
            title="Replay progress",
            description=(
                "Fraction of the replay file already read by the one-shot ingest service; "
                "empty once the replay has finished and the container exits."
            ),
            panel_type="stat",
            grid=(18, 0, 6, 4),
            targets=[
                prometheus_target(
                    "A",
                    "max(seismic_replay_progress_ratio) or vector(0)",
                    legend_format="replay",
                    instant=True,
                )
            ],
            unit="percentunit",
            no_value="No replay running",
            options=stat_options(color_mode="background"),
            tags=["expect_data", "seismic"],
            defaults={"min": 0, "max": 1, "thresholds": thresholds(("blue", None), ("green", 1))},
        ),
        panel(
            panel_id=6,
            title="Earthquake map",
            description=(
                "Offline map: Grafana's bundled countries GeoJSON is the basemap, and "
                "markers are sized by magnitude and colored by depth."
            ),
            panel_type="geomap",
            grid=(0, 4, 12, 12),
            targets=[
                clickhouse_target(
                    "A",
                    """SELECT latitude, longitude, magnitude, depth_km, place
FROM seismic.events_latest
WHERE magnitude >= $min_mag
ORDER BY event_time DESC
LIMIT 500""",
                )
            ],
            unit="none",
            no_value="No events in this data window",
            options={
                "view": {"id": "fit", "allLayers": True, "padding": 12, "noRepeat": False},
                "basemap": {
                    "type": "geojson",
                    "name": "Countries (offline)",
                    "config": {
                        "src": "public/maps/countries.geojson",
                        "rules": [],
                        "style": {
                            "color": {"fixed": "#4f6d8c"},
                            "opacity": 0.35,
                            "lineWidth": 1,
                        },
                    },
                    "opacity": 0.6,
                },
                "layers": [
                    {
                        "type": "markers",
                        "name": "Earthquakes",
                        "config": {
                            "showLegend": True,
                            "style": {
                                "size": {
                                    "field": "magnitude",
                                    "fixed": 5,
                                    "min": 2,
                                    "max": 20,
                                },
                                "color": {"field": "depth_km", "fixed": "dark-red"},
                                "opacity": 0.9,
                                "symbol": {
                                    "mode": "fixed",
                                    "fixed": "img/icons/marker/circle.svg",
                                },
                                "symbolAlign": {"horizontal": "center", "vertical": "center"},
                                "textConfig": {
                                    "fontSize": 12,
                                    "textAlign": "center",
                                    "textBaseline": "middle",
                                    "offsetX": 0,
                                    "offsetY": 0,
                                },
                            },
                        },
                        "location": {
                            "mode": "coords",
                            "latitude": "latitude",
                            "longitude": "longitude",
                        },
                        "tooltip": True,
                    }
                ],
                "controls": {
                    "showZoom": True,
                    "mouseWheelZoom": True,
                    "showAttribution": False,
                    "showScale": False,
                    "showDebug": False,
                    "showMeasure": False,
                },
                "tooltip": {"mode": "details"},
            },
            tags=["expect_data", "seismic"],
        ),
        panel(
            panel_id=7,
            title="Events per hour (stream windows)",
            description=(
                "Sum of the processor's 1-hour tumbling-window counts, shifted to the "
                "data clock so the replay history fills the last 24 hours."
            ),
            panel_type="timeseries",
            grid=(12, 4, 12, 12),
            targets=[
                clickhouse_target(
                    "A",
                    f"""{CLOCK_CTE}
SELECT
    window_start + toIntervalSecond(shift_seconds) AS "time",
    sum(event_count) AS "Events"
FROM seismic.window_stats_latest
GROUP BY window_start
ORDER BY window_start""",
                    query_type="timeseries",
                )
            ],
            unit="short",
            no_value="No stream windows in this data window",
            options=timeseries_options(),
            tags=["expect_data", "seismic"],
            defaults={"custom": {"drawStyle": "bars", "fillOpacity": 40, "lineWidth": 1}},
        ),
        panel(
            panel_id=8,
            title="Top regions",
            description="Regions with the most stored events in the data window (top 10).",
            panel_type="bargauge",
            grid=(0, 16, 8, 8),
            targets=[
                clickhouse_target(
                    "A",
                    """SELECT region AS "Region", count() AS "Events"
FROM seismic.events_latest
WHERE magnitude >= $min_mag
GROUP BY region
ORDER BY count() DESC
LIMIT 10""",
                )
            ],
            unit="short",
            no_value="No regions in this data window",
            options={
                "displayMode": "gradient",
                "orientation": "horizontal",
                "showUnfilled": True,
                "valueMode": "color",
                "namePlacement": "auto",
                "sizing": "auto",
                "minVizHeight": 16,
                "maxVizHeight": 300,
                "reduceOptions": {"calcs": [], "fields": "", "values": True},
            },
            tags=["expect_data", "seismic"],
        ),
        panel(
            panel_id=9,
            title="Magnitude histogram",
            description="Stored events in 0.5-magnitude bins; useful to spot aftershock clusters.",
            panel_type="barchart",
            grid=(8, 16, 8, 8),
            targets=[
                clickhouse_target(
                    "A",
                    """SELECT floor(magnitude * 2) / 2 AS "Magnitude bin", count() AS "Events"
FROM seismic.events_latest
WHERE magnitude >= $min_mag
GROUP BY "Magnitude bin"
ORDER BY "Magnitude bin" """,
                )
            ],
            unit="short",
            no_value="No events in this data window",
            options={
                "xField": "Magnitude bin",
                "orientation": "vertical",
                "stacking": "none",
                "groupWidth": 0.7,
                "barWidth": 0.8,
                "showValue": "auto",
                "legend": {"showLegend": False, "displayMode": "list", "placement": "bottom"},
                "tooltip": {"mode": "single", "sort": "none"},
            },
            tags=["expect_data", "seismic"],
        ),
        panel(
            panel_id=10,
            title="Latest alerts",
            description=(
                "Newest alerts with the USGS detail link behind the detail_url column; "
                "rows are limited to the selected minimum magnitude."
            ),
            panel_type="table",
            grid=(16, 16, 8, 8),
            targets=[
                clickhouse_target(
                    "A",
                    f"""{CLOCK_CTE}
SELECT
    a.event_time + toIntervalSecond(shift_seconds) AS "time",
    a.level AS "Level",
    round(a.magnitude, 1) AS "Magnitude",
    a.place AS "Place",
    a.reason AS "Reason",
    e.detail_url AS "detail_url"
FROM seismic.alerts_latest AS a
LEFT JOIN seismic.events_latest AS e ON a.event_id = e.event_id
WHERE a.magnitude >= $min_mag
ORDER BY a.event_time DESC
LIMIT 50""",
                )
            ],
            unit="short",
            no_value="No alerts in this data window",
            options=table_options(),
            overrides=[alert_level_override(), usgs_link_override()],
            tags=["expect_data", "seismic"],
        ),
        panel(
            panel_id=11,
            title="Latest events",
            description=(
                "Newest earthquake rows with magnitude, place, region and depth; the "
                "detail_url column links back to the USGS event page."
            ),
            panel_type="table",
            grid=(0, 24, 16, 10),
            targets=[
                clickhouse_target(
                    "A",
                    f"""{CLOCK_CTE}
SELECT
    event_time + toIntervalSecond(shift_seconds) AS "time",
    round(magnitude, 1) AS "Magnitude",
    place AS "Place",
    region AS "Region",
    round(depth_km, 1) AS "Depth (km)",
    detail_url AS "detail_url"
FROM seismic.events_latest
WHERE magnitude >= $min_mag
ORDER BY event_time DESC
LIMIT 100""",
                )
            ],
            unit="short",
            no_value="No events in this data window",
            options=table_options(),
            overrides=[usgs_link_override()],
            tags=["expect_data", "seismic"],
        ),
        panel(
            panel_id=12,
            title="Ecuador region watch",
            description=(
                "Events inside the documented Ecuador bounding box (including Galapagos), "
                "with an explicit empty state for data windows without local activity."
            ),
            panel_type="table",
            grid=(16, 24, 8, 10),
            targets=[
                clickhouse_target(
                    "A",
                    f"""{CLOCK_CTE}
SELECT
    event_time + toIntervalSecond(shift_seconds) AS "time",
    round(magnitude, 1) AS "Magnitude",
    place AS "Place",
    round(depth_km, 1) AS "Depth (km)",
    detail_url AS "detail_url"
FROM seismic.events_latest
WHERE in_ecuador_region = 1
  AND magnitude >= $min_mag
ORDER BY event_time DESC
LIMIT 50""",
                )
            ],
            unit="short",
            no_value=ECUADOR_NO_VALUE,
            options=table_options(),
            overrides=[usgs_link_override()],
            tags=["expect_data", "seismic"],
        ),
    ]

    return {
        "annotations": {"list": []},
        "editable": False,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "links": [
            {
                "asDropdown": False,
                "icon": "dashboard",
                "includeVars": True,
                "keepTime": True,
                "tags": [],
                "targetBlank": False,
                "title": "Pipeline Health",
                "tooltip": "Services, throughput, lag and dead letters",
                "type": "link",
                "url": "/d/pipeline-health",
            }
        ],
        "panels": panels,
        "refresh": "5s",
        "schemaVersion": SCHEMA_VERSION,
        "tags": ["seismic", "live", "streaming"],
        "templating": {
            "list": [
                {
                    "name": "min_mag",
                    "label": "Minimum magnitude",
                    "description": "Applied as magnitude >= $min_mag in every seismic query.",
                    "type": "custom",
                    "query": "0,1,2,3,4,5",
                    "options": [
                        {"selected": index == 0, "text": value, "value": value}
                        for index, value in enumerate(["0", "1", "2", "3", "4", "5"])
                    ],
                    "current": {"selected": True, "text": "0", "value": "0"},
                    "hide": 0,
                    "includeAll": False,
                    "multi": False,
                    "refresh": 0,
                    "skipUrlSync": False,
                }
            ]
        },
        "time": {"from": "now-24h", "to": "now"},
        "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
        "timezone": "utc",
        "title": "Seismic Live",
        "uid": "seismic-live",
        "version": 1,
        "weekStart": "",
    }


def pipeline_health_dashboard() -> dict[str, Any]:
    """The operational Pipeline Health dashboard (S16)."""
    panels = [
        panel(
            panel_id=1,
            title="Long-running services up",
            description=(
                "Prometheus up metric for the processor, sink, monitor and Redpanda; "
                "ingest is intentionally absent because it is a one-shot service."
            ),
            panel_type="stat",
            grid=(0, 0, 6, 4),
            targets=[
                prometheus_target(
                    "A",
                    'up{job=~"processor|sink|monitor|redpanda"}',
                    legend_format="{{job}}",
                    instant=True,
                )
            ],
            unit="short",
            no_value="No scrape targets",
            options=stat_options(color_mode="background"),
            tags=["expect_data", "health"],
            defaults={"thresholds": thresholds(("red", None), ("green", 1))},
        ),
        panel(
            panel_id=2,
            title="Dead letters (total)",
            description=(
                "Cumulative DLQ messages by stage and error type, summed across the "
                "ingest and sink services."
            ),
            panel_type="stat",
            grid=(6, 0, 6, 4),
            targets=[
                prometheus_target(
                    "A",
                    "sum(seismic_dlq_messages_total) or vector(0)",
                    legend_format="dead letters",
                    instant=True,
                )
            ],
            unit="short",
            no_value="No dead letters",
            options=stat_options(color_mode="background"),
            tags=["health"],
            defaults={"thresholds": thresholds(("green", None), ("orange", 1), ("red", 10))},
        ),
        panel(
            panel_id=3,
            title="Sink paused",
            description=(
                "1 while the sink has paused its consumer assignment because ClickHouse "
                "is unavailable; back to 0 after recovery."
            ),
            panel_type="stat",
            grid=(12, 0, 6, 4),
            targets=[
                prometheus_target(
                    "A",
                    "max(seismic_sink_paused) or vector(0)",
                    legend_format="paused",
                    instant=True,
                )
            ],
            unit="short",
            no_value="Sink not reporting",
            options=stat_options(color_mode="background"),
            tags=["expect_data", "health"],
            defaults={"thresholds": thresholds(("green", None), ("red", 1))},
        ),
        panel(
            panel_id=4,
            title="Replay schedule lag",
            description=(
                "Seconds by which the replay pacer is behind its schedule; zero at "
                "maximum speed and while the one-shot ingest is not running."
            ),
            panel_type="stat",
            grid=(18, 0, 6, 4),
            targets=[
                prometheus_target(
                    "A",
                    "max(seismic_replay_schedule_lag_seconds) or vector(0)",
                    legend_format="schedule lag",
                    instant=True,
                )
            ],
            unit="s",
            no_value="No replay running",
            options=stat_options(color_mode="background"),
            tags=["expect_data", "health"],
            defaults={"thresholds": thresholds(("green", None), ("orange", 1), ("red", 5))},
        ),
        panel(
            panel_id=5,
            title="Throughput",
            description=(
                "Ingest publish rate and sink row rate per second over a one-minute "
                "Prometheus window."
            ),
            panel_type="timeseries",
            grid=(0, 4, 12, 9),
            targets=[
                prometheus_target(
                    "A",
                    "sum(rate(seismic_ingest_records_total[1m]))",
                    legend_format="ingest events/s",
                ),
                prometheus_target(
                    "B",
                    "sum(rate(seismic_sink_rows_total[1m]))",
                    legend_format="sink rows/s",
                ),
            ],
            unit="ops",
            no_value="No throughput data",
            options=timeseries_options(),
            tags=["expect_data", "throughput"],
        ),
        panel(
            panel_id=6,
            title="Consumer lag",
            description="Consumer lag in messages per Kafka consumer group and topic.",
            panel_type="timeseries",
            grid=(12, 4, 12, 9),
            targets=[
                prometheus_target(
                    "A",
                    "seismic_consumer_lag_messages",
                    legend_format="{{group}} / {{topic}}",
                )
            ],
            unit="short",
            no_value="No lag samples",
            options=timeseries_options(),
            tags=["expect_data", "lag"],
        ),
        panel(
            panel_id=7,
            title="End-to-end latency p50/p95",
            description=(
                "Median and 95th percentile latency from the ingest timestamp Kafka "
                "header to the stored ClickHouse row, computed from the sink histogram."
            ),
            panel_type="timeseries",
            grid=(0, 13, 12, 9),
            targets=[
                prometheus_target(
                    "A",
                    "histogram_quantile(0.5, "
                    "sum by (le) (rate(seismic_sink_end_to_end_seconds_bucket[5m])))",
                    legend_format="p50",
                ),
                prometheus_target(
                    "B",
                    "histogram_quantile(0.95, "
                    "sum by (le) (rate(seismic_sink_end_to_end_seconds_bucket[5m])))",
                    legend_format="p95",
                ),
            ],
            unit="s",
            no_value="No latency samples",
            options=timeseries_options(),
            tags=["expect_data", "latency"],
        ),
        panel(
            panel_id=8,
            title="Sink insert duration p95",
            description=(
                "95th percentile duration of the ClickHouse batch inserts per table, "
                "including retries observed after a transient outage."
            ),
            panel_type="timeseries",
            grid=(12, 13, 12, 9),
            targets=[
                prometheus_target(
                    "A",
                    "histogram_quantile(0.95, sum by (le, table) "
                    "(rate(seismic_sink_insert_seconds_bucket[5m])))",
                    legend_format="{{table}}",
                )
            ],
            unit="s",
            no_value="No insert samples",
            options=timeseries_options(),
            tags=["expect_data", "latency"],
        ),
        panel(
            panel_id=9,
            title="Producer backpressure waits",
            description=(
                "Rate of producer queue-full waits during ingest; non-zero means the "
                "broker or network is applying backpressure."
            ),
            panel_type="timeseries",
            grid=(0, 22, 8, 8),
            targets=[
                prometheus_target(
                    "A",
                    "sum(rate(seismic_ingest_backpressure_waits_total[1m]))",
                    legend_format="waits/s",
                )
            ],
            unit="ops",
            no_value="No backpressure observed",
            options=timeseries_options(),
            tags=["throughput"],
        ),
        panel(
            panel_id=10,
            title="Dead letters by stage and error type",
            description=(
                "Current DLQ counters grouped by stage and error type; the bar gauge "
                "makes the failing stage obvious during chaos runs."
            ),
            panel_type="bargauge",
            grid=(8, 22, 8, 8),
            targets=[
                prometheus_target(
                    "A",
                    "sum by (stage, error_type) (seismic_dlq_messages_total)",
                    legend_format="{{stage}} / {{error_type}}",
                    instant=True,
                )
            ],
            unit="short",
            no_value="No dead letters",
            options={
                "displayMode": "gradient",
                "orientation": "horizontal",
                "showUnfilled": True,
                "valueMode": "color",
                "namePlacement": "auto",
                "sizing": "auto",
                "minVizHeight": 16,
                "maxVizHeight": 300,
                "reduceOptions": {"calcs": [], "fields": "", "values": True},
            },
            tags=["health"],
        ),
        panel(
            panel_id=11,
            title="Redpanda produce/consume latency p95",
            description=(
                "95th percentile broker request latency for produce and consume calls "
                "from Redpanda's public metrics."
            ),
            panel_type="timeseries",
            grid=(16, 22, 8, 8),
            targets=[
                prometheus_target(
                    "A",
                    "histogram_quantile(0.95, sum by (le, redpanda_request) "
                    "(rate(redpanda_kafka_request_latency_seconds_bucket"
                    '{redpanda_request=~"produce|consume"}[5m])))',
                    legend_format="{{redpanda_request}}",
                )
            ],
            unit="s",
            no_value="No broker latency samples",
            options=timeseries_options(),
            tags=["expect_data", "latency"],
        ),
        panel(
            panel_id=12,
            title="Latest dead letters",
            description=(
                "Newest rows from the ClickHouse dead_letters table, including the "
                "preserved payload metadata for triage."
            ),
            panel_type="table",
            grid=(0, 30, 24, 10),
            targets=[
                clickhouse_target(
                    "A",
                    """SELECT
    failed_at AS "time",
    stage AS "Stage",
    error_type AS "Error type",
    error_message AS "Message",
    source_topic AS "Source topic",
    attempts AS "Attempts"
FROM seismic.dead_letters_latest
ORDER BY failed_at DESC
LIMIT 50""",
                )
            ],
            unit="short",
            no_value="No dead letters stored",
            options=table_options(),
            tags=["health"],
        ),
    ]

    return {
        "annotations": {"list": []},
        "editable": False,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 1,
        "links": [
            {
                "asDropdown": False,
                "icon": "dashboard",
                "includeVars": True,
                "keepTime": True,
                "tags": [],
                "targetBlank": False,
                "title": "Seismic Live",
                "tooltip": "Events, map, alerts and Ecuador watch",
                "type": "link",
                "url": "/d/seismic-live",
            }
        ],
        "panels": panels,
        "refresh": "5s",
        "schemaVersion": SCHEMA_VERSION,
        "tags": ["seismic", "pipeline", "health"],
        "templating": {"list": []},
        "time": {"from": "now-24h", "to": "now"},
        "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
        "timezone": "utc",
        "title": "Pipeline Health",
        "uid": "pipeline-health",
        "version": 1,
        "weekStart": "",
    }


def build_dashboards() -> dict[str, dict[str, Any]]:
    """Return every dashboard keyed by uid, exactly as committed."""
    return {
        "seismic-live": seismic_live_dashboard(),
        "pipeline-health": pipeline_health_dashboard(),
    }


def dump_dashboard(dashboard: dict[str, Any]) -> str:
    """Serialize a dashboard with a stable, reviewable layout."""
    return json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n"


def write_dashboards() -> list[Path]:
    """Write all generated dashboards and return their paths."""
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    for uid, dashboard in build_dashboards().items():
        path = DASHBOARD_DIR / f"{uid}.json"
        path.write_text(dump_dashboard(dashboard), encoding="utf-8")
        written.append(path)
    return written


def check_dashboards() -> list[Path]:
    """Return the dashboard files that differ from the generator output."""
    drifted = []
    for uid, dashboard in build_dashboards().items():
        path = DASHBOARD_DIR / f"{uid}.json"
        expected = dump_dashboard(dashboard)
        if not path.exists() or path.read_text(encoding="utf-8") != expected:
            drifted.append(path)
    return drifted


def main(argv: Iterable[str] | None = None) -> int:
    """Write the dashboards, or verify them with ``--check``."""
    parser = argparse.ArgumentParser(
        description="Generate or verify the provisioned Grafana dashboards.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if the committed JSON differs from the generator output",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.check:
        drifted = check_dashboards()
        if drifted:
            names = ", ".join(str(path.relative_to(REPO_ROOT)) for path in drifted)
            print(f"dashboard drift detected: {names}", file=sys.stderr)
            return 1
        print("dashboards are up to date")
        return 0

    for path in write_dashboards():
        print(path.relative_to(REPO_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
