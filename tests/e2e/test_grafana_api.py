"""Grafana provisioning and datasource contracts against the real stack (S16).

The test asks Grafana for its provisioned dashboards and executes every panel
target through ``/api/ds/query``. Panels tagged ``expect_data`` must return at
least one row after the bundled replay, so an empty or broken query cannot pass
just because it returned HTTP 200.
"""

from __future__ import annotations

from typing import Any

import pytest

from tests.e2e.helpers import grafana_json

pytestmark = pytest.mark.e2e

DASHBOARD_UIDS = ("seismic-live", "pipeline-health")
DATASOURCE_UIDS = ("clickhouse", "prometheus")
GRAFANA_SCHEMA_VERSION = 42


def _dashboard(uid: str) -> dict[str, Any]:
    response = grafana_json(f"/api/dashboards/uid/{uid}")
    return dict(response["dashboard"])


def _variables(dashboard: dict[str, Any]) -> dict[str, str]:
    variables: dict[str, str] = {}
    for variable in dashboard.get("templating", {}).get("list", []):
        current = variable.get("current", {})
        if "value" in current and current["value"] is not None:
            variables[str(variable["name"])] = str(current["value"])
    return variables


def _interpolated(target: dict[str, Any], variables: dict[str, str]) -> dict[str, Any]:
    query = dict(target)
    for name, value in variables.items():
        query["rawSql"] = str(query.get("rawSql", "")).replace(f"${name}", value)
    query.update({"intervalMs": 10_000, "maxDataPoints": 10_000})
    return query


def _frames(target: dict[str, Any], variables: dict[str, str]) -> list[dict[str, Any]]:
    payload = {
        "queries": [_interpolated(target, variables)],
        "from": "now-24h",
        "to": "now",
    }
    results = grafana_json("/api/ds/query", payload)["results"]
    result = results.get(target["refId"], {})
    assert not result.get("error"), f"{target['refId']} failed: {result['error']}"
    assert int(result.get("status", 200)) < 400, result
    return list(result.get("frames", []))


def _row_count(frames: list[dict[str, Any]]) -> int:
    rows = 0
    for frame in frames:
        values = frame.get("data", {}).get("values") or []
        if values:
            rows += len(values[0])
    return rows


def test_health_and_provisioned_dashboard_search(dashboard_stack: None) -> None:
    health = grafana_json("/api/health")
    assert health["database"] == "ok"
    assert str(health["version"]).startswith("13.")

    dashboards = grafana_json("/api/search?type=dash-db")
    assert {item["uid"] for item in dashboards} == set(DASHBOARD_UIDS)


@pytest.mark.parametrize("uid", DATASOURCE_UIDS)
def test_datasources_report_healthy(uid: str, dashboard_stack: None) -> None:
    health = grafana_json(f"/api/datasources/uid/{uid}/health")

    assert health["status"] == "OK", health
    assert health.get("message")


def test_every_panel_target_executes_and_expect_data_has_rows(
    dashboard_stack: None,
) -> None:
    executed = 0
    for uid in DASHBOARD_UIDS:
        dashboard = _dashboard(uid)
        assert dashboard["schemaVersion"] == GRAFANA_SCHEMA_VERSION
        variables = _variables(dashboard)
        assert dashboard["panels"], uid

        for panel in dashboard["panels"]:
            panel_rows = 0
            for target in panel["targets"]:
                frames = _frames(target, variables)
                executed += 1
                panel_rows += _row_count(frames)
            if "expect_data" in panel.get("tags", []):
                assert panel_rows >= 1, f"{uid}/{panel['title']} returned no rows"

    assert executed >= 20


def test_dashboards_link_to_each_other(dashboard_stack: None) -> None:
    for uid in DASHBOARD_UIDS:
        dashboard = _dashboard(uid)
        other = next(item for item in DASHBOARD_UIDS if item != uid)
        urls = [link["url"] for link in dashboard["links"]]
        assert any(f"/d/{other}" in url for url in urls), uid
