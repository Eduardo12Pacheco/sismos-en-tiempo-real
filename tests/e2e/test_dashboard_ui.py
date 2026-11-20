"""Playwright smoke test for the finished dashboards in both themes (S16).

The browser talks to the real provisioned Grafana instance. It verifies that
every panel title is visible, that no panel reports an error, that the offline
Geomap actually painted pixels, and that the Ecuador table shows either rows or
its configured empty state.
"""

from __future__ import annotations

import io
from typing import Any

import pytest

from tests.e2e.helpers import grafana_json

pytestmark = pytest.mark.ui

sync_api = pytest.importorskip("playwright.sync_api", reason="run with the ui dependency group")
image_module = pytest.importorskip("PIL.Image", reason="run with the ui dependency group")
image_stat = pytest.importorskip("PIL.ImageStat", reason="run with the ui dependency group")

GRAFANA_URL = "http://127.0.0.1:13000"
ECUADOR_EMPTY = "No events in the Ecuador region in this data window"
DASHBOARD_UIDS = ("seismic-live", "pipeline-health")


def _titles(uid: str) -> list[str]:
    dashboard = grafana_json(f"/api/dashboards/uid/{uid}")["dashboard"]
    return [panel["title"] for panel in dashboard["panels"]]


def _panel(page: Any, title: str) -> Any:
    heading = page.get_by_role("heading", name=title, exact=True)
    assert heading.count() == 1, title
    return heading.locator("xpath=ancestor::*[@data-viz-panel-key][1]")


def _non_blank(png: bytes) -> bool:
    image = image_module.open(io.BytesIO(png)).convert("RGB")
    statistics = image_stat.Stat(image)
    colors = image.getcolors(maxcolors=1_000_000) or []
    return max(statistics.stddev) > 2.0 and len(colors) > 50


def test_dashboards_render_in_dark_and_light(dashboard_stack: None) -> None:
    seismic_titles = _titles("seismic-live")
    health_titles = _titles("pipeline-health")

    with sync_api.sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])
        try:
            for theme in ("dark", "light"):
                page = browser.new_page(viewport={"width": 1440, "height": 900})

                page.goto(
                    f"{GRAFANA_URL}/d/seismic-live?kiosk&theme={theme}",
                    wait_until="networkidle",
                    timeout=60_000,
                )
                page.wait_for_function(
                    "(count) => document.querySelectorAll('h2').length >= count",
                    arg=len(seismic_titles),
                    timeout=60_000,
                )
                page.wait_for_timeout(5_000)

                for title in seismic_titles:
                    assert page.get_by_role("heading", name=title, exact=True).is_visible(), (
                        f"{theme}: {title} is not visible"
                    )
                assert page.locator(".panel-error").count() == 0
                assert page.locator("[data-testid*='panel-error']").count() == 0
                assert page.locator("[aria-label*='Panel error']").count() == 0

                geomap = _panel(page, "Earthquake map")
                canvases = geomap.locator("canvas")
                assert canvases.count() >= 1
                assert any(
                    _non_blank(canvases.nth(index).screenshot())
                    for index in range(canvases.count())
                ), f"{theme}: the Geomap canvas is blank"

                ecuador_panel = _panel(page, "Ecuador region watch")
                ecuador_panel.scroll_into_view_if_needed()
                ecuador = ""
                for _ in range(20):
                    ecuador = ecuador_panel.inner_text()
                    if ECUADOR_EMPTY in ecuador or "Salinas" in ecuador:
                        break
                    page.wait_for_timeout(500)
                assert ECUADOR_EMPTY in ecuador or "Salinas" in ecuador, ecuador

                page.goto(
                    f"{GRAFANA_URL}/d/pipeline-health?kiosk&theme={theme}",
                    wait_until="networkidle",
                    timeout=60_000,
                )
                page.wait_for_function(
                    "(count) => document.querySelectorAll('h2').length >= count",
                    arg=len(health_titles),
                    timeout=60_000,
                )
                page.wait_for_timeout(5_000)
                for title in health_titles:
                    assert page.get_by_role("heading", name=title, exact=True).is_visible(), (
                        f"{theme}: {title} is not visible"
                    )
                assert page.locator(".panel-error").count() == 0
                assert page.locator("[data-testid*='panel-error']").count() == 0
                page.close()
        finally:
            browser.close()
