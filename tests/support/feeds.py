"""USGS summary-feed fixtures built from the bundled recorded sample.

Tests never touch the network: these helpers slice the committed recording into
deterministic pages that mimic the real-time summary feed, including an
overlapping page and an in-place revision of one feature.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLE_PATH = REPO_ROOT / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"
FEED_URL_TEMPLATE = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/{feed}.geojson"
REVISION_DELAY_MS = 120_000
LAST_MODIFIED = "Fri, 20 Nov 2026 03:33:00 GMT"


def load_sample() -> dict[str, Any]:
    """Load the committed FeatureCollection as plain JSON objects."""
    payload: dict[str, Any] = json.loads(SAMPLE_PATH.read_text(encoding="utf-8"))
    return payload


def sample_features() -> list[dict[str, Any]]:
    """Return independent copies of every feature in the recorded sample."""
    payload = load_sample()
    features: list[dict[str, Any]] = payload["features"]
    return [deepcopy(feature) for feature in features]


def feature_collection(features: list[dict[str, Any]]) -> dict[str, Any]:
    """Wrap features in the FeatureCollection shape served by USGS."""
    return {"type": "FeatureCollection", "features": deepcopy(features)}


def page_a() -> list[dict[str, Any]]:
    """First recorded page: sample features ``[0:5]``."""
    return sample_features()[0:5]


def page_b() -> list[dict[str, Any]]:
    """Overlapping page: sample features ``[3:8]`` (three are new after A)."""
    return sample_features()[3:8]


def page_c() -> list[dict[str, Any]]:
    """Page B with feature index 4 revised (``updated + 120_000``)."""
    features = page_b()
    properties = features[4]["properties"]
    properties["updated"] = int(properties["updated"]) + REVISION_DELAY_MS
    return features
