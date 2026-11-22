"""Optional USGS real-time summary-feed source.

The adapter polls the documented USGS GeoJSON summary feeds, makes conditional
requests once a ``Last-Modified`` header is known, and yields only features whose
``(id, updated)`` pair has not been seen in a bounded LRU window. Every wait goes
through the injected :class:`~seismic_stream.application.ports.Clock`, so the
backoff and polling contract is deterministic and fully testable offline.
"""

from __future__ import annotations

import logging
import math
import random
import threading
from collections import OrderedDict
from collections.abc import Iterator, Mapping
from typing import Any, Final

import httpx

from seismic_stream import __version__
from seismic_stream.adapters.sources.replay import event_time_ms_from_feature
from seismic_stream.application.ports import Clock, SourceError, SourceRecord

LOGGER = logging.getLogger(__name__)

FEED_BASE_URL: Final = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary"
ALLOWED_FEEDS: Final[frozenset[str]] = frozenset(
    {
        "all_hour",
        "all_day",
        "all_week",
        "2.5_hour",
        "2.5_day",
        "4.5_hour",
        "4.5_day",
        "significant_hour",
        "significant_day",
    }
)
PROJECT_URL: Final = "https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real"
MIN_POLL_INTERVAL_SECONDS: Final = 60.0
BASE_BACKOFF_SECONDS: Final = 60.0
MAX_BACKOFF_SECONDS: Final = 900.0
SEEN_CAPACITY: Final = 50_000

SeenKey = tuple[str, int | None]


def feed_url(feed: str) -> str:
    """Return the summary-feed URL for ``feed``."""
    return f"{FEED_BASE_URL}/{feed}.geojson"


def default_user_agent() -> str:
    """Identify this application to USGS, as their guidance requests."""
    return f"realtime-seismic-stream/{__version__} (+{PROJECT_URL})"


def backoff_cap(attempt: int) -> float:
    """Un-jittered exponential backoff cap for a zero-based attempt counter."""
    if attempt < 0:
        raise ValueError(f"attempt must be >= 0, got {attempt}")
    exponent = min(attempt, 16)
    return min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * float(2**exponent))


def jittered_backoff(
    attempt: int,
    rng: random.Random,
    retry_after: float | None = None,
) -> float:
    """Full-jitter delay bounded by :func:`backoff_cap`.

    A server-provided ``Retry-After`` is a floor: the wait is never shorter than
    the instruction, even when the random jitter would be.
    """
    delay = rng.uniform(0.0, backoff_cap(attempt))
    if retry_after is not None and retry_after > delay:
        return retry_after
    return delay


def parse_retry_after(value: str | None) -> float | None:
    """Parse a numeric ``Retry-After`` header; anything else is ignored."""
    if value is None:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return seconds


class UsgsLiveSource:
    """Poll a USGS summary feed and yield new or revised features."""

    def __init__(
        self,
        client: httpx.Client,
        *,
        clock: Clock,
        rng: random.Random,
        user_agent: str,
        stop: threading.Event,
        feed: str = "all_hour",
        poll_interval_s: float = 60.0,
    ) -> None:
        if feed not in ALLOWED_FEEDS:
            allowed = ", ".join(sorted(ALLOWED_FEEDS))
            raise ValueError(f"unsupported USGS feed {feed!r}; expected one of: {allowed}")
        if not math.isfinite(poll_interval_s) or poll_interval_s < MIN_POLL_INTERVAL_SECONDS:
            raise ValueError(
                "poll_interval_s must be a finite number >= 60 "
                f"(USGS polling guidance), got {poll_interval_s!r}"
            )

        self._client = client
        self._clock = clock
        self._rng = rng
        self._user_agent = user_agent
        self._stop = stop
        self._feed = feed
        self._poll_interval_s = poll_interval_s
        self._url = feed_url(feed)

    def records(self) -> Iterator[SourceRecord]:
        """Yield unseen features forever, sleeping only through the clock."""
        last_modified: str | None = None
        seen: OrderedDict[SeenKey, None] = OrderedDict()
        attempt = 0

        while not self._stop.is_set():
            try:
                response = self._fetch(last_modified)
            except httpx.HTTPError as exc:
                reason = f"{type(exc).__name__}: {exc}"
                delay = self._log_failure(attempt, reason, retry_after=None)
                attempt += 1
                self._clock.sleep(delay)
                continue

            status = response.status_code
            if status == 304:
                attempt = 0
                last_modified = response.headers.get("Last-Modified", last_modified)
                self._wait_for_next_poll()
                continue

            if status != 200:
                retry_after = (
                    parse_retry_after(response.headers.get("Retry-After"))
                    if status == 429
                    else None
                )
                delay = self._log_failure(attempt, f"HTTP {status}", retry_after)
                attempt += 1
                self._clock.sleep(delay)
                continue

            try:
                features = _parse_features(response)
            except SourceError as exc:
                delay = self._log_failure(attempt, str(exc), retry_after=None)
                attempt += 1
                self._clock.sleep(delay)
                continue

            attempt = 0
            header = response.headers.get("Last-Modified")
            if header is not None:
                last_modified = header

            for feature in _fresh_features(features, seen):
                yield SourceRecord(
                    feature=feature,
                    event_time_ms=event_time_ms_from_feature(feature),
                )

            self._wait_for_next_poll()

    def _fetch(self, last_modified: str | None) -> httpx.Response:
        headers = {"User-Agent": self._user_agent}
        if last_modified is not None:
            headers["If-Modified-Since"] = last_modified
        return self._client.get(self._url, headers=headers)

    def _log_failure(self, attempt: int, reason: str, retry_after: float | None) -> float:
        delay = jittered_backoff(attempt, self._rng, retry_after=retry_after)
        LOGGER.warning(
            "usgs live feed fetch failed (attempt %d): %s; retrying in %.3f s",
            attempt + 1,
            reason,
            delay,
        )
        return delay

    def _wait_for_next_poll(self) -> None:
        if not self._stop.is_set():
            self._clock.sleep(self._poll_interval_s)


def _parse_features(response: httpx.Response) -> list[dict[str, Any]]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise SourceError(f"invalid JSON from USGS feed: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise SourceError("USGS feed payload is not a GeoJSON FeatureCollection")
    raw_features = payload.get("features")
    if not isinstance(raw_features, list):
        raise SourceError("USGS feed payload is not a GeoJSON FeatureCollection")

    features: list[dict[str, Any]] = []
    for index, raw_feature in enumerate(raw_features):
        if not isinstance(raw_feature, Mapping):
            raise SourceError(f"USGS feed feature {index} is not a JSON object")
        features.append(dict(raw_feature))
    return features


def _fresh_features(
    features: list[dict[str, Any]],
    seen: OrderedDict[SeenKey, None],
) -> list[dict[str, Any]]:
    fresh: list[dict[str, Any]] = []
    for feature in features:
        key = _feature_key(feature)
        if key in seen:
            continue
        seen[key] = None
        fresh.append(feature)
        while len(seen) > SEEN_CAPACITY:
            seen.popitem(last=False)

    fresh.sort(key=_feature_sort_key)
    return fresh


def _feature_key(feature: Mapping[str, Any]) -> SeenKey:
    raw_id = feature.get("id")
    feature_id = raw_id if isinstance(raw_id, str) else ""
    properties = feature.get("properties")
    updated: int | None = None
    if isinstance(properties, Mapping):
        raw_updated = properties.get("updated")
        if isinstance(raw_updated, int) and not isinstance(raw_updated, bool):
            updated = raw_updated
    return (feature_id, updated)


def _feature_sort_key(feature: Mapping[str, Any]) -> tuple[int, int, str]:
    event_time_ms = event_time_ms_from_feature(feature)
    raw_id = feature.get("id")
    feature_id = raw_id if isinstance(raw_id, str) else ""
    if event_time_ms is None:
        return (1, 0, feature_id)
    return (0, event_time_ms, feature_id)
