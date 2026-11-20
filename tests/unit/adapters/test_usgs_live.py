"""Offline contract tests for the optional USGS live summary-feed source."""

from __future__ import annotations

import json
import logging
import math
import random
import threading
from typing import Any

import httpx
import pytest
import respx

from seismic_stream.adapters.sources import usgs_live
from seismic_stream.adapters.sources.usgs_live import (
    ALLOWED_FEEDS,
    UsgsLiveSource,
    default_user_agent,
    feed_url,
    parse_retry_after,
)
from tests.support import feeds
from tests.support.fakes import FakeClock

URL = feed_url("all_hour")


class MaxRng:
    """Deterministic RNG that always picks the upper jitter bound."""

    def uniform(self, low: float, high: float) -> float:
        return high


def _source(
    clock: FakeClock | None = None,
    *,
    rng: random.Random | MaxRng | None = None,
    stop: threading.Event | None = None,
    feed: str = "all_hour",
    poll_interval_s: float = 60.0,
) -> tuple[UsgsLiveSource, FakeClock, threading.Event, httpx.Client]:
    client = httpx.Client()
    clock = clock or FakeClock()
    stop = stop or threading.Event()
    source = UsgsLiveSource(
        client=client,
        clock=clock,
        rng=rng if rng is not None else random.Random(7),
        user_agent=default_user_agent(),
        stop=stop,
        feed=feed,
        poll_interval_s=poll_interval_s,
    )
    return source, clock, stop, client


def _ok(
    features: list[dict[str, Any]],
    last_modified: str | None = None,
) -> httpx.Response:
    headers = {"Last-Modified": last_modified} if last_modified is not None else None
    return httpx.Response(200, json=feeds.feature_collection(features), headers=headers)


def _ids(records: list[Any]) -> list[str]:
    return [str(record.feature["id"]) for record in records]


def _sorted_ids(features: list[dict[str, Any]]) -> list[str]:
    ordered = sorted(
        features,
        key=lambda feature: (
            feature["properties"]["time"],
            feature["id"],
        ),
    )
    return [str(feature["id"]) for feature in ordered]


def test_first_fetch_yields_every_feature_and_sends_user_agent(
    respx_mock: respx.MockRouter,
) -> None:
    features = feeds.page_a()
    route = respx_mock.get(URL).mock(return_value=_ok(features))
    source, _, _, client = _source()
    try:
        iterator = source.records()
        records = [next(iterator) for _ in range(len(features))]
        iterator.close()
    finally:
        client.close()

    assert _ids(records) == _sorted_ids(features)
    assert route.call_count == 1
    request = route.calls[0].request
    assert request.headers["User-Agent"] == default_user_agent()
    assert request.headers["User-Agent"].startswith("realtime-seismic-stream/")


def test_features_are_sorted_by_time_then_id(respx_mock: respx.MockRouter) -> None:
    features = list(reversed(feeds.page_a()))
    respx_mock.get(URL).mock(return_value=_ok(features))
    source, _, _, client = _source()
    try:
        iterator = source.records()
        records = [next(iterator) for _ in range(len(features))]
        iterator.close()
    finally:
        client.close()

    assert _ids(records) == _sorted_ids(features)


def test_second_poll_yields_only_unseen_features(respx_mock: respx.MockRouter) -> None:
    first = feeds.page_a()
    second = feeds.page_b()
    respx_mock.get(URL).mock(side_effect=[_ok(first), _ok(second)])
    source, _, _, client = _source()
    try:
        records = []
        iterator = source.records()
        while len(records) < len(first) + 3:
            records.append(next(iterator))
        iterator.close()
    finally:
        client.close()

    assert len(records) == len(first) + 3
    assert _ids(records[: len(first)]) == _sorted_ids(first)
    assert _ids(records[len(first) :]) == _sorted_ids(second[2:])


def test_revised_feature_is_yielded_again(respx_mock: respx.MockRouter) -> None:
    first = feeds.page_a()
    second = feeds.page_b()
    revised = feeds.page_c()
    respx_mock.get(URL).mock(side_effect=[_ok(first), _ok(second), _ok(revised)])
    source, _, _, client = _source()
    try:
        records = []
        iterator = source.records()
        while len(records) < len(first) + 3 + 1:
            records.append(next(iterator))
        iterator.close()
    finally:
        client.close()

    assert len(records) == len(first) + 3 + 1
    last = records[-1]
    assert last.feature["id"] == second[4]["id"]
    assert last.feature["properties"]["updated"] == (
        second[4]["properties"]["updated"] + feeds.REVISION_DELAY_MS
    )


def test_304_yields_nothing_and_sends_if_modified_since(
    respx_mock: respx.MockRouter,
) -> None:
    first = feeds.page_a()
    calls: list[httpx.Request] = []
    stop = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) == 1:
            return _ok(first, last_modified=feeds.LAST_MODIFIED)
        stop.set()
        return httpx.Response(304)

    respx_mock.get(URL).mock(side_effect=handler)
    source, _, _, client = _source(stop=stop)
    try:
        records = list(source.records())
    finally:
        client.close()

    assert _ids(records) == _sorted_ids(first)
    assert len(calls) == 2
    assert calls[1].headers["If-Modified-Since"] == feeds.LAST_MODIFIED


def test_429_retry_after_is_honored_via_clock(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "120"}),
            _ok(feeds.page_a()),
        ]
    )
    source, clock, _, client = _source(rng=MaxRng())
    try:
        first = next(source.records())
    finally:
        client.close()

    assert first.feature["id"] == feeds.page_a()[0]["id"]
    assert clock.total_slept >= 120.0
    assert clock.sleeps[0] == pytest.approx(120.0)


def test_consecutive_503_backs_off_60_120_240_then_resets(
    respx_mock: respx.MockRouter,
) -> None:
    first = feeds.page_a()
    respx_mock.get(URL).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(503),
            _ok(first),
        ]
    )
    source, clock, _, client = _source(rng=MaxRng())
    try:
        first_record = next(source.records())
    finally:
        client.close()

    assert first_record.feature["id"] == first[0]["id"]
    assert clock.sleeps == pytest.approx([60.0, 120.0, 240.0])


def test_attempt_counter_resets_after_a_success(respx_mock: respx.MockRouter) -> None:
    first = feeds.page_a()
    second = feeds.page_b()
    respx_mock.get(URL).mock(
        side_effect=[
            httpx.Response(503),
            _ok(first),
            httpx.Response(503),
            _ok(second),
        ]
    )
    source, clock, _, client = _source(rng=MaxRng())
    try:
        iterator = source.records()
        records = []
        while len(records) < len(first) + 1:
            records.append(next(iterator))
        iterator.close()
    finally:
        client.close()

    assert len(records) == len(first) + 1
    assert clock.sleeps[0] == pytest.approx(60.0)
    assert clock.sleeps[1] == pytest.approx(60.0)  # poll interval
    assert clock.sleeps[2] == pytest.approx(60.0)  # reset to the first cap


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, content=b"not json"),
        httpx.Response(200, json=[]),
        httpx.Response(200, json={"type": "Feature"}),
        httpx.Response(200, json={"type": "FeatureCollection", "features": [1]}),
        httpx.Response(500),
        httpx.Response(404),
    ],
    ids=[
        "invalid-json",
        "payload-not-an-object",
        "not-a-collection",
        "feature-not-an-object",
        "server-error",
        "client-error",
    ],
)
def test_unusable_responses_are_logged_and_retried(
    respx_mock: respx.MockRouter,
    response: httpx.Response,
    caplog: pytest.LogCaptureFixture,
) -> None:
    respx_mock.get(URL).mock(side_effect=[response, _ok(feeds.page_a())])
    source, clock, _, client = _source(rng=MaxRng())
    try:
        with caplog.at_level(
            logging.WARNING,
            logger="seismic_stream.adapters.sources.usgs_live",
        ):
            first = next(source.records())
    finally:
        client.close()

    assert first.feature["id"] == feeds.page_a()[0]["id"]
    assert clock.sleeps == pytest.approx([60.0])
    assert "usgs live feed" in caplog.text.lower()


def test_connection_error_is_logged_and_retried(respx_mock: respx.MockRouter) -> None:
    respx_mock.get(URL).mock(
        side_effect=[httpx.ConnectError("no route to host"), _ok(feeds.page_a())]
    )
    source, clock, _, client = _source(rng=MaxRng())
    try:
        first = next(source.records())
    finally:
        client.close()

    assert first.feature["id"] == feeds.page_a()[0]["id"]
    assert clock.sleeps == pytest.approx([60.0])


@pytest.mark.parametrize("feed", sorted(ALLOWED_FEEDS))
def test_every_documented_feed_is_accepted(
    respx_mock: respx.MockRouter,
    feed: str,
) -> None:
    respx_mock.get(feed_url(feed)).mock(return_value=_ok(feeds.page_a()))
    source, _, _, client = _source(feed=feed)
    try:
        first = next(source.records())
    finally:
        client.close()

    assert first.event_time_ms is not None


def test_unknown_feed_is_rejected() -> None:
    with pytest.raises(ValueError, match="feed"):
        _source(feed="all_decade")


@pytest.mark.parametrize("poll_interval_s", [0.0, 30.0, 59.999, -5.0, math.nan, math.inf])
def test_poll_interval_below_one_minute_is_rejected(poll_interval_s: float) -> None:
    with pytest.raises(ValueError, match="poll_interval_s"):
        _source(poll_interval_s=poll_interval_s)


@pytest.mark.respx(assert_all_called=False)
def test_pre_set_stop_makes_no_requests(respx_mock: respx.MockRouter) -> None:
    route = respx_mock.get(URL)
    stop = threading.Event()
    stop.set()
    source, _, _, client = _source(stop=stop)
    try:
        records = list(source.records())
    finally:
        client.close()

    assert records == []
    assert route.call_count == 0


def test_user_agent_contains_project_url_and_version() -> None:
    agent = default_user_agent()

    assert agent.startswith("realtime-seismic-stream/")
    assert "https://github.com/Eduardo12Pacheco/realtime-seismic-stream" in agent


def test_failures_do_not_write_to_stdout(
    respx_mock: respx.MockRouter,
    capsys: pytest.CaptureFixture[str],
) -> None:
    respx_mock.get(URL).mock(side_effect=[httpx.Response(500), _ok(feeds.page_a())])
    source, _, _, client = _source(rng=MaxRng())
    try:
        next(source.records())
    finally:
        client.close()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_fixtures_are_serializable() -> None:
    payload = json.loads(json.dumps(feeds.feature_collection(feeds.page_a())))

    assert payload["type"] == "FeatureCollection"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("120", 120.0),
        (" 30 ", 30.0),
        ("yesterday", None),
        ("-1", None),
        ("nan", None),
        ("inf", None),
    ],
)
def test_parse_retry_after(value: str | None, expected: float | None) -> None:
    assert parse_retry_after(value) == expected


def test_features_without_time_or_updated_are_sorted_last(
    respx_mock: respx.MockRouter,
) -> None:
    timed = feeds.page_a()[0]
    no_properties = {"id": "no-properties", "properties": []}
    bad_updated = {"id": "bad-updated", "properties": {"updated": "yesterday"}}
    respx_mock.get(URL).mock(return_value=_ok([no_properties, bad_updated, timed]))
    source, _, _, client = _source()
    try:
        iterator = source.records()
        records = [next(iterator) for _ in range(3)]
        iterator.close()
    finally:
        client.close()

    assert records[0].feature["id"] == timed["id"]
    assert records[0].event_time_ms is not None
    assert _ids(records[1:]) == ["bad-updated", "no-properties"]
    assert all(record.event_time_ms is None for record in records[1:])


def test_seen_keys_are_a_bounded_lru(
    respx_mock: respx.MockRouter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(usgs_live, "SEEN_CAPACITY", 2)
    features = feeds.sample_features()[:3]
    first, second, third = features
    respx_mock.get(URL).mock(
        side_effect=[
            _ok([first, second]),
            _ok([third, first]),
        ]
    )
    source, _, _, client = _source()
    try:
        iterator = source.records()
        records = [next(iterator) for _ in range(4)]
        iterator.close()
    finally:
        client.close()

    ids = _ids(records)
    assert ids.count(str(first["id"])) == 2  # evicted, therefore yielded again
    assert _ids(records[2:]) == _sorted_ids([first, third])
