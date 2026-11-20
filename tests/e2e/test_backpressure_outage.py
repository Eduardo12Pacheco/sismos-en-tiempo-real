"""End-to-end proof of S11 backpressure and automatic recovery.

The replay is paced to last longer than the ClickHouse outage. While the store is
down the sink must pause its assignment (visible as ``seismic_sink_paused=1``),
retry, and leave the consumer lag growing. After the store returns, the sink must
resume, drain every topic and ``verify`` must find every earthquake stored.
"""

from __future__ import annotations

import json
import subprocess

import pytest

from tests.e2e.helpers import (
    compose,
    find_metric,
    metric_samples,
    metric_text,
    run_cli,
    start_ingest_background,
    wait_until,
)

pytestmark = pytest.mark.e2e


def _sink_paused() -> bool:
    samples = metric_samples(metric_text("sink"))
    return find_metric(samples, "seismic_sink_paused") == 1.0


def _sink_lag() -> int:
    samples = metric_samples(metric_text("monitor"))
    lag = find_metric(
        samples,
        "seismic_consumer_lag_messages",
        group="seismic-sink",
        topic="seismic.events.v1",
    )
    return int(lag or 0)


def _retries() -> float:
    samples = metric_samples(metric_text("sink"))
    return find_metric(samples, "seismic_sink_retries_total") or 0.0


def _wait_container(container_id: str, timeout: float) -> int:
    result = subprocess.run(
        ["docker", "wait", container_id],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    return int(result.stdout.strip().splitlines()[-1])


def test_clickhouse_outage_pauses_the_sink_and_recovers_without_loss(stack: None) -> None:
    container_id = start_ingest_background(["SEISMIC_REPLAY_SPEED=20000"])
    try:
        # Let the replay fill the pipeline before taking the store down.
        wait_until(
            lambda: _sink_lag() > 0,
            timeout=60,
            message="sink never built up a backlog before the outage",
        )

        compose("stop", "clickhouse")
        wait_until(
            lambda: _sink_paused() and _retries() > 0,
            timeout=60,
            message="sink did not pause and retry while ClickHouse was down",
        )
        wait_until(
            lambda: _sink_lag() > 0,
            timeout=60,
            message="consumer lag did not grow while the sink was paused",
        )

        compose("start", "clickhouse")
        wait_until(
            _clickhouse_healthy,
            timeout=120,
            message="ClickHouse did not become healthy again",
        )

        assert _wait_container(container_id, timeout=300) == 0

        verify = run_cli("verify", "--timeout", "240", "--json")
        assert verify.returncode == 0, verify.stdout + verify.stderr
        payload = json.loads([line for line in verify.stdout.splitlines() if line.strip()][-1])
        assert payload["ok"] is True
        assert payload["missing"] == 0
        assert all(lag == 0 for lag in payload["lag"].values())
    finally:
        subprocess.run(["docker", "stop", container_id], capture_output=True, check=False)


def _clickhouse_healthy() -> bool:
    result = compose(
        "exec",
        "-T",
        "clickhouse",
        "wget",
        "--spider",
        "-q",
        "http://localhost:8123/ping",
        check=False,
    )
    return result.returncode == 0
