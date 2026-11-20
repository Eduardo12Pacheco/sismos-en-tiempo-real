"""Full-stack Compose fixtures for the e2e suite (S1, S11-S14).

The fixture builds the application image once per session and starts a clean
stack for every test, so the chaos, outage and poison scenarios never inherit
topics, offsets or rows from a previous scenario. Docker is required for e2e:
locally the suite skips when it is missing, while CI sets
``SEISMIC_REQUIRE_DOCKER=1`` to turn that skip into a failure.
"""

from __future__ import annotations

import os
import time

import pytest

from tests.e2e.helpers import compose, compose_logs, docker_available, run_cli, run_ingest


@pytest.fixture(scope="session")
def built_images() -> None:
    """Build the application image once and require a working Docker engine."""
    if not docker_available():
        message = "Docker with the Compose plugin is required for e2e tests"
        if os.environ.get("SEISMIC_REQUIRE_DOCKER") == "1":
            pytest.fail(message)
        pytest.skip(message)

    result = compose("build", check=False)
    if result.returncode != 0:
        output = (result.stdout or "") + (result.stderr or "")
        pytest.fail(f"docker compose build failed:\n{output[-4000:]}")


@pytest.fixture
def stack(built_images: None) -> None:
    """Start a clean full stack and tear it down with its volumes afterwards."""
    compose("down", "-v", "--remove-orphans", check=False)
    started = compose(
        "up",
        "-d",
        "--wait",
        "redpanda",
        "clickhouse",
        "prometheus",
        "grafana",
        "processor",
        "sink",
        "monitor",
        check=False,
    )
    if started.returncode != 0:
        logs = compose_logs()
        compose("down", "-v", "--remove-orphans", check=False)
        pytest.fail(
            "e2e stack failed to become healthy:\n"
            f"{started.stdout}\n{started.stderr}\n--- logs ---\n{logs[-8000:]}"
        )
    yield
    if os.environ.get("SEISMIC_KEEP_STACK") != "1":
        compose("down", "-v", "--remove-orphans", check=False)


def _start_stack() -> None:
    compose("down", "-v", "--remove-orphans", check=False)
    started = compose(
        "up",
        "-d",
        "--wait",
        "redpanda",
        "clickhouse",
        "prometheus",
        "grafana",
        "processor",
        "sink",
        "monitor",
        check=False,
    )
    if started.returncode != 0:
        logs = compose_logs()
        compose("down", "-v", "--remove-orphans", check=False)
        pytest.fail(
            "dashboard stack failed to become healthy:\n"
            f"{started.stdout}\n{started.stderr}\n--- logs ---\n{logs[-8000:]}"
        )


def _replay_and_verify() -> None:
    # A finite replay at ``max`` finishes faster than Prometheus' 5 s scrape
    # interval, so the ingest/sink counters are born at their final value and
    # ``rate(...[1m])`` has no non-zero sample. Use the documented screenshot
    # speed (3600x, ~3 minutes) so throughput panels observe real increments.
    replay = run_ingest("--speed", "3600")
    if replay.returncode != 0:
        pytest.fail(
            "dashboard replay failed:\n"
            f"{(replay.stdout or '')[-2000:]}\n{(replay.stderr or '')[-2000:]}"
        )
    verified = run_cli("verify", "--timeout", "180")
    if verified.returncode != 0:
        logs = compose_logs()
        pytest.fail(
            "verify after the dashboard replay failed:\n"
            f"{(verified.stdout or '')[-2000:]}\n{(verified.stderr or '')[-2000:]}\n"
            f"--- logs ---\n{logs[-8000:]}"
        )
    # Prometheus scrapes every 5 s. Give it a chance to observe the sink and
    # broker counters once more before dashboard panels compute rates.
    time.sleep(10)


@pytest.fixture(scope="module")
def dashboard_stack(built_images: None) -> None:
    """One replayed stack per dashboard module; the dashboards are read-only."""
    _start_stack()
    try:
        _replay_and_verify()
        yield
    finally:
        if os.environ.get("SEISMIC_KEEP_STACK") != "1":
            compose("down", "-v", "--remove-orphans", check=False)
