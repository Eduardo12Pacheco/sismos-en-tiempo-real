"""Full-stack Compose fixtures for the e2e suite (S1, S11-S14).

The fixture builds the application image once per session and starts a clean
stack for every test, so the chaos, outage and poison scenarios never inherit
topics, offsets or rows from a previous scenario. Docker is required for e2e:
locally the suite skips when it is missing, while CI sets
``SEISMIC_REQUIRE_DOCKER=1`` to turn that skip into a failure.
"""

from __future__ import annotations

import os

import pytest

from tests.e2e.helpers import compose, compose_logs, docker_available


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
