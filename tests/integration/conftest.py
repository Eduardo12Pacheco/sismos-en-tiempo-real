"""Session fixture for integration tests that need the Docker Compose subset.

The stack starts from a clean state so bootstrap assertions ("created" on the
first run) are meaningful, and is torn down with ``down -v`` unless
``SEISMIC_KEEP_STACK=1``. Docker is optional locally: tests skip when it is
missing, unless ``SEISMIC_REQUIRE_DOCKER=1`` (set by CI).
"""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILES = ("-f", "compose.yaml", "-f", "compose.test.yaml")
COMPOSE_TIMEOUT_SECONDS = 300


def docker_available() -> bool:
    """Return whether a working Docker engine and Compose plugin are present."""
    if shutil.which("docker") is None:
        return False
    try:
        engine = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=20,
            check=False,
        )
        compose = subprocess.run(
            ["docker", "compose", "version"],
            capture_output=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return engine.returncode == 0 and compose.returncode == 0


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run ``docker compose`` against this repository's Compose files."""
    return subprocess.run(
        ["docker", "compose", *COMPOSE_FILES, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=COMPOSE_TIMEOUT_SECONDS,
        check=check,
    )


@pytest.fixture(scope="session")
def stack() -> Iterator[None]:
    """Start Redpanda and ClickHouse and tear the subset down afterwards."""
    if not docker_available():
        message = "Docker with the Compose plugin is required for integration tests"
        if os.environ.get("SEISMIC_REQUIRE_DOCKER") == "1":
            pytest.fail(message)
        pytest.skip(message)

    compose("down", "-v", "--remove-orphans", check=False)
    started = compose("up", "-d", "--wait", "redpanda", "clickhouse", check=False)
    if started.returncode != 0:
        logs = compose("logs", "--no-color", "--tail=200", check=False)
        pytest.fail(f"Test stack failed to start:\n{started.stderr}\n{logs.stdout}\n{logs.stderr}")
    try:
        yield
    finally:
        if os.environ.get("SEISMIC_KEEP_STACK") != "1":
            compose("down", "-v", "--remove-orphans", check=False)
