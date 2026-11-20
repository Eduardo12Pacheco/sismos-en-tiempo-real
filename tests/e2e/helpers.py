"""Shared helpers for the full-stack e2e suite (T10).

Every helper drives the real Compose stack through the CLI, the service metrics
endpoints or ClickHouse. Nothing imports the pipeline's adapters except the
replay source, the chaos policy and the domain ACL used to compute the expected
results independently.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import time
import urllib.parse
import urllib.request
from collections.abc import Callable, Iterable
from datetime import UTC
from pathlib import Path
from typing import Any, Final

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from seismic_stream.adapters.sources.chaos import ChaosPolicy, ChaoticSource
from seismic_stream.adapters.sources.replay import ReplaySource
from seismic_stream.application.ports import SourceRecord
from seismic_stream.domain.errors import EventValidationError
from seismic_stream.domain.normalize import feature_to_event

REPO_ROOT: Final = Path(__file__).resolve().parents[2]
COMPOSE_FILES: Final = ("-f", "compose.yaml", "-f", "compose.test.yaml")
SAMPLE: Final = REPO_ROOT / "data" / "samples" / "usgs_2026-11-02_2026-11-09_m1.geojson"
COMPOSE_TIMEOUT_SECONDS: Final = 600
CLICKHOUSE_HOST: Final = "127.0.0.1"
CLICKHOUSE_PORT: Final = 13123
METRICS_PORTS: Final = {"ingest": 9101, "processor": 9102, "sink": 9103, "monitor": 9104}
EVENTS_TOPIC: Final = "seismic.events.v1"


def docker_available() -> bool:
    """Return whether a Docker engine and the Compose plugin are usable."""
    if shutil.which("docker") is None:
        return False
    try:
        engine = subprocess.run(["docker", "info"], capture_output=True, timeout=20, check=False)
        compose = subprocess.run(
            ["docker", "compose", "version"], capture_output=True, timeout=20, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    return engine.returncode == 0 and compose.returncode == 0


def compose(*args: str, check: bool = True, timeout: int = COMPOSE_TIMEOUT_SECONDS):
    """Run ``docker compose`` against this repository's stack."""
    result = subprocess.run(
        ["docker", "compose", *COMPOSE_FILES, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(args)} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def run_ingest(*args: str) -> subprocess.CompletedProcess[str]:
    """Run the one-shot ingest service against the already-running stack."""
    return compose("run", "--rm", "--no-deps", "ingest", *args)


def start_ingest_background(env: Iterable[str], *args: str) -> str:
    """Start a detached ingest run and return its container id."""
    env_args = [item for value in env for item in ("-e", value)]
    result = compose("run", "--rm", "-d", "--no-deps", *env_args, "ingest", *args)
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return lines[-1]


def container_metric_text(container_id: str, port: int) -> str:
    """Read ``/metrics`` from a detached ``compose run`` container."""
    code = (
        "import urllib.request;"
        f"print(urllib.request.urlopen('http://localhost:{port}/metrics', timeout=5)"
        ".read().decode())"
    )
    result = subprocess.run(
        ["docker", "exec", container_id, "python", "-c", code],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"docker exec metrics probe failed: {result.stderr}")
    return result.stdout


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    """Run the tools-profile CLI container against the running stack."""
    return compose("run", "--rm", "--no-deps", "cli", *args)


def compose_logs() -> str:
    """Capture recent stack logs for a failure message."""
    result = compose("logs", "--no-color", "--tail=300", check=False)
    return (result.stdout or "") + (result.stderr or "")


def clickhouse() -> Client:
    """Open a host-side client to the test override's ClickHouse port."""
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username="seismic",
        password="seismic-local",
        database="seismic",
    )


def query_rows(sql: str) -> list[tuple[Any, ...]]:
    """Run a ClickHouse query and return its rows."""
    client = clickhouse()
    try:
        return list(client.query(sql).result_rows)
    finally:
        client.close()


def scalar(sql: str) -> Any:
    """Return the first column of the first row."""
    rows = query_rows(sql)
    return rows[0][0] if rows else None


def events_count() -> int:
    """Number of deduplicated events stored so far."""
    return int(scalar("SELECT count() FROM seismic.events_latest") or 0)


def metric_text(service: str, port: int | None = None) -> str:
    """Read ``/metrics`` from a service container without touching the network."""
    resolved = port or METRICS_PORTS[service]
    code = (
        "import urllib.request;"
        f"print(urllib.request.urlopen('http://localhost:{resolved}/metrics', timeout=5)"
        ".read().decode())"
    )
    result = compose(
        "exec",
        "-T",
        service,
        "python",
        "-c",
        code,
    )
    return result.stdout


def service_health(service: str, path: str = "/readyz") -> int:
    """Return the HTTP status that ``service`` answers on its own health path."""
    port = METRICS_PORTS[service]
    code = (
        "import urllib.request;"
        f"print(urllib.request.urlopen('http://localhost:{port}{path}', timeout=5).status)"
    )
    result = compose("exec", "-T", service, "python", "-c", code)
    return int(result.stdout.strip().splitlines()[-1])


_LABEL_RE = re.compile(r'(\w+)="([^"]*)"')


def metric_samples(text: str) -> dict[tuple[str, tuple[tuple[str, str], ...]], float]:
    """Parse Prometheus text exposition into ``(name, labels) -> value``."""
    samples: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        head, _, raw_value = stripped.rpartition(" ")
        name = head.split("{", 1)[0]
        labels = tuple(sorted(_LABEL_RE.findall(head)))
        try:
            samples[(name, labels)] = float(raw_value)
        except ValueError:
            continue
    return samples


def find_metric(
    samples: dict[tuple[str, tuple[tuple[str, str], ...]], float],
    name: str,
    **labels: str,
) -> float | None:
    """Look up one sample by name and exact label set."""
    key = (name, tuple(sorted(labels.items())))
    return samples.get(key)


def prometheus_query(query: str) -> list[dict[str, Any]]:
    """Run a PromQL query through the Prometheus container."""
    encoded = urllib.parse.quote(query, safe="")
    url = f"http://localhost:9090/api/v1/query?query={encoded}"
    result = compose("exec", "-T", "prometheus", "wget", "-qO-", url)
    payload = json.loads(result.stdout)
    return list(payload["data"]["result"])


def wait_until(
    predicate: Callable[[], bool],
    *,
    timeout: float,
    interval: float = 0.5,
    message: str = "condition not met in time",
) -> None:
    """Poll ``predicate`` until true or raise with the supplied message."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)


def earthquake_events(records: Iterable[SourceRecord]) -> list:
    """Normalize valid earthquake features from source records."""
    events = []
    for record in records:
        try:
            event = feature_to_event(record.feature)
        except EventValidationError:
            continue
        if event.event_type == "earthquake":
            events.append(event)
    return events


def sample_earthquake_ids() -> set[str]:
    """Ids of the valid earthquakes in the bundled sample."""
    return {event.event_id for event in earthquake_events(ReplaySource(SAMPLE).records())}


def chaotic_records() -> list[SourceRecord]:
    """Materialize the deterministic chaos stream used by the e2e proof."""
    source = ChaoticSource(ReplaySource(SAMPLE), ChaosPolicy(seed=42))
    return list(source.records())


def to_milliseconds(value: Any) -> int:
    """Convert a ClickHouse ``DateTime64`` value to epoch milliseconds.

    clickhouse-connect may deserialize ``DateTime64`` as a naive datetime even
    when the column declares the ``UTC`` time zone. Treat naive values as UTC so
    the assertion never depends on the host's local timezone.
    """
    if getattr(value, "tzinfo", None) is None:
        value = value.replace(tzinfo=UTC)
    return round(value.timestamp() * 1000)


GRAFANA_URL: Final = "http://127.0.0.1:13000"


def grafana_json(
    path: str,
    payload: dict[str, Any] | None = None,
    *,
    timeout: int = 60,
) -> Any:
    """Call Grafana's HTTP API and decode the JSON response."""
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{GRAFANA_URL}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))
