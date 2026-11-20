#!/usr/bin/env python3
"""Measure the real Compose pipeline and write ``docs/BENCHMARKS.md``.

The benchmark is intentionally end-to-end: it starts a clean stack, replays the
bundled one-week sample at ``--speed max`` and stops the clock only when
``seismic-stream verify`` confirms that every earthquake is stored and the sink
lag is zero. End-to-end latency comes from the sink histogram through the
Prometheus API, not from an in-process timer, so the published numbers describe
what an operator would observe.

Three clean runs are performed and the median is reported (S26). Every command
is printed in the generated file so a reader can reproduce the measurement.
"""

from __future__ import annotations

import argparse
import json
import platform
import re
import subprocess
import sys
import time
import urllib.parse
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

REPO_ROOT: Final = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT: Final = REPO_ROOT / "docs" / "BENCHMARKS.md"
COMPOSE: Final = ("docker", "compose", "-f", "compose.yaml")
STACK_SERVICES: Final = (
    "redpanda",
    "clickhouse",
    "prometheus",
    "processor",
    "sink",
    "monitor",
)
DEFAULT_RUNS: Final = 3
VERIFY_TIMEOUT_SECONDS: Final = 120
PROMETHEUS_QUERY_URL: Final = "http://localhost:9090/api/v1/query"
LATENCY_METRIC: Final = "seismic_sink_end_to_end_seconds"
SCRAPE_TIMEOUT_SECONDS: Final = 60
JSON_LINE_RE = re.compile(r"^\{.*\}$")


@dataclass(frozen=True, slots=True)
class HistogramBucket:
    """One cumulative Prometheus histogram bucket."""

    upper_bound: float
    cumulative: float


@dataclass(frozen=True, slots=True)
class BenchRun:
    """One clean replay measurement."""

    wall_seconds: float
    events: int
    events_per_second: float
    p50_ms: float | None
    p95_ms: float | None


def compose(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Run ``docker compose`` against this repository's base stack."""
    result = subprocess.run(
        [*COMPOSE, *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(args)} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def histogram_quantile(quantile: float, buckets: Sequence[HistogramBucket]) -> float | None:
    """Interpolate a quantile from cumulative buckets, like Prometheus does."""
    if not buckets:
        return None
    ordered = sorted(buckets, key=lambda bucket: bucket.upper_bound)
    total = ordered[-1].cumulative
    if total <= 0:
        return None
    rank = quantile * total
    previous_upper = 0.0
    previous_cumulative = 0.0
    for bucket in ordered:
        if bucket.cumulative < rank:
            previous_upper = bucket.upper_bound
            previous_cumulative = bucket.cumulative
            continue
        if bucket.upper_bound == float("inf"):
            return previous_upper
        if bucket.cumulative == previous_cumulative:
            return bucket.upper_bound
        fraction = (rank - previous_cumulative) / (bucket.cumulative - previous_cumulative)
        return previous_upper + fraction * (bucket.upper_bound - previous_upper)
    return ordered[-1].upper_bound


def parse_histogram(result: Sequence[Mapping[str, Any]]) -> list[HistogramBucket]:
    """Convert a Prometheus instant-query result into cumulative buckets."""
    buckets = []
    for sample in result:
        labels = sample.get("metric", {})
        raw_value = sample.get("value", [0, "0"])[1]
        buckets.append(
            HistogramBucket(
                upper_bound=float(labels["le"]),
                cumulative=float(raw_value),
            )
        )
    return buckets


def median(values: Sequence[float]) -> float:
    """Return the statistical median of a non-empty sequence."""
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def median_run(runs: Sequence[BenchRun]) -> BenchRun:
    """Return the run whose wall time is the median of the series."""
    ordered = sorted(runs, key=lambda run: run.wall_seconds)
    return ordered[len(ordered) // 2]


def prometheus_query(expression: str) -> list[dict[str, Any]]:
    """Run one instant PromQL query through the running Prometheus container."""
    encoded = urllib.parse.quote(expression, safe="")
    result = compose(
        "exec",
        "-T",
        "prometheus",
        "wget",
        "-qO-",
        f"{PROMETHEUS_QUERY_URL}?query={encoded}",
    )
    payload = json.loads(result.stdout)
    if payload.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {payload}")
    return list(payload["data"]["result"])


def _scalar_query(expression: str) -> float:
    result = prometheus_query(expression)
    if not result:
        return 0.0
    return float(result[0]["value"][1])


def measure_latency(expected_events: int) -> tuple[float | None, float | None]:
    """Wait for a stable scrape and return the p50/p95 latency in milliseconds."""
    deadline = time.monotonic() + SCRAPE_TIMEOUT_SECONDS
    previous: float | None = None
    while time.monotonic() < deadline:
        observed = _scalar_query(f"sum({LATENCY_METRIC}_count)")
        if observed >= expected_events and observed == previous:
            break
        previous = observed
        time.sleep(2.0)
    else:
        raise RuntimeError(
            f"Prometheus did not observe {expected_events} latency samples within "
            f"{SCRAPE_TIMEOUT_SECONDS}s"
        )
    buckets = parse_histogram(prometheus_query(f"sum by (le) ({LATENCY_METRIC}_bucket)"))
    p50 = histogram_quantile(0.50, buckets)
    p95 = histogram_quantile(0.95, buckets)
    return (
        None if p50 is None else p50 * 1000.0,
        None if p95 is None else p95 * 1000.0,
    )


def _verify_json(stdout: str) -> dict[str, Any]:
    for line in reversed(stdout.splitlines()):
        candidate = line.strip()
        if JSON_LINE_RE.match(candidate):
            return dict(json.loads(candidate))
    raise RuntimeError(f"verify did not print a JSON object:\n{stdout}")


def run_once(index: int, total: int) -> BenchRun:
    """Run one clean end-to-end measurement."""
    print(f"[bench] run {index}/{total}: starting a clean stack", flush=True)
    compose("down", "-v", "--remove-orphans", check=False)
    compose("up", "-d", "--wait", *STACK_SERVICES)

    print(f"[bench] run {index}/{total}: replaying at --speed max", flush=True)
    started = time.monotonic()
    compose("run", "--rm", "--no-deps", "ingest", "--speed", "max")
    verify = compose(
        "run",
        "--rm",
        "--no-deps",
        "cli",
        "verify",
        "--timeout",
        str(VERIFY_TIMEOUT_SECONDS),
        "--json",
    )
    wall_seconds = time.monotonic() - started
    result = _verify_json(verify.stdout)
    if not result.get("ok"):
        raise RuntimeError(f"verify failed after the replay: {result}")
    events = int(result["stored"])
    p50_ms, p95_ms = measure_latency(events)
    events_per_second = events / wall_seconds
    print(
        f"[bench] run {index}/{total}: {events} events in {wall_seconds:.2f}s "
        f"({events_per_second:.1f} events/s), p50={p50_ms}, p95={p95_ms} ms",
        flush=True,
    )
    return BenchRun(
        wall_seconds=wall_seconds,
        events=events,
        events_per_second=events_per_second,
        p50_ms=p50_ms,
        p95_ms=p95_ms,
    )


def _read_first(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()[0].strip()
    except OSError:
        return None


def host_info() -> dict[str, str]:
    """Collect the machine facts a reader needs to interpret the numbers."""
    cpu = "unknown"
    try:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.lower().startswith("model name"):
                cpu = line.split(":", 1)[-1].strip() or "unknown"
                break
    except OSError:
        cpu = platform.processor() or "unknown"
    ram = "unknown"
    try:
        meminfo = Path("/proc/meminfo").read_text(encoding="utf-8")
        match = re.search(r"^MemTotal:\s+(\d+)\s+kB$", meminfo, re.MULTILINE)
        if match:
            ram = f"{int(match.group(1)) / (1024 * 1024):.1f} GiB"
    except OSError:
        ram = "unknown"
    systems = Path("/etc/os-release")
    os_name = platform.platform()
    if systems.is_file():
        for line in systems.read_text(encoding="utf-8").splitlines():
            if line.startswith("PRETTY_NAME="):
                os_name = line.split("=", 1)[1].strip().strip('"')
                break
    docker = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    return {
        "cpu": cpu,
        "ram": ram,
        "os": os_name,
        "docker": docker or "unknown",
        "python": platform.python_version(),
        "date": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def _format_number(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f}"


def render_report(
    machine: Mapping[str, str],
    runs: Sequence[BenchRun],
    median_result: BenchRun,
) -> str:
    """Render the committed benchmark report from measured values."""
    columns = " | ".join(f"Run {index}" for index in range(1, len(runs) + 1))
    separator = " | ".join("---" for _ in range(len(runs) + 2))

    def row(label: str, values: Sequence[str], median_value: str) -> str:
        return f"| {label} | {' | '.join(values)} | {median_value} |"

    lines = [
        "# Benchmarks",
        "",
        "Generated by `make bench` (`scripts/bench.py`). Do not edit by hand:",
        "the script starts a clean Compose stack, replays the bundled sample at",
        "`--speed max` and stops the clock when `seismic-stream verify` confirms",
        "delivery and zero sink lag.",
        "",
        "## Machine",
        "",
        "| Item | Value |",
        "| --- | --- |",
        f"| Date (UTC) | {machine['date']} |",
        f"| CPU | {machine['cpu']} |",
        f"| RAM | {machine['ram']} |",
        f"| OS | {machine['os']} |",
        f"| Docker Engine | {machine['docker']} |",
        f"| Python | {machine['python']} |",
        "",
        "## Results",
        "",
        f"Median of {len(runs)} clean runs. Wall time covers container start, the",
        "full replay and `verify`; throughput is stored earthquakes divided by that",
        "wall time. Latency is the sink's ingest-to-storage histogram through the",
        "Prometheus API (milliseconds).",
        "",
        f"| Metric | {columns} | Median |",
        f"| --- | {separator} |",
        row(
            "Wall time (s)",
            [_format_number(run.wall_seconds) for run in runs],
            _format_number(median_result.wall_seconds),
        ),
        row(
            "Stored earthquakes",
            [str(run.events) for run in runs],
            str(median_result.events),
        ),
        row(
            "Throughput (events/s)",
            [_format_number(run.events_per_second) for run in runs],
            _format_number(median_result.events_per_second),
        ),
        row(
            "End-to-end p50 (ms)",
            [_format_number(run.p50_ms) for run in runs],
            _format_number(median_result.p50_ms),
        ),
        row(
            "End-to-end p95 (ms)",
            [_format_number(run.p95_ms) for run in runs],
            _format_number(median_result.p95_ms),
        ),
        "",
        "## Reproduce",
        "",
        "```sh",
        "uv sync --frozen",
        "docker compose build",
        "docker compose -f compose.yaml up -d --wait redpanda clickhouse \\",
        "  prometheus processor sink monitor",
        "docker compose -f compose.yaml run --rm --no-deps ingest --speed max",
        "docker compose -f compose.yaml run --rm --no-deps cli verify --timeout 120 --json",
        "make bench",
        "```",
        "",
        "The numbers depend on the host, Docker Desktop's VM and other containers",
        "running at the same time; they demonstrate this machine, not a service",
        "level objective.",
        "",
    ]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Run the benchmark suite and write the report."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS, help="clean runs (default: 3)")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="report path")
    args = parser.parse_args(argv)
    if args.runs < 1:
        parser.error("--runs must be at least 1")

    try:
        compose("build")
        runs = [run_once(index, args.runs) for index in range(1, args.runs + 1)]
    except (RuntimeError, OSError, json.JSONDecodeError) as error:
        print(f"error: benchmark failed: {error}", file=sys.stderr)
        return 1
    finally:
        compose("down", "-v", "--remove-orphans", check=False)

    report = render_report(host_info(), runs, median_run(runs))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    try:
        shown = args.output.relative_to(REPO_ROOT)
    except ValueError:
        shown = args.output
    print(f"wrote {shown}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
