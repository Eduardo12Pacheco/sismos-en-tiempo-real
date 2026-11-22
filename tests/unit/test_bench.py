"""Benchmark report contracts (S26, D26).

``scripts/bench.py`` is the only source of the numbers published in the README:
it measures the real Compose stack and renders ``docs/BENCHMARKS.md``. These
tests pin the pure parts (histogram quantiles, median and report rendering) so a
formatting change cannot silently desynchronise the committed report from the
command that produced it.
"""

from __future__ import annotations

import pytest
from scripts import bench


def test_histogram_quantile_interpolates_within_bucket() -> None:
    buckets = [
        bench.HistogramBucket(0.01, 0),
        bench.HistogramBucket(0.02, 50),
        bench.HistogramBucket(0.03, 90),
        bench.HistogramBucket(0.04, 100),
        bench.HistogramBucket(float("inf"), 100),
    ]

    assert bench.histogram_quantile(0.50, buckets) == pytest.approx(0.02)
    assert bench.histogram_quantile(0.95, buckets) == pytest.approx(0.035)
    assert bench.histogram_quantile(0.99, buckets) == pytest.approx(0.039)
    assert bench.histogram_quantile(0.00, buckets) == pytest.approx(0.01)


def test_histogram_quantile_returns_none_without_samples() -> None:
    buckets = [
        bench.HistogramBucket(0.01, 0),
        bench.HistogramBucket(float("inf"), 0),
    ]
    assert bench.histogram_quantile(0.95, buckets) is None
    assert bench.histogram_quantile(0.95, []) is None


def test_parse_histogram_reads_prometheus_samples() -> None:
    result = [
        {"metric": {"le": "0.01"}, "value": [1234, "0"]},
        {"metric": {"le": "0.02"}, "value": [1234, "3"]},
        {"metric": {"le": "+Inf"}, "value": [1234, "3"]},
    ]

    buckets = bench.parse_histogram(result)

    assert [bucket.upper_bound for bucket in buckets] == [0.01, 0.02, float("inf")]
    assert [bucket.cumulative for bucket in buckets] == [0.0, 3.0, 3.0]


def test_median_uses_average_of_middle_values() -> None:
    assert bench.median([3.0, 1.0, 2.0]) == 2.0
    assert bench.median([4.0, 1.0, 3.0, 2.0]) == 2.5


def test_render_report_lists_runs_median_and_reproduction_commands() -> None:
    machine = {
        "cpu": "Test CPU 8-core",
        "ram": "31.0 GiB",
        "os": "Test Linux 6.0",
        "docker": "27.0.0",
        "python": "3.12.0",
        "date": "2026-11-20T12:00:00Z",
    }
    runs = [
        bench.BenchRun(
            wall_seconds=12.0, events=1000, events_per_second=83.3, p50_ms=10.0, p95_ms=20.0
        ),
        bench.BenchRun(
            wall_seconds=14.0, events=1000, events_per_second=71.4, p50_ms=12.0, p95_ms=22.0
        ),
        bench.BenchRun(
            wall_seconds=13.0, events=1000, events_per_second=76.9, p50_ms=11.0, p95_ms=21.0
        ),
    ]
    median_run = bench.median_run(runs)

    report = bench.render_report(machine, runs, median_run)

    assert "# Benchmarks" in report
    assert "Test CPU 8-core" in report
    assert "| Wall time (s) | 12.0 | 14.0 | 13.0 | 13.0 |" in report
    assert "| Throughput (events/s) |" in report
    assert "| End-to-end p50 (ms) |" in report
    assert "| End-to-end p95 (ms) |" in report
    assert "make bench" in report
    assert "ingest --speed max" in report
    assert "verify --timeout 120 --json" in report
    assert str(bench.REPO_ROOT) not in report


def test_median_run_returns_the_middle_wall_time() -> None:
    runs = [
        bench.BenchRun(
            wall_seconds=30.0, events=1000, events_per_second=33.3, p50_ms=1.0, p95_ms=2.0
        ),
        bench.BenchRun(
            wall_seconds=10.0, events=1000, events_per_second=100.0, p50_ms=3.0, p95_ms=4.0
        ),
        bench.BenchRun(
            wall_seconds=20.0, events=1000, events_per_second=50.0, p50_ms=5.0, p95_ms=6.0
        ),
    ]

    assert bench.median_run(runs).wall_seconds == 20.0


MACHINE = {
    "cpu": "Test CPU 8-core",
    "ram": "31.0 GiB",
    "os": "Test Linux 6.0",
    "docker": "27.0.0",
    "python": "3.12.0",
    "date": "2026-11-20T12:00:00Z",
}


def _cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def test_median_run_takes_the_median_of_every_metric_across_runs() -> None:
    """The published p95 is the median of the three p95 values, not the run at median wall time."""
    runs = [
        bench.BenchRun(
            wall_seconds=6.7, events=1739, events_per_second=259.7, p50_ms=230.4, p95_ms=470.6
        ),
        bench.BenchRun(
            wall_seconds=6.1, events=1739, events_per_second=283.2, p50_ms=375.0, p95_ms=487.5
        ),
        bench.BenchRun(
            wall_seconds=16.6, events=1739, events_per_second=104.9, p50_ms=230.4, p95_ms=677.5
        ),
    ]

    summary = bench.median_run(runs)

    assert summary.p95_ms == 487.5
    assert summary.p50_ms == 230.4
    assert summary.events_per_second == 259.7
    assert summary.wall_seconds == 6.7


def test_median_run_reports_no_latency_when_a_run_has_none() -> None:
    runs = [
        bench.BenchRun(wall_seconds=1.0, events=1, events_per_second=1.0, p50_ms=None, p95_ms=None),
        bench.BenchRun(wall_seconds=2.0, events=1, events_per_second=1.0, p50_ms=5.0, p95_ms=6.0),
    ]

    summary = bench.median_run(runs)

    assert summary.p50_ms is None
    assert summary.p95_ms is None


@pytest.mark.parametrize("run_count", [1, 3])
def test_render_report_table_header_and_delimiter_have_the_same_cell_count(run_count: int) -> None:
    """GFM renders a table only when the delimiter has one cell per header cell (S2)."""
    runs = [
        bench.BenchRun(
            wall_seconds=10.0 + index,
            events=1000,
            events_per_second=100.0,
            p50_ms=10.0,
            p95_ms=20.0,
        )
        for index in range(run_count)
    ]

    report = bench.render_report(MACHINE, runs, bench.median_run(runs))

    lines = report.splitlines()
    start = next(index for index, line in enumerate(lines) if line.startswith("| Metric |"))
    assert len(_cells(lines[start + 1])) == len(_cells(lines[start]))


def test_committed_report_median_column_is_the_median_of_its_runs() -> None:
    """Each published median is computed from its run cells, not copied from one run (S1, S2)."""
    lines = bench.DEFAULT_OUTPUT.read_text(encoding="utf-8").splitlines()
    start = next(index for index, line in enumerate(lines) if line.startswith("| Metric |"))
    header = _cells(lines[start])
    assert len(_cells(lines[start + 1])) == len(header)
    rows = []
    for line in lines[start + 2 :]:
        if not line.startswith("|"):
            break
        rows.append(_cells(line))
    assert rows
    for cells in rows:
        runs = [float(cell) for cell in cells[1:-1]]
        assert float(cells[-1]) == pytest.approx(bench.median(runs), abs=0.05), cells[0]
