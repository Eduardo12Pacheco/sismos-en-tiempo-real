"""End-to-end observability proof for S13.

Two things are asserted against the running stack:

* each service's real ``/metrics`` output exposes exactly the catalog subset that
  service owns (no sink gauges leaking into the processor, no missing families);
* Prometheus scrapes the five configured jobs and reports the long-running
  services as ``up`` while the one-shot ingest job is down between replays.
"""

from __future__ import annotations

import subprocess

import pytest

from seismic_stream.adapters.observability.metrics import SERVICE_METRICS
from tests.e2e.helpers import (
    compose,
    container_metric_text,
    metric_samples,
    metric_text,
    prometheus_query,
    run_ingest,
    start_ingest_background,
    wait_until,
)

pytestmark = pytest.mark.e2e

POISON_PRODUCER = (
    "from confluent_kafka import Producer;"
    "p=Producer({'bootstrap.servers':'redpanda:9092'});"
    "p.produce('seismic.events.v1', key=b'observability-poison', value=b'\\xff\\xfeobs');"
    "p.flush(10)"
)

EXPECTED_JOBS = {"ingest", "processor", "sink", "monitor", "redpanda"}
ALWAYS_UP = {"processor", "sink", "monitor", "redpanda"}


def _families(text: str, catalog: set[str]) -> set[str]:
    families: set[str] = set()
    for (name, _labels), _value in metric_samples(text).items():
        normalized = name
        for suffix in ("_created", "_bucket", "_sum", "_count"):
            if name.endswith(suffix) and name[: -len(suffix)] in catalog:
                normalized = name[: -len(suffix)]
                break
        families.add(normalized)
    return {name for name in families if name in catalog}


def test_each_service_exposes_its_metric_catalog_subset(stack: None) -> None:
    ingest = run_ingest("--speed", "max", "--chaos", "--seed", "42")
    assert ingest.returncode == 0, ingest.stderr
    compose(
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "python",
        "sink",
        "-c",
        POISON_PRODUCER,
    )

    processor_catalog = set(SERVICE_METRICS["processor"])
    wait_until(
        lambda: (
            "seismic_processor_skipped_total"
            in _families(metric_text("processor"), processor_catalog)
        ),
        timeout=60,
        message="processor never recorded the skipped poison record",
    )

    monitor_catalog = set(SERVICE_METRICS["monitor"])
    wait_until(
        lambda: (
            {
                "seismic_consumer_lag_messages",
                "seismic_topic_end_offset",
            }
            <= _families(metric_text("monitor"), monitor_catalog)
        ),
        timeout=60,
        message="monitor never exported a lag or end-offset sample",
    )

    # ingest is a one-shot service: scrape it from a detached, slow replay.
    ingest_id = start_ingest_background(["SEISMIC_REPLAY_SPEED=20000"], "--chaos", "--seed", "42")
    try:
        ingest_catalog = set(SERVICE_METRICS["ingest"])

        def ingest_ready() -> bool:
            try:
                return ingest_catalog <= _families(
                    container_metric_text(ingest_id, 9101), ingest_catalog
                )
            except RuntimeError:
                return False

        wait_until(
            ingest_ready,
            timeout=90,
            message="ingest never exported its metric catalog subset",
        )
        ingest_families = _families(container_metric_text(ingest_id, 9101), ingest_catalog)
    finally:
        subprocess.run(["docker", "stop", ingest_id], capture_output=True, check=False)
    assert ingest_families == ingest_catalog

    for service in ("processor", "sink", "monitor"):
        catalog = set(SERVICE_METRICS[service])
        families = _families(metric_text(service), catalog)
        assert families == catalog, f"{service} metric drift: {sorted(catalog ^ families)}"


def test_prometheus_scrapes_the_five_jobs_and_reports_services_up(stack: None) -> None:
    def targets() -> dict[str, str]:
        result = prometheus_query('up{job=~"ingest|processor|sink|monitor|redpanda"}')
        return {str(sample["metric"].get("job")): str(sample["value"][1]) for sample in result}

    wait_until(
        lambda: (
            set(targets()) == EXPECTED_JOBS and all(targets().get(job) == "1" for job in ALWAYS_UP)
        ),
        timeout=90,
        message="Prometheus did not register the five scraped jobs as up",
    )
    assert all(targets().get(job) == "1" for job in ALWAYS_UP)
    # ingest is a one-shot service, so its target is down between replays.
    assert targets().get("ingest") == "0"

    samples = metric_samples(metric_text("sink"))
    assert any(name == "seismic_build_info" for name, _labels in samples)
