"""End-to-end proof for S10(c): poison bytes never stop the pipeline.

Raw garbage is produced into ``seismic.events.v1``. The processor must stay
ready and skip it, while the sink must isolate it as exactly one
``sink_decode`` dead letter that preserves the original payload. Afterwards a
normal replay must still store new events.
"""

from __future__ import annotations

import itertools

import pytest

from seismic_stream.adapters.sources.replay import ReplaySource
from tests.e2e.helpers import (
    SAMPLE,
    compose,
    earthquake_events,
    events_count,
    find_metric,
    metric_samples,
    metric_text,
    query_rows,
    run_ingest,
    scalar,
    service_health,
    wait_until,
)

pytestmark = pytest.mark.e2e

POISON = b"\xff\xfepoison-batch"
PRODUCER = (
    "from confluent_kafka import Producer;"
    "p=Producer({'bootstrap.servers':'redpanda:9092'});"
    f"p.produce('seismic.events.v1', key=b'poison', value={POISON!r});"
    "p.flush(10)"
)
LIMIT = 100


def _dead_letters() -> list[tuple[str, str, str, str, int, int]]:
    return [
        (str(stage), str(error_type), str(payload), str(topic), int(partition), int(offset))
        for stage, error_type, payload, topic, partition, offset in query_rows(
            "SELECT stage, error_type, hex(payload), source_topic, source_partition, "
            "source_offset FROM seismic.dead_letters_latest WHERE stage = 'sink_decode'"
        )
    ]


def test_poison_bytes_are_isolated_and_the_pipeline_keeps_running(stack: None) -> None:
    produced = compose(
        "run",
        "--rm",
        "--no-deps",
        "--entrypoint",
        "python",
        "sink",
        "-c",
        PRODUCER,
    )
    assert produced.returncode == 0, produced.stderr

    wait_until(
        lambda: len(_dead_letters()) == 1,
        timeout=60,
        message="the sink did not isolate the poison record as a sink_decode dead letter",
    )
    stage, error_type, payload_hex, topic, partition, offset = _dead_letters()[0]
    assert stage == "sink_decode"
    assert error_type
    assert payload_hex.lower() == POISON.hex()
    assert topic == "seismic.events.v1"
    assert partition == 0
    assert offset >= 0

    assert service_health("processor") == 200

    expected = {
        event.event_id
        for event in earthquake_events(itertools.islice(ReplaySource(SAMPLE).records(), LIMIT))
    }
    ingested = run_ingest("--speed", "max", "--no-chaos", "--limit", str(LIMIT))
    assert ingested.returncode == 0, ingested.stderr

    wait_until(
        lambda: events_count() == len(expected),
        timeout=90,
        message="events after the poison record were not stored",
    )
    wait_until(
        lambda: (
            find_metric(
                metric_samples(metric_text("monitor")),
                "seismic_consumer_lag_messages",
                group="seismic-sink",
                topic="seismic.events.v1",
            )
            == 0
        ),
        timeout=90,
        message="sink lag did not return to zero after the poison record",
    )
    assert service_health("processor") == 200
    assert int(scalar("SELECT count() FROM seismic.dead_letters_latest") or 0) == 1
