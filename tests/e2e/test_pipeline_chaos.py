"""End-to-end chaos proof for S12 (effectively-once delivery).

A clean stack replays the bundled sample at maximum speed with deterministic
chaos (malformed copies, exact duplicates and revisions). The test then proves
the S12 invariants against ClickHouse and the independent reference
implementation: stored ids, revised versions, dead-letter counts, aggregates and
alerts.
"""

from __future__ import annotations

import json
import logging
import math

import pytest

from seismic_stream.adapters.sources.chaos import ChaosPolicy
from seismic_stream.adapters.sources.replay import ReplaySource
from tests.e2e.helpers import (
    SAMPLE,
    chaotic_records,
    events_count,
    query_rows,
    run_cli,
    run_ingest,
    sample_earthquake_ids,
    scalar,
    to_milliseconds,
)
from tests.support.reference import events_from_records, reference_result

pytestmark = pytest.mark.e2e


def _last_json(stdout: str) -> dict[str, object]:
    lines = [line for line in stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def _approx(left: float, right: float) -> bool:
    return math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9)


def test_chaos_replay_is_effectively_once(stack: None) -> None:
    ingest = run_ingest("--speed", "max", "--chaos", "--seed", "42")
    assert ingest.returncode == 0, ingest.stderr
    assert json.loads(ingest.stdout.strip().splitlines()[-1])["invalid"] > 0

    verify = run_cli("verify", "--timeout", "180", "--json")
    assert verify.returncode == 0, verify.stdout + verify.stderr
    assert _last_json(verify.stdout)["ok"] is True

    expected_ids = sample_earthquake_ids()
    stored = {
        str(event_id): to_milliseconds(updated_at)
        for event_id, updated_at in query_rows(
            "SELECT event_id, updated_at FROM seismic.events_latest"
        )
    }
    assert set(stored) == expected_ids

    reference = reference_result(records=chaotic_records())

    # Revised ids carry the bumped updated_at, never the original version.
    plan = ChaosPolicy(seed=42).plan(ReplaySource(SAMPLE).records())
    original_updates = {
        event.event_id: event.updated_at_ms
        for _record, event in events_from_records(ReplaySource(SAMPLE).records())
    }
    assert plan.revisions > 0
    for event_id in plan.revised_ids:
        assert stored[event_id] == original_updates[event_id] + 60_000

    # Every injected malformed copy became exactly one dead letter at ingest.
    dead_letters = int(
        scalar("SELECT count() FROM seismic.dead_letters_latest WHERE stage = 'ingest_validation'")
        or 0
    )
    assert dead_letters == plan.malformed

    # Window aggregates equal the independent reference implementation.
    rows = query_rows(
        "SELECT region, toUnixTimestamp64Milli(window_start), event_count, max_magnitude, "
        "mean_magnitude, mean_depth_km, total_energy_joules "
        "FROM seismic.window_stats_latest"
    )
    actual_windows = {
        (str(region), int(start)): (
            int(count),
            float(max_magnitude),
            float(mean_magnitude),
            float(mean_depth_km),
            float(energy),
        )
        for region, start, count, max_magnitude, mean_magnitude, mean_depth_km, energy in rows
    }
    assert set(actual_windows) == set(reference.windows)
    for key, expected in reference.windows.items():
        actual = actual_windows[key]
        assert actual[0] == expected.event_count
        assert _approx(actual[1], expected.max_magnitude)
        assert _approx(actual[2], expected.mean_magnitude)
        assert _approx(actual[3], expected.mean_depth_km)
        assert math.isclose(actual[4], expected.total_energy_joules, rel_tol=1e-9, abs_tol=1e-9)

    # Alerts equal the policy applied to the final version of every event.
    alert_rows = query_rows("SELECT event_id, level, reason FROM seismic.alerts_latest")
    actual_alerts = {
        str(event_id): (str(level), str(reason)) for event_id, level, reason in alert_rows
    }
    expected_alerts = {
        event_id: (alert.level.value, alert.reason) for event_id, alert in reference.alerts.items()
    }
    assert actual_alerts == expected_alerts

    raw = int(scalar("SELECT count() FROM seismic.events") or 0)
    latest = events_count()
    assert raw >= latest
    logging.getLogger(__name__).warning(
        "raw events rows: %d, events_latest rows: %d, delta: %d", raw, latest, raw - latest
    )
