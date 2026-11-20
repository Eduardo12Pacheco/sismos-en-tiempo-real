"""CLI-level test for the bootstrap command's JSON contract."""

from __future__ import annotations

import json

from typer.testing import CliRunner

from seismic_stream import cli
from seismic_stream.adapters.kafka.admin import TopicAdminResult
from seismic_stream.adapters.kafka.bootstrap import (
    BootstrapResult,
    CompatibilityResult,
    SubjectResult,
)
from seismic_stream.adapters.kafka.topics import SUBJECTS, TOPIC_SPECS


def test_bootstrap_prints_json_summary(monkeypatch) -> None:
    result = BootstrapResult(
        topics=TopicAdminResult(created=tuple(spec.name for spec in TOPIC_SPECS), unchanged=()),
        subjects=tuple(SubjectResult(subject=s, action="created") for s in SUBJECTS),
        compatibility=tuple(
            CompatibilityResult(subject=s, level="BACKWARD", action="updated") for s in SUBJECTS
        ),
    )
    monkeypatch.setattr(cli, "run_bootstrap", lambda settings: result)

    run = CliRunner().invoke(cli.app, ["bootstrap"])

    assert run.exit_code == 0, run.output
    payload = json.loads(run.stdout)
    assert len(payload["topics"]["created"]) == 4
    assert payload["subjects"]["created"] == list(SUBJECTS)
