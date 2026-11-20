"""Release tag guard used by the tag-triggered publishing workflow (S23)."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from scripts import check_version_tag

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_version_tag_check_accepts_matching_tag() -> None:
    assert check_version_tag.read_version(REPO_ROOT / "pyproject.toml") == "0.1.0"
    assert check_version_tag.tag_matches("v0.1.0", "0.1.0") is True
    assert check_version_tag.tag_matches("v0.1.0-rc1", "0.1.0") is False
    assert check_version_tag.tag_matches("0.1.0", "0.1.0") is False


@pytest.mark.parametrize(
    ("tag", "expected"),
    [("v0.1.0", 0), ("v9.9.9", 1), ("0.1.0", 1)],
)
def test_check_version_tag_cli_exit_codes(tag: str, expected: int) -> None:
    result = subprocess.run(
        [sys.executable, str(REPO_ROOT / "scripts" / "check_version_tag.py"), tag],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )

    assert result.returncode == expected, result.stdout + result.stderr
