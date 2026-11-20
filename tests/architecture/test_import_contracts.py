"""The hexagonal import contracts must hold from the first commit."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _lint_imports_command() -> list[str]:
    executable = shutil.which("lint-imports")
    if executable is not None:
        return [executable]
    return [sys.executable, "-m", "importlinter"]


def test_import_contracts_are_kept() -> None:
    result = subprocess.run(
        _lint_imports_command(),
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "Contracts: 3 kept, 0 broken." in output
