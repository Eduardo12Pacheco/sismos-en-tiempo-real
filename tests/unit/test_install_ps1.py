"""PowerShell installer contract (S21).

The script is fully exercised by the Windows CI job; this offline check keeps
the cross-platform flags, strict-mode setup and JSON output in sync.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_PS1 = REPO_ROOT / "install.ps1"
CHECK_IDS = {
    "shell",
    "docker_cli",
    "docker_compose",
    "docker_engine",
    "memory",
    "disk",
    "port_13000",
}


def test_powershell_installer_declares_the_same_contract() -> None:
    text = INSTALL_PS1.read_text(encoding="utf-8")
    for flag in ("Check", "Json", "Replay", "Speed", "Chaos", "Down", "Purge", "Yes"):
        assert f"[switch]${flag}" in text or f"[string]${flag}" in text
    assert "Set-StrictMode -Version Latest" in text
    assert "$ErrorActionPreference = 'Stop'" in text
    assert "Write-Host" not in text
    assert "ConvertTo-Json" in text
    for check_id in CHECK_IDS:
        assert check_id in text
