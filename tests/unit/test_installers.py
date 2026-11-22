"""Cross-platform installer and release-tooling contracts (S21, S22, S23).

The bash installer is exercised directly with a stubbed ``docker`` executable,
so these tests never need a Docker daemon, a network connection or a real
Compose project. The PowerShell installer is validated statically on every OS
and fully exercised by the Windows CI job.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_SH = REPO_ROOT / "install.sh"
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

DOCKER_STUB = textwrap.dedent(
    """\
    #!/usr/bin/env bash
    set -u
    case "$*" in
      "--version")
        echo "Docker version 29.9.3, build __DOCKER_BUILD__"
        ;;
      "compose version --short"|"compose version")
        echo "__COMPOSE_VERSION__"
        ;;
      "info --format {{.OSType}}")
        echo "__DOCKER_OSTYPE__"
        ;;
      "ps --filter publish=13000 --format {{.Label \\"com.docker.compose.project\\"}}")
        printf '%s\\n' "__COMPOSE_PROJECT__"
        ;;
      *)
        echo "unexpected docker invocation: $*" >&2
        exit 64
        ;;
    esac
    """
)


def _write_docker_stub(
    directory: Path,
    *,
    compose_version: str = "v5.7.0",
    project: str = "",
    ostype: str = "linux",
) -> Path:
    stub = directory / "docker"
    stub.write_text(
        DOCKER_STUB.replace("__DOCKER_BUILD__", "stub")
        .replace("__COMPOSE_VERSION__", compose_version)
        .replace("__DOCKER_OSTYPE__", ostype)
        .replace("__COMPOSE_PROJECT__", project),
        encoding="utf-8",
    )
    stub.chmod(0o755)
    return stub


def _posix_bash() -> str | None:
    """Return a POSIX bash, preferring Git for Windows on Windows.

    On Windows runners ``bash`` on PATH can resolve to WSL's ``bash.exe``, which
    cannot see Windows paths such as ``D:\\a\\...\\install.sh``.
    """
    if os.name != "nt":
        return shutil.which("bash")
    git = shutil.which("git")
    if git is None:
        return None
    candidate = Path(git).parents[1] / "bin" / "bash.exe"
    return str(candidate) if candidate.is_file() else None


def _run_sh(
    *args: str,
    stub_dir: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    bash = _posix_bash() or "bash"
    env = dict(os.environ)
    env.pop("SEISMIC_REPLAY_SPEED", None)
    env.pop("SEISMIC_REPLAY_CHAOS", None)
    if os.name == "nt":
        # Stub scripts use "#!/usr/bin/env bash"; keep that lookup on the same bash.
        env["PATH"] = f"{Path(bash).parent}{os.pathsep}{env.get('PATH', '')}"
    if stub_dir is not None:
        env["PATH"] = f"{stub_dir}{os.pathsep}{env.get('PATH', '')}"
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [bash, str(INSTALL_SH), *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )


bash_required = pytest.mark.skipif(
    _posix_bash() is None, reason="bash is required to exercise install.sh"
)


def _json_checks(
    result: subprocess.CompletedProcess[str],
) -> tuple[dict[str, object], dict[str, str]]:
    payload = json.loads(result.stdout)
    assert set(payload) == {"checks", "ok"}
    assert isinstance(payload["ok"], bool)
    statuses = {item["id"]: item["status"] for item in payload["checks"]}
    assert set(statuses) == CHECK_IDS
    for item in payload["checks"]:
        assert set(item) == {"id", "status", "detail"}
        assert item["status"] in {"pass", "warn", "fail"}
        assert isinstance(item["detail"], str)
    return payload, statuses


@bash_required
def test_check_json_reports_every_check_and_is_ok(tmp_path: Path) -> None:
    _write_docker_stub(tmp_path)

    result = _run_sh("--check", "--json", stub_dir=tmp_path)

    assert result.returncode == 0, result.stderr
    payload, statuses = _json_checks(result)
    assert payload["ok"] is True
    assert statuses["shell"] == "pass"
    assert statuses["docker_cli"] == "pass"
    assert statuses["docker_compose"] == "pass"
    assert statuses["docker_engine"] == "pass"
    assert statuses["port_13000"] == "pass"
    assert result.stderr == ""


@bash_required
def test_check_human_output_never_prints_json(tmp_path: Path) -> None:
    _write_docker_stub(tmp_path)

    result = _run_sh("--check", stub_dir=tmp_path)

    assert result.returncode == 0, result.stderr
    assert "PASS" in result.stdout
    assert '"checks"' not in result.stdout
    assert not result.stdout.lstrip().startswith("{")
    with pytest.raises(json.JSONDecodeError):
        json.loads(result.stdout)


@bash_required
def test_check_fails_when_compose_is_too_old(tmp_path: Path) -> None:
    _write_docker_stub(tmp_path, compose_version="v2.19.4")

    result = _run_sh("--check", "--json", stub_dir=tmp_path)

    assert result.returncode == 1
    payload, statuses = _json_checks(result)
    assert payload["ok"] is False
    assert statuses["docker_compose"] == "fail"


@bash_required
def test_check_fails_when_docker_engine_is_not_linux(tmp_path: Path) -> None:
    _write_docker_stub(tmp_path, ostype="windows")

    result = _run_sh("--check", "--json", stub_dir=tmp_path)

    assert result.returncode == 1
    _, statuses = _json_checks(result)
    assert statuses["docker_engine"] == "fail"


@bash_required
def test_check_fails_when_docker_cli_is_broken(tmp_path: Path) -> None:
    broken = tmp_path / "docker"
    broken.write_text("#!/usr/bin/env bash\nexit 127\n", encoding="utf-8")
    broken.chmod(0o755)

    result = _run_sh("--check", "--json", stub_dir=tmp_path)

    assert result.returncode == 1
    _, statuses = _json_checks(result)
    assert statuses["docker_cli"] == "fail"


@bash_required
def test_check_accepts_port_owned_by_this_project(tmp_path: Path) -> None:
    _write_docker_stub(tmp_path, project="realtime-seismic-stream")

    result = _run_sh("--check", "--json", stub_dir=tmp_path)

    assert result.returncode == 0, result.stderr
    payload, statuses = _json_checks(result)
    assert payload["ok"] is True
    assert statuses["port_13000"] == "pass"
    detail = next(item["detail"] for item in payload["checks"] if item["id"] == "port_13000")
    assert "this project" in detail.lower()


@bash_required
def test_check_fails_when_port_is_owned_by_another_project(tmp_path: Path) -> None:
    _write_docker_stub(tmp_path, project="another-project")

    result = _run_sh("--check", "--json", stub_dir=tmp_path)

    assert result.returncode == 1
    _, statuses = _json_checks(result)
    assert statuses["port_13000"] == "fail"


@bash_required
def test_help_lists_every_documented_flag() -> None:
    result = _run_sh("--help")

    assert result.returncode == 0
    for flag in ("--check", "--json", "--replay", "--speed", "--chaos", "--down", "--purge"):
        assert flag in result.stdout
    assert "--yes" in result.stdout


@bash_required
@pytest.mark.parametrize("args", [("--bogus",), ("--speed", "0", "--replay"), ("--speed", "abc")])
def test_usage_errors_exit_2(args: tuple[str, ...]) -> None:
    result = _run_sh(*args)

    assert result.returncode == 2


@bash_required
def test_check_mode_does_not_create_env_file(tmp_path: Path) -> None:
    _write_docker_stub(tmp_path)
    env_file = REPO_ROOT / ".env"
    backup = None
    if env_file.exists():
        backup = env_file.read_bytes()
        env_file.unlink()
    try:
        result = _run_sh("--check", "--json", stub_dir=tmp_path)
        assert result.returncode == 0, result.stderr
        assert not env_file.exists()
    finally:
        if backup is not None:
            env_file.write_bytes(backup)
        elif env_file.exists():
            env_file.unlink()
