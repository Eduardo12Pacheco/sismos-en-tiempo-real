#!/usr/bin/env python3
"""Capture the README screenshots and hero GIF from the real Grafana stack.

The script starts a clean Compose stack, runs the bundled replay at ``3600x``
(about three minutes of wall time for the recorded week) and drives Chromium
through Playwright against the provisioned dashboards. It captures the two
themes at 1440x900, the Pipeline Health dashboard from a clean replay and once
more from a chaotic replay so the dead-letter panel shows activity, and builds
the 1280x720 hero GIF with Pillow.

Everything is real: no mock-ups, no hand-edited images. The output files are
committed under ``docs/assets`` and referenced by the READMEs.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Final

REPO_ROOT: Final = Path(__file__).resolve().parents[1]
OUTPUT_DIR: Final = REPO_ROOT / "docs" / "assets"
GRAFANA_URL: Final = "http://127.0.0.1:13000"
COMPOSE: Final = ("docker", "compose", "-f", "compose.yaml")
STACK_SERVICES: Final = (
    "redpanda",
    "clickhouse",
    "prometheus",
    "grafana",
    "processor",
    "sink",
    "monitor",
)
SPEED: Final = "3600"
GIF_VIEWPORT: Final = (1280, 720)
PNG_VIEWPORT: Final = (1440, 900)
FRAME_INTERVAL_SECONDS: Final = 2.0
FRAME_PROGRESS_START: Final = 0.25
FRAME_PROGRESS_END: Final = 0.70
CHAOS_DLQ_MINIMUM: Final = 5.0
CHAOS_TIMEOUT_SECONDS: Final = 180
PROGRESS_TIMEOUT_SECONDS: Final = 420
MAX_GIF_BYTES: Final = 4 * 1024 * 1024


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


def parse_metric(text: str, name: str) -> float | None:
    """Return the first value of an exposition line named ``name``."""
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith(f"{name}{{") or stripped.startswith(f"{name} "):
            try:
                return float(stripped.rsplit(" ", 1)[-1])
            except ValueError:
                continue
    return None


def _container_metrics(container_id: str, port: int) -> str:
    code = (
        "import urllib.request;"
        f"print(urllib.request.urlopen('http://localhost:{port}/metrics', timeout=5)"
        ".read().decode())"
    )
    result = subprocess.run(
        ["docker", "exec", container_id, "python", "-c", code],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"metrics probe failed: {result.stderr}")
    return result.stdout


def _replay_progress(container_id: str) -> float | None:
    try:
        return parse_metric(_container_metrics(container_id, 9101), "seismic_replay_progress_ratio")
    except RuntimeError:
        # The detached replay container may still be starting; the caller retries.
        return None


def _dlq_count(container_id: str) -> float:
    try:
        value = parse_metric(_container_metrics(container_id, 9101), "seismic_dlq_messages_total")
    except RuntimeError:
        return 0.0
    return 0.0 if value is None else value


def wait_for_progress(
    container_id: str,
    threshold: float,
    *,
    timeout: float = PROGRESS_TIMEOUT_SECONDS,
) -> float:
    """Poll the ingest progress gauge until ``threshold`` is reached."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        progress = _replay_progress(container_id)
        if progress is not None and progress >= threshold:
            return progress
        time.sleep(1.0)
    raise RuntimeError(f"replay did not reach {threshold:.0%} progress within {timeout}s")


def _start_replay(*args: str) -> str:
    result = compose("run", "--rm", "-d", "--no-deps", "ingest", *args)
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("docker compose run did not print a container id")
    return lines[-1]


def _new_page(browser: object, viewport: tuple[int, int]):
    page = browser.new_page(viewport={"width": viewport[0], "height": viewport[1]})
    page.set_default_timeout(60_000)
    return page


def _open_dashboard(page: object, uid: str, theme: str, minimum_panels: int) -> None:
    page.goto(
        f"{GRAFANA_URL}/d/{uid}?kiosk&theme={theme}",
        wait_until="networkidle",
        timeout=60_000,
    )
    page.wait_for_function(
        "(count) => document.querySelectorAll('h2').length >= count",
        arg=minimum_panels,
        timeout=60_000,
    )
    page.wait_for_timeout(5_000)


def capture_frames(container_id: str, page: object, frame_dir: Path) -> list[Path]:
    """Screenshot the live dashboard every two seconds during the replay."""
    frames: list[Path] = []
    wait_for_progress(container_id, FRAME_PROGRESS_START)
    while True:
        index = len(frames)
        path = frame_dir / f"frame-{index:04d}.png"
        page.screenshot(path=str(path))
        frames.append(path)
        progress = _replay_progress(container_id)
        if progress is None or progress >= FRAME_PROGRESS_END:
            break
        time.sleep(FRAME_INTERVAL_SECONDS)
    print(f"captured {len(frames)} replay frames", flush=True)
    return frames


def build_gif(
    frames: Sequence[Path],
    output: Path,
    *,
    max_bytes: int = MAX_GIF_BYTES,
    duration_ms: int = int(FRAME_INTERVAL_SECONDS * 1000),
) -> int:
    """Assemble the hero GIF, shrinking the palette/frames to respect the budget."""
    if not frames:
        raise ValueError("at least one frame is required to build a GIF")

    from PIL import Image  # noqa: PLC0415 - optional ui dependency

    images = [Image.open(path).convert("RGB") for path in frames]
    try:
        for colors in (128, 64, 32):
            size = _save_gif(images, output, colors, duration_ms)
            if size <= max_bytes:
                return size
        while len(images) > 4:
            images = images[::2]
            size = _save_gif(images, output, 32, duration_ms * 2)
            if size <= max_bytes:
                return size
        raise RuntimeError(f"hero GIF is larger than {max_bytes} bytes even after downsampling")
    finally:
        for image in images:
            image.close()


def _save_gif(images: Sequence[object], output: Path, colors: int, duration_ms: int) -> int:
    from PIL import Image  # noqa: PLC0415 - optional ui dependency

    quantized = [image.quantize(colors=colors, method=Image.Quantize.MEDIANCUT) for image in images]
    quantized[0].save(
        output,
        save_all=True,
        append_images=quantized[1:],
        duration=duration_ms,
        loop=0,
        optimize=True,
        disposal=2,
    )
    for frame in quantized:
        frame.close()
    return output.stat().st_size


def _capture_clean(browser: object, workdir: Path) -> list[Path]:
    compose("down", "-v", "--remove-orphans", check=False)
    compose("up", "-d", "--wait", *STACK_SERVICES)
    container_id = _start_replay("--speed", SPEED)
    frame_dir = workdir / "frames"
    frame_dir.mkdir(parents=True, exist_ok=True)
    gif_page = _new_page(browser, GIF_VIEWPORT)
    try:
        _open_dashboard(gif_page, "seismic-live", "dark", 2)
        frames = capture_frames(container_id, gif_page, frame_dir)
    finally:
        gif_page.close()

    for theme in ("dark", "light"):
        page = _new_page(browser, PNG_VIEWPORT)
        try:
            _open_dashboard(page, "seismic-live", theme, 2)
            page.screenshot(path=str(OUTPUT_DIR / f"seismic-live-{theme}.png"))
            print(f"captured seismic-live ({theme}) from {GRAFANA_URL}", flush=True)
        finally:
            page.close()

    health = _new_page(browser, PNG_VIEWPORT)
    try:
        _open_dashboard(health, "pipeline-health", "dark", 3)
        health.screenshot(path=str(OUTPUT_DIR / "pipeline-health.png"))
        print(f"captured pipeline-health from {GRAFANA_URL}", flush=True)
    finally:
        health.close()
    return frames


def _capture_chaos(browser: object) -> None:
    compose("down", "-v", "--remove-orphans", check=False)
    compose("up", "-d", "--wait", *STACK_SERVICES)
    container_id = _start_replay("--speed", SPEED, "--chaos", "--seed", "42")
    # ChaoticSource has no __len__, so ingest cannot publish a progress ratio for
    # this mode; the dead-letter counter is the observable that proves the run is
    # producing the injected malformed records.
    deadline = time.monotonic() + CHAOS_TIMEOUT_SECONDS
    while time.monotonic() < deadline and _dlq_count(container_id) < CHAOS_DLQ_MINIMUM:
        time.sleep(2.0)
    if _dlq_count(container_id) < CHAOS_DLQ_MINIMUM:
        raise RuntimeError("the chaotic replay did not inject enough malformed records")
    time.sleep(8.0)
    page = _new_page(browser, PNG_VIEWPORT)
    try:
        _open_dashboard(page, "pipeline-health", "dark", 3)
        page.screenshot(path=str(OUTPUT_DIR / "pipeline-health-chaos.png"))
        print(f"captured pipeline-health (chaos) from {GRAFANA_URL}", flush=True)
    finally:
        page.close()


def main(argv: Sequence[str] | None = None) -> int:
    """Capture every committed asset and stop the stack afterwards."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-chaos", action="store_true", help="skip the chaos capture")
    args = parser.parse_args(argv)

    if shutil.which("docker") is None:
        print("error: Docker is required to capture screenshots", file=sys.stderr)
        return 1

    from playwright.sync_api import sync_playwright  # noqa: PLC0415 - optional ui dependency

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        compose("build")
        with tempfile.TemporaryDirectory(prefix="seismic-frames-") as scratch:
            workdir = Path(scratch)
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])
                try:
                    frames = _capture_clean(browser, workdir)
                    if not args.skip_chaos:
                        _capture_chaos(browser)
                finally:
                    browser.close()
            size = build_gif(frames, OUTPUT_DIR / "hero.gif")
            print(f"wrote docs/assets/hero.gif ({size} bytes)", flush=True)
    except (RuntimeError, OSError) as error:
        print(f"error: screenshot capture failed: {error}", file=sys.stderr)
        return 1
    finally:
        compose("down", "-v", "--remove-orphans", check=False)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
