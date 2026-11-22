#!/usr/bin/env python3
"""Record the bundled USGS earthquake sample and verify its checksum.

The download uses only the Python standard library so the sample can be
re-recorded on any machine without extra dependencies. The query is pinned to
the documented USGS FDSN request; USGS may revise events over time, so a newly
recorded file is expected to have a different checksum (never byte-identical).
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import urllib.parse
import urllib.request
from pathlib import Path

FDSN_BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
QUERY_PARAMS: dict[str, str] = {
    "format": "geojson",
    "starttime": "2026-11-02T00:00:00",
    "endtime": "2026-11-09T00:00:00",
    "minmagnitude": "1.0",
    "orderby": "time-asc",
}
SAMPLE_NAME = "usgs_2026-11-02_2026-11-09_m1.geojson"
DEFAULT_OUTPUT = Path("data") / "samples" / SAMPLE_NAME
USER_AGENT = (
    "realtime-seismic-stream/0.1.0 (+https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real)"
)
CHUNK_SIZE = 1024 * 1024


def main(argv: list[str] | None = None) -> int:
    """Run the recorder or the offline checksum verification."""
    parser = argparse.ArgumentParser(
        description="Record the pinned USGS sample or verify an existing copy.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Destination file (default: {DEFAULT_OUTPUT}).",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Do not download; compare the file with its SHA256SUMS manifest.",
    )
    args = parser.parse_args(argv)

    if args.verify:
        return verify(args.output)
    return record(args.output)


def record(output: Path) -> int:
    """Download the pinned query and write ``output`` plus its checksum manifest."""
    url = f"{FDSN_BASE_URL}?{urllib.parse.urlencode(QUERY_PARAMS)}"
    request = urllib.request.Request(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "application/geo+json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            payload = response.read()
    except OSError as exc:
        print(f"error: cannot download {url}: {exc}", file=sys.stderr)
        return 1

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    _write_manifest(output, digest)

    print(f"wrote {output} ({len(payload)} bytes)")
    print(f"sha256: {digest}")
    print("re-recorded samples may differ from the committed checksum")
    return 0


def verify(output: Path) -> int:
    """Compare ``output`` with the checksum recorded in its SHA256SUMS manifest."""
    if not output.exists():
        print(f"error: {output} does not exist", file=sys.stderr)
        return 1

    manifest = output.parent / "SHA256SUMS"
    if not manifest.exists():
        print(f"error: {manifest} does not exist", file=sys.stderr)
        return 1

    expected = _read_expected(manifest, output.name)
    if expected is None:
        print(f"error: {output.name} is not listed in {manifest}", file=sys.stderr)
        return 1

    actual = _sha256(output)
    if actual != expected:
        print(f"MISMATCH {output.name}: expected {expected}, got {actual}")
        return 1

    print(f"{output.name}: OK ({actual})")
    return 0


def _write_manifest(output: Path, digest: str) -> None:
    manifest = output.parent / "SHA256SUMS"
    manifest.write_text(f"{digest}  {output.name}\n", encoding="utf-8")


def _read_expected(manifest: Path, filename: str) -> str | None:
    for line in manifest.read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        if Path(parts[1].lstrip("*")).name == filename:
            return parts[0].lower()
    return None


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    sys.exit(main())
