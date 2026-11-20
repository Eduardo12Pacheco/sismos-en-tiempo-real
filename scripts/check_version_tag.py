#!/usr/bin/env python3
"""Verify that a release tag matches the project version (S23).

The release workflow runs this before building artifacts, so a tag such as
``v0.2.0`` can never publish a wheel built from a different version. The tag
argument defaults to ``GITHUB_REF_NAME`` when omitted.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TAG_RE = re.compile(r"^v(?P<version>\d+\.\d+\.\d+)$")


def read_version(pyproject: Path) -> str:
    """Return the ``[project].version`` declared in ``pyproject``."""
    with pyproject.open("rb") as handle:
        data = tomllib.load(handle)
    return str(data["project"]["version"])


def tag_matches(tag: str, version: str) -> bool:
    """Return whether ``tag`` is exactly ``v<version>`` (no pre-release suffixes)."""
    match = TAG_RE.fullmatch(tag)
    return match is not None and match.group("version") == version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Check that a vX.Y.Z tag equals the pyproject version."
    )
    parser.add_argument(
        "tag",
        nargs="?",
        default=os.environ.get("GITHUB_REF_NAME", ""),
        help="tag to check (defaults to GITHUB_REF_NAME)",
    )
    parser.add_argument(
        "--pyproject",
        type=Path,
        default=REPO_ROOT / "pyproject.toml",
        help="path to pyproject.toml (defaults to the repository root)",
    )
    args = parser.parse_args(argv)

    version = read_version(args.pyproject)
    if not args.tag:
        print("error: no tag provided and GITHUB_REF_NAME is not set", file=sys.stderr)
        return 1
    if not tag_matches(args.tag, version):
        print(
            f"error: tag '{args.tag}' does not match pyproject version 'v{version}'",
            file=sys.stderr,
        )
        return 1

    print(f"release tag {args.tag} matches project version {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
