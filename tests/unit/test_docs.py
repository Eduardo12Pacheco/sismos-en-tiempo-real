"""Documentation drift tests (S24, T13).

The documentation is part of the product: these tests fail when code and prose
drift apart, so a new setting, metric or diagram cannot land undocumented.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from seismic_stream.adapters.observability.metrics import METRIC_CATALOG
from seismic_stream.config import ENV_PREFIX, Settings

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"
ARCHITECTURE = DOCS / "ARCHITECTURE.md"
OPERATIONS = DOCS / "OPERATIONS.md"
ENV_EXAMPLE = ROOT / ".env.example"
ADR_DIR = DOCS / "adr"
README_FILES = (ROOT / "README.md", ROOT / "README.es.md")
MERMAID_KEYWORDS = ("flowchart", "sequenceDiagram", "classDiagram", "stateDiagram-v2")
MERMAID_DOCS = (ARCHITECTURE, ROOT / "README.md")

#: Patterns named in decision D28; each must be documented with its module.
REQUIRED_PATTERNS = (
    "Ports and Adapters",
    "Composition Root",
    "Strategy",
    "Decorator",
    "Pipeline",
    "Dead Letter Channel",
    "Idempotent Receiver",
    "Bisection",
    "Repository",
    "Anti-corruption layer",
    "pause/resume",
)

LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
MERMAID_RE = re.compile(r"```mermaid\n(.*?)```", re.DOTALL)
BENCHMARK_TOKEN_RE = re.compile(r"events/s|ms p95")
FORBIDDEN_MARKERS_RE = re.compile(r"\b(?:TODO|TBD|FIXME)\b|lorem", re.IGNORECASE)


def _markdown_files() -> list[Path]:
    """Every prose file covered by the link test."""
    files = list(README_FILES) + sorted(DOCS.rglob("*.md"))
    return [path for path in files if path.is_file()]


def _mermaid_blocks(path: Path) -> list[str]:
    return [block.strip() for block in MERMAID_RE.findall(path.read_text(encoding="utf-8"))]


ENV_VARIABLES = [f"{ENV_PREFIX}{name.upper()}" for name in Settings.model_fields]


@pytest.mark.parametrize("variable", ENV_VARIABLES)
def test_every_setting_is_documented_and_exampled(variable: str) -> None:
    """Every typed setting appears in OPERATIONS.md and .env.example (S19/S24)."""
    operations = OPERATIONS.read_text(encoding="utf-8")
    example = ENV_EXAMPLE.read_text(encoding="utf-8")
    assert variable in operations, f"{variable} missing from docs/OPERATIONS.md"
    assert variable in example, f"{variable} missing from .env.example"


@pytest.mark.parametrize("spec", METRIC_CATALOG, ids=lambda spec: spec.name)
def test_every_metric_is_documented(spec: object) -> None:
    """Every catalog metric appears in the OPERATIONS.md metric table (S13/S24)."""
    name = spec.name  # type: ignore[attr-defined]
    assert name in OPERATIONS.read_text(encoding="utf-8"), f"{name} missing from OPERATIONS.md"


@pytest.mark.parametrize("adr", sorted(ADR_DIR.glob("*.md")), ids=lambda path: path.stem)
def test_adr_has_required_sections_and_metadata(adr: Path) -> None:
    """Each ADR carries Status, Date and the five required sections (S24/D27)."""
    text = adr.read_text(encoding="utf-8")
    status = re.search(r"^- \*\*Status:\*\* \w+", text, re.MULTILINE)
    date = re.search(r"^- \*\*Date:\*\* \d{4}-\d{2}-\d{2}", text, re.MULTILINE)
    assert status, f"{adr.name}: no Status"
    assert date, f"{adr.name}: no Date"
    for heading in (
        "## Context",
        "## Decision",
        "## Consequences",
        "## Alternatives considered",
    ):
        assert heading in text, f"{adr.name}: missing {heading}"


@pytest.mark.parametrize("path", _markdown_files(), ids=lambda path: str(path.relative_to(ROOT)))
def test_relative_links_resolve(path: Path) -> None:
    """Relative Markdown links point at files that exist (S24)."""
    text = path.read_text(encoding="utf-8")
    missing = []
    for target in LINK_RE.findall(text):
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        target_path = target.split("#", 1)[0]
        if not target_path:
            continue
        if not (path.parent / target_path).resolve().is_file():
            missing.append(target)
    assert not missing, f"{path.relative_to(ROOT)} has broken links: {missing}"


def test_readme_is_short_enough() -> None:
    """README.md stays within the 150-line budget (S24)."""
    lines = (ROOT / "README.md").read_text(encoding="utf-8").splitlines()
    assert len(lines) <= 150, f"README.md has {len(lines)} lines (max 150)"


@pytest.mark.parametrize("path", MERMAID_DOCS, ids=lambda path: path.name)
def test_mermaid_blocks_are_structurally_valid(path: Path) -> None:
    """Mermaid blocks use a known diagram keyword and balanced delimiters (S24)."""
    blocks = _mermaid_blocks(path)
    for block in blocks:
        first_line = next((line.strip() for line in block.splitlines() if line.strip()), "")
        assert first_line.split(" ", 1)[0] in MERMAID_KEYWORDS, (
            f"{path.name}: unknown Mermaid diagram keyword in {first_line!r}"
        )
        for opening, closing in (("(", ")"), ("[", "]"), ("{", "}")):
            assert block.count(opening) == block.count(closing), (
                f"{path.name}: unbalanced {opening}{closing} in Mermaid block"
            )


def test_architecture_names_and_links_every_pattern() -> None:
    """ARCHITECTURE.md documents every D28 pattern with a source link (S24)."""
    text = ARCHITECTURE.read_text(encoding="utf-8")
    for pattern in REQUIRED_PATTERNS:
        assert pattern in text, f"ARCHITECTURE.md does not name the {pattern!r} pattern"
    assert "../src/seismic_stream/" in text, "ARCHITECTURE.md must link implementing modules"
    assert "sequenceDiagram" in text, "ARCHITECTURE.md must include the outage sequence diagram"


@pytest.mark.parametrize("path", _markdown_files(), ids=lambda path: str(path.relative_to(ROOT)))
def test_docs_have_no_placeholder_markers(path: Path) -> None:
    """No TODO/TBD/FIXME/lorem placeholders survive into the docs (S24)."""
    matches = FORBIDDEN_MARKERS_RE.findall(path.read_text(encoding="utf-8"))
    assert not matches, f"{path.relative_to(ROOT)} contains placeholders: {matches}"


@pytest.mark.parametrize("path", sorted(DOCS.rglob("*.md")), ids=lambda path: path.name)
def test_no_uncited_benchmark_claims(path: Path) -> None:
    """Benchmark numbers live only in the generated BENCHMARKS.md (S26)."""
    if path.name == "BENCHMARKS.md":
        pytest.skip("generated in T14; the only file allowed to carry benchmark numbers")
    text = path.read_text(encoding="utf-8")
    assert not BENCHMARK_TOKEN_RE.search(text), f"{path.name} claims a benchmark number"


# --- T14: README, Spanish mirror, generated benchmarks and changelog ---------

BENCHMARKS = DOCS / "BENCHMARKS.md"
CHANGELOG = ROOT / "CHANGELOG.md"
README_EN = ROOT / "README.md"
README_ES = ROOT / "README.es.md"
ASSETS = DOCS / "assets"
RESULTS_HEADING = "## Results"
NUMBER_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")


def _heading_levels(path: Path) -> list[int]:
    levels = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = re.match(r"^(#{1,6}) ", line)
        if match:
            levels.append(len(match.group(1)))
    return levels


def _section(path: Path, heading: str) -> str:
    """Return the lines between ``heading`` and the next level-2 heading."""
    lines = path.read_text(encoding="utf-8").splitlines()
    try:
        start = lines.index(heading) + 1
    except ValueError as error:  # pragma: no cover - the assertion is the contract
        raise AssertionError(f"{path.name} is missing {heading}") from error
    section: list[str] = []
    for line in lines[start:]:
        if line.startswith("## "):
            break
        section.append(line)
    return "\n".join(section)


def test_spanish_readme_mirrors_the_english_structure() -> None:
    """Both READMEs share the same heading outline (S24)."""
    assert _heading_levels(README_EN) == _heading_levels(README_ES)


def test_readme_results_table_is_backed_by_benchmarks() -> None:
    """Every number in the README results table appears verbatim in BENCHMARKS.md (S26)."""
    assert BENCHMARKS.is_file(), "docs/BENCHMARKS.md is generated by `make bench`"
    results = _section(README_EN, RESULTS_HEADING)
    assert results.strip(), f"README.md has an empty {RESULTS_HEADING} section"
    report = BENCHMARKS.read_text(encoding="utf-8")
    numbers = NUMBER_RE.findall(results)
    assert numbers, "the README results table must publish measured numbers"
    missing = [number for number in numbers if number not in report]
    assert not missing, f"README numbers absent from docs/BENCHMARKS.md: {missing}"


def test_readme_limitation_names_event_time_windows() -> None:
    """The limitation bullet names event-time windows, not the operating system (S3)."""
    text = README_EN.read_text(encoding="utf-8")
    assert "Windows are provisional" not in text
    assert "Event-time windows are provisional" in text


def test_readme_links_the_real_assets() -> None:
    """The hero and screenshot assets referenced by the README exist (S26)."""
    text = README_EN.read_text(encoding="utf-8")
    names = ("hero.gif", "seismic-live-dark.png", "seismic-live-light.png", "pipeline-health.png")
    for name in names:
        assert name in text, f"README.md does not show {name}"
        assert (ASSETS / name).is_file(), f"docs/assets/{name} is missing"


def test_readmes_have_no_placeholder_comments() -> None:
    """The T1 placeholders are gone from both READMEs (S24)."""
    for path in README_FILES:
        text = path.read_text(encoding="utf-8").lower()
        assert "placeholder" not in text, f"{path.name} still has a placeholder"
        assert "marcador de posición" not in text, f"{path.name} still has a placeholder"


def test_changelog_has_the_v010_release() -> None:
    """CHANGELOG.md documents the 0.1.0 release generated from the commits (S25)."""
    text = CHANGELOG.read_text(encoding="utf-8")
    assert "## [0.1.0]" in text
    assert "### Features" in text
    assert "### Tests" in text
