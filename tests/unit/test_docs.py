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
README_FILES = (ROOT / "README.md", ROOT / "README.en.md")
MERMAID_KEYWORDS = ("flowchart", "sequenceDiagram", "classDiagram", "stateDiagram-v2")
MERMAID_DOCS = (ARCHITECTURE, ROOT / "README.md", ROOT / "README.en.md")

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
    """Each ADR carries Estado, Fecha and the required sections (S24/D27, Spanish S39)."""
    text = adr.read_text(encoding="utf-8")
    status = re.search(r"^- \*\*Estado:\*\* \w+", text, re.MULTILINE)
    date = re.search(r"^- \*\*Fecha:\*\* \d{4}-\d{2}-\d{2}", text, re.MULTILINE)
    assert status, f"{adr.name}: no Estado"
    assert date, f"{adr.name}: no Fecha"
    for heading in (
        "## Contexto",
        "## Decisión",
        "## Consecuencias",
        "## Alternativas consideradas",
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


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_is_short_enough(path: Path) -> None:
    """Both READMEs stay within the 150-line budget (S24, S38)."""
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) <= 150, f"{path.name} has {len(lines)} lines (max 150)"


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


#: The generated BENCHMARKS.md is the only prose file allowed to carry benchmark numbers (S26).
UNCITED_CLAIM_CANDIDATES = [
    path for path in sorted(DOCS.rglob("*.md")) if path.name != "BENCHMARKS.md"
]


@pytest.mark.parametrize("path", UNCITED_CLAIM_CANDIDATES, ids=lambda path: path.name)
def test_no_uncited_benchmark_claims(path: Path) -> None:
    """Benchmark numbers live only in the generated BENCHMARKS.md (S26)."""
    text = path.read_text(encoding="utf-8")
    assert not BENCHMARK_TOKEN_RE.search(text), f"{path.name} claims a benchmark number"


# --- T14: README, Spanish mirror, generated benchmarks and changelog ---------

BENCHMARKS = DOCS / "BENCHMARKS.md"
CHANGELOG = ROOT / "CHANGELOG.md"
README_ES = ROOT / "README.md"
README_EN = ROOT / "README.en.md"
ASSETS = DOCS / "assets"
RESULTS_HEADINGS = {README_ES: "## Resultados", README_EN: "## Results"}
#: The limitation must name event-time windows; the bare phrase is the old wording.
EVENT_TIME_LIMITATIONS = {
    README_ES: (
        "Las ventanas de tiempo de evento son provisionales",
        "Las ventanas son provisionales",
    ),
    README_EN: ("Event-time windows are provisional", "Windows are provisional"),
}
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


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_results_table_is_backed_by_benchmarks(path: Path) -> None:
    """Every number in the README results table appears verbatim in BENCHMARKS.md (S26, S38)."""
    assert BENCHMARKS.is_file(), "docs/BENCHMARKS.md is generated by `make bench`"
    heading = RESULTS_HEADINGS[path]
    results = _section(path, heading)
    assert results.strip(), f"{path.name} has an empty {heading} section"
    report = BENCHMARKS.read_text(encoding="utf-8")
    numbers = NUMBER_RE.findall(results)
    assert numbers, "the README results table must publish measured numbers"
    missing = [number for number in numbers if number not in report]
    assert not missing, f"README numbers absent from docs/BENCHMARKS.md: {missing}"


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_limitation_names_event_time_windows(path: Path) -> None:
    """The limitation bullet names event-time windows, not the operating system (S3, S38)."""
    text = path.read_text(encoding="utf-8")
    expected, stale = EVENT_TIME_LIMITATIONS[path]
    assert stale not in text
    assert expected in text


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_links_the_real_assets(path: Path) -> None:
    """The hero and screenshot assets referenced by the README exist (S26, S38)."""
    text = path.read_text(encoding="utf-8")
    names = ("hero.gif", "seismic-live-dark.png", "seismic-live-light.png", "pipeline-health.png")
    for name in names:
        assert name in text, f"{path.name} does not show {name}"
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


# --- README redesign: project-native hero, alt text and badge budget (S40-S42) --

README_ASSETS = ASSETS / "readme"
HERO_ES = README_ASSETS / "hero.svg"
HERO_EN = README_ASSETS / "hero.en.svg"
HERO_FOR = {README_ES: HERO_ES, README_EN: HERO_EN}
STATUS_LABEL = {README_ES: "Estado:", README_EN: "Status:"}
IMG_TAG_RE = re.compile(r"<img\b[^>]*>")
ALT_ATTR_RE = re.compile(r'\balt="([^"]*)"')
MARKDOWN_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(")
#: Features GitHub strips or that break on other renderers; the hero must use none of them.
FRAGILE_SVG_RE = re.compile(
    r"<script|foreignObject|<style|@import|@font-face|@keyframes|<animate|<set\b"
    r"|https?://(?!www\.w3\.org/)",
    re.IGNORECASE,
)


@pytest.mark.parametrize("hero", [HERO_ES, HERO_EN], ids=lambda path: path.name)
def test_hero_svg_is_static_and_self_contained(hero: Path) -> None:
    """Pure SVG hero with its own background and no fragile features (S40)."""
    text = hero.read_text(encoding="utf-8")
    assert text.lstrip().startswith("<svg"), f"{hero.name} is not an SVG document"
    assert 'viewBox="0 0 1200 ' in text, f"{hero.name} must use a 1200-unit viewBox"
    assert "<title" in text, f"{hero.name} needs a <title>"
    assert "<desc" in text, f"{hero.name} needs a <desc>"
    assert re.search(r'<rect width="1200" height="\d+"', text), (
        f"{hero.name} needs its own background"
    )
    assert not FRAGILE_SVG_RE.search(text), f"{hero.name} uses a feature GitHub may strip"


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_shows_the_hero_in_its_own_language(path: Path) -> None:
    """Each README embeds its matching hero: Spanish hero.svg, English hero.en.svg (S40, S42)."""
    text = path.read_text(encoding="utf-8")
    hero = HERO_FOR[path]
    other = HERO_EN if hero == HERO_ES else HERO_ES
    assert hero.is_file(), f"docs/assets/readme/{hero.name} is missing"
    assert f"docs/assets/readme/{hero.name}" in text, f"{path.name} does not embed {hero.name}"
    assert other.name not in text, f"{path.name} embeds the other language's hero"


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_gif_proof_comes_right_after_the_hero(path: Path) -> None:
    """The GIF proof sits before the first section, right after the hero (S41)."""
    text = path.read_text(encoding="utf-8")
    assert "hero.gif" in text[: text.index("\n## ")], f"{path.name} shows no proof before sections"


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_every_readme_image_has_alt_text(path: Path) -> None:
    """Every image carries alt text for terminals, registries and screen readers (S40, S41)."""
    text = path.read_text(encoding="utf-8")
    tags = IMG_TAG_RE.findall(text)
    assert tags, f"{path.name} has no HTML images to check"
    for tag in tags:
        alt = ALT_ATTR_RE.search(tag)
        assert alt is not None, f"{path.name} has an image without alt text: {tag}"
        assert alt.group(1).strip(), f"{path.name} has an image without alt text: {tag}"
    markdown_alts = MARKDOWN_IMAGE_RE.findall(text)
    assert markdown_alts, f"{path.name} lost its Markdown image proof"
    assert all(alt.strip() for alt in markdown_alts), f"{path.name} has empty Markdown alt text"


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_badges_stay_within_the_four_badge_budget(path: Path) -> None:
    """At most four assessment badges; the row never grows into a badge wall (S41)."""
    assert len(re.findall(r"img\.shields\.io", path.read_text(encoding="utf-8"))) <= 4


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_has_no_hand_written_table_of_contents(path: Path) -> None:
    """The host builds the outline from headings; an anchor list would go stale (S41)."""
    assert "](#" not in path.read_text(encoding="utf-8"), f"{path.name} has an anchor list"


@pytest.mark.parametrize("path", README_FILES, ids=lambda path: path.name)
def test_readme_states_its_project_status(path: Path) -> None:
    """The project status is visible without opening the commit history (S41, A4)."""
    label = STATUS_LABEL[path]
    assert label in path.read_text(encoding="utf-8"), f"{path.name} has no {label!r} status line"
