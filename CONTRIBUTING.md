# Contributing

Thanks for taking a look at `sismos-en-tiempo-real`. This guide covers the local
workflow, the quality gates and the conventions used in this repository.

## Requirements

- Python 3.12 (managed through [uv](https://docs.astral.sh/uv/); see `.python-version`)
- Docker with Compose v2 for the integration and end-to-end suites
- `make` is a convenience on Linux and macOS; Windows users run the underlying `uv`
  commands directly

## Setup

```bash
uv sync --frozen
uv run seismic-stream --version
```

## Commands

| Command | Purpose |
|---|---|
| `make lint` | Ruff lint plus formatting check |
| `make fmt` | Apply Ruff fixes and formatting |
| `make types` | `mypy --strict src` |
| `make arch` | Import-linter hexagonal contracts |
| `make test` | Unit, property and architecture tests (no Docker) |
| `make test-int` | Integration tests (Docker required) |
| `make test-e2e` | End-to-end tests (full stack) |
| `make test-ui` | Playwright dashboard tests (full stack) |
| `make cov` | Every test with the 85% coverage gate |
| `make check` | Installer prerequisite checks (`./install.sh --check`) |
| `make install` | Build the stack and start the replay |
| `make down` | Stop the stack and keep its volumes |
| `make shellcheck` | Lint `install.sh` |
| `make compose-config` | Validate every Compose override combination |
| `make actionlint` | Lint the GitHub Actions workflows |
| `make audit` | `pip-audit` dependency audit |
| `make ci` | Local equivalent of every CI job (Docker required) |

Tests never reach the network: `pytest-socket` blocks every host except
`127.0.0.1`/`::1`. Tests that need a service bind ephemeral ports (port `0`).

## Documentation

Documentation is part of the definition of done and is drift-tested by
`tests/unit/test_docs.py`:

| File | Audience and content |
|---|---|
| `README.md`, `README.en.md` | Public pitch, quickstart, limitations; `README.md` (Spanish, primary) and `README.en.md` (English) each stay at 150 lines or fewer |
| `docs/ARCHITECTURE.md` | Layers, the D28 patterns with their implementing module, data flow, schemas, delivery semantics and scaling path |
| `docs/DATA.md` | Sample provenance, query, checksum, re-recording, licence and credit |
| `docs/OPERATIONS.md` | Prerequisites, commands, every environment variable, metrics, ports and troubleshooting |
| `docs/adr/` | Numbered decisions with status, date and alternatives |

When you change behaviour:

- a new or changed `Settings` field must appear in `.env.example` and in the
  `docs/OPERATIONS.md` configuration table, named `SEISMIC_<FIELD>`;
- a new or changed `METRIC_CATALOG` entry must appear in the
  `docs/OPERATIONS.md` metric table;
- every ADR keeps the `Status`, `Date`, `Context`, `Decision`, `Consequences` and
  `Alternatives considered` sections;
- relative links must resolve, Mermaid blocks must start with a known diagram
  keyword and keep balanced delimiters, and no `TODO`/`TBD`/`FIXME`/`lorem`
  placeholder may survive;
- benchmark numbers belong only in the generated `docs/BENCHMARKS.md`.

Run the checks with:

```bash
uv run pytest tests/unit/test_docs.py -q
```

## Commit convention

Commits follow [Conventional Commits](https://www.conventionalcommits.org/):
`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `ci:`, `build:`, `refactor:`. Keep each
commit a coherent, reviewable unit (roughly 400 changed lines or fewer) and keep tests
and docs in the same commit as the behaviour they describe. No AI attribution lines.

## Architecture rules

The codebase is hexagonal: `domain` has no I/O and no third-party imports,
`application` depends only on `domain`, and `adapters` depend on both. The
`cli`/`composition` layer wires everything together. These rules are enforced by
`import-linter` and by `tests/architecture/test_import_contracts.py`; never weaken a
contract to make a change pass.

## Type checking overrides

`mypy --strict` runs over `src/`. Any module-level `ignore_missing_imports` override
must be listed here with the reason.

| Module | Reason |
|---|---|
| `confluent_kafka` (and submodules) | Ships no PEP 561 marker or stubs; typed wrappers live in `adapters/kafka`. |
| `authlib` (and submodules) | Transitive confluent-kafka extra with no stubs; only its warning class is imported to silence third-party noise. |

## Tests

Write the failing test first, then the implementation. Do not skip, delete or weaken
tests to obtain a green run. Property-based tests use Hypothesis profiles: `dev` is the
default and `ci` (`HYPOTHESIS_PROFILE=ci`) fixes `max_examples=100` and derandomizes.

## License

By contributing you agree that your contribution is licensed under the MIT License.
