.PHONY: help sync lint fmt types arch lock shellcheck compose-config dashboards actionlint check install up down replay demo-chaos test test-int test-e2e test-ui cov audit ci screenshots bench clean

PYTEST ?= uv run pytest
COMPOSE ?= docker compose
COV_ARGS ?= --cov=src/seismic_stream --cov-report=term-missing --cov-fail-under=85

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*##"; printf "Usage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  %-16s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

sync: ## Install the locked development environment
	uv sync --frozen

lint: ## Run ruff lint and format checks
	uv run ruff check .
	uv run ruff format --check .

fmt: ## Auto-fix lint issues and format code
	uv run ruff check --fix .
	uv run ruff format .

types: ## Type-check src/ with mypy strict
	uv run mypy --strict src

arch: ## Check hexagonal import contracts
	uv run lint-imports

lock: ## Verify that uv.lock matches pyproject.toml
	uv lock --check

shellcheck: ## Lint install.sh with ShellCheck
	shellcheck install.sh

compose-config: ## Validate every supported Compose override combination
	$(COMPOSE) config -q
	$(COMPOSE) -f compose.yaml -f compose.test.yaml config -q
	$(COMPOSE) -f compose.yaml -f compose.live.yaml config -q
	$(COMPOSE) -f compose.yaml -f compose.test.yaml -f compose.live.yaml config -q

dashboards: ## Regenerate Grafana dashboards
	uv run python scripts/build_dashboards.py

actionlint: ## Lint the GitHub Actions workflows
	docker run --rm -v "$(CURDIR):/repo" -w /repo rhysd/actionlint:latest

check: ## Check installer prerequisites only
	./install.sh --check

install: ## Build the stack and start the replay
	./install.sh

up: install ## Start the full stack (alias of install)

down: ## Stop the stack but keep volumes
	./install.sh --down

replay: ## Start another background replay on the running stack
	$(COMPOSE) up -d --force-recreate ingest

demo-chaos: ## Run a chaotic replay on the running stack
	SEISMIC_REPLAY_CHAOS=true $(COMPOSE) up -d --force-recreate ingest

test: ## Run unit, property and architecture tests
	$(PYTEST) -m "not integration and not e2e and not ui"

test-int: ## Run integration tests (Docker required)
	$(PYTEST) -m integration

test-e2e: ## Run end-to-end tests (Docker required)
	$(PYTEST) -m e2e

test-ui: ## Run Playwright dashboard tests (Docker required)
	$(PYTEST) -m ui

cov: ## Run every test with the 85% coverage gate (Docker required)
	$(PYTEST) $(COV_ARGS)

audit: ## Audit dependencies for known vulnerabilities
	uv export --frozen --no-emit-project --no-hashes -o req.txt
	@uvx pip-audit -r req.txt --no-deps --disable-pip || { rm -f req.txt; exit 1; }
	@rm -f req.txt

ci: ## Local equivalent of every CI job (Docker and ShellCheck required)
	$(MAKE) lint
	$(MAKE) types
	$(MAKE) arch
	$(MAKE) lock
	$(MAKE) shellcheck
	$(MAKE) compose-config
	$(MAKE) dashboards
	$(MAKE) actionlint
	$(MAKE) cov
	$(MAKE) audit

screenshots: ## Capture dashboard screenshots and hero GIF
	@test -f scripts/capture_screenshots.py || { echo "screenshots are implemented in T14"; exit 1; }
	uv run python scripts/capture_screenshots.py

bench: ## Produce docs/BENCHMARKS.md
	@test -f scripts/bench.py || { echo "benchmarks are implemented in T14"; exit 1; }
	uv run python scripts/bench.py

clean: ## Remove local caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml dist build htmlcov req.txt
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
