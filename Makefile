.PHONY: help sync lint fmt types arch test test-int test-e2e cov audit ci up down replay demo-chaos dashboards screenshots bench clean

PYTEST ?= uv run pytest

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

test: ## Run unit, property and architecture tests
	$(PYTEST) -m "not integration and not e2e and not ui"

test-int: ## Run integration tests (Docker required)
	$(PYTEST) -m integration

test-e2e: ## Run end-to-end tests (Docker required)
	$(PYTEST) -m e2e

cov: ## Run tests with coverage
	$(PYTEST) --cov=src/seismic_stream --cov-report=term-missing

audit: ## Audit dependencies for known vulnerabilities
	uv run pip-audit

ci: ## Local equivalent of the CI lint and unit jobs
	$(MAKE) lint
	$(MAKE) types
	$(MAKE) arch
	$(MAKE) test
	$(MAKE) audit

up: ## Start the full stack
	@echo "not available yet"

down: ## Stop the stack but keep volumes
	docker compose down --remove-orphans

replay: ## Run a replay against the running stack
	@echo "not available yet"

demo-chaos: ## Run a chaotic replay
	@echo "not available yet"

dashboards: ## Regenerate Grafana dashboards
	@echo "not available yet"

screenshots: ## Capture dashboard screenshots and hero GIF
	@echo "not available yet"

bench: ## Produce docs/BENCHMARKS.md
	@echo "not available yet"

clean: ## Remove local caches and build artifacts
	rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage coverage.xml dist build htmlcov
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
