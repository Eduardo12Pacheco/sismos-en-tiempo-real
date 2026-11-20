# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-20

### Bug Fixes

- **kafka:** Configure compatibility when a subject has no config yet
- **config:** Use the portfolio Schema Registry host port 13081
- **sink:** Retry transient errors during bisection
- **kafka:** End batch polling when no message is pending
- **compose:** Build the shared app image once from bootstrap

### Build

- **deps:** Add confluent-kafka Avro stack and fastavro
- Add the application image and Redpanda Compose services
- **deps:** Add Quix Streams for stream processing
- **deps:** Add ClickHouse client and schema migrations
- **grafana:** Provision the ClickHouse plugin and dashboards
- **make:** Run every local CI gate through make ci

### CI

- Run lint, cross-platform unit, e2e, installer and security jobs
- **release:** Publish tag-gated releases and run CodeQL
- Trust PSGallery for PSScriptAnalyzer and run security independently

### Chores

- Scaffold project with tooling and CI skeleton
- **observability:** Quiet HTTP client request logs at INFO
- Silence authlib import warning and document mypy override

### Documentation

- Document data lineage and replay design
- Record Redpanda and Avro schema decisions
- **adr:** Document the Quix Streams decision
- **adr:** Document storage, delivery and DLQ decisions
- **adr:** Record dashboard-as-code and offline Geomap decisions
- Add architecture guide with patterns and contracts
- **operations:** Add runbook for settings, metrics and drills

### Features

- **domain:** Add models, geography, energy, alerts and versioning
- **domain:** Normalize USGS features into validated events
- **application:** Add replay ports, pacer and system clock
- **sources:** Add replay source and deterministic chaos decorator
- **data:** Bundle pinned USGS sample with checksum tooling
- **cli:** Add replay-info command
- **sources:** Add optional USGS live summary feed adapter
- **config:** Add typed settings and CLI config error handling
- **observability:** Define metric catalog and Prometheus adapter
- **observability:** Add structured logging and health server
- **kafka:** Add versioned Avro contracts for the four topics
- **kafka:** Add Avro codecs and offline contract tests
- **kafka:** Add idempotent topic and schema registry administration
- **cli:** Add bootstrap command for topics and Avro contracts
- **ingest:** Add ingest use case with in-memory publishers
- **kafka:** Add resilient producer and topic publishers
- **cli:** Wire the ingest command and Compose service
- **processing:** Add dedup and window shaping helpers
- **stream:** Add Quix dedup, window and alert topology
- **cli:** Wire the process command and processor service
- **storage:** Add idempotent ClickHouse event store
- **sink:** Add retry, bisection and batch sink use case
- **cli:** Wire the sink command and Compose service
- **sink:** Report unready after sustained store failures
- **monitoring:** Add consumer lag monitor use case
- **monitoring:** Read Kafka offsets and expose the monitor command
- **cli:** Add doctor and verify diagnostics
- **observability:** Scope metrics to their owning service
- **compose:** Run Prometheus and the lag monitor in the stack
- **grafana:** Generate the provisioned seismic dashboards
- **install:** Add idempotent bash installer with prerequisite checks
- **install:** Add PowerShell installer for Docker Desktop

### Tests

- **domain:** Cover every ACL rule and domain invariant
- **sources:** Cover live polling, revisions, retries and validation
- **integration:** Verify bootstrap against a local Redpanda
- Rename bootstrap unit test to avoid a module-name collision
- **ingest:** Cover validation, filtering, limits and shutdown flushing
- **kafka:** Cover backpressure retries and delivery failures
- **cli:** Verify ingest options, JSON summary and exit codes
- **integration:** Verify ingest events and DLQ against Redpanda
- **cli:** Normalize colorized output in usage-error assertions
- **processor:** Verify clean aggregates and alerts against the reference
- **processor:** Cover chaos revisions and poison bytes
- **integration:** Verify ClickHouse idempotency and sink delivery
- **e2e:** Add the full-stack fixture and drivers
- **e2e:** Prove offline isolation and per-service metrics
- **e2e:** Prove chaos delivery and poison isolation
- **e2e:** Prove sink backpressure and outage recovery
- **doctor:** Cover diagnostics, monitor and verify paths
- **e2e:** Validate dashboards through Grafana and Playwright
- **e2e:** Pace dashboard replay so throughput panels see data
- **docs:** Enforce drift checks and finalize data, ADR and contributor docs

