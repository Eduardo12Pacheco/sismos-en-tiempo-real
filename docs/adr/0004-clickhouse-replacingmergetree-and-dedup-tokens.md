# ADR 0004: ClickHouse ReplacingMergeTree with insert deduplication tokens

- **Status:** Accepted
- **Date:** 2026-11-20
- **Related:** [Architecture](../ARCHITECTURE.md) · [ADR 0005](0005-delivery-semantics-effectively-once.md) · [ADR 0006](0006-dead-letter-channel-and-backpressure.md)

## Context

The sink stores four append-oriented streams: earthquake events, window
aggregates, alerts and dead letters. Kafka delivery is at least once, replaying
the bundled sample is a first-class demo action, and revisions of the same USGS
event arrive with a newer `updated_at_ms`. The repository must therefore accept
the same logical row more than once and still expose one current row per identity.

The storage must be local, columnar, shared by several containers, and queryable
from Grafana while the stack runs. It must also work on Linux, macOS and Windows
through Docker Desktop, without a cloud account.

## Decision

1. Use `clickhouse/clickhouse-server:26.8` (the LTS line) as the only database and
   `clickhouse-connect` over HTTP 8123 from the sink. Rejected DuckDB (one writer,
   no live shared service) and PostgreSQL (row store, weaker columnar scans).
2. Store events and alerts in `ReplacingMergeTree(<version>)` tables keyed by
   `event_id`, with `updated_at` as the version. Store dead letters keyed by
   `dlq_id` with `failed_at` as the version. Store aggregates keyed by
   `(region, window_start)` with `event_count` as the version, so the newest
   aggregate update wins.
3. Do **not** partition the tables. ClickHouse replaces rows only inside one
   partition, and a revision can change `event_time`; a single partition keeps the
   identity and version semantics correct for this data volume.
4. Read current state only through `*_latest` views that use `FINAL`:
   `events_latest`, `window_stats_latest`, `alerts_latest`,
   `dead_letters_latest`. A `data_clock` view exposes the newest stored
   `event_time`, the wall clock and their shift for the dashboards.
5. Give every insert an explicit `insert_deduplication_token` of the form
   `"<topic>:<partition>:<first_offset>-<last_offset>"` and set
   `non_replicated_deduplication_window = 1000` on every table. A retried Kafka
   batch therefore inserts zero extra raw rows, even before background merges.
   Bisected sub-batches append `:<lo>-<hi>` row-index ranges so their writes stay
   distinct from the parent attempt.
6. Ship the schema as six bundled `.sql` files applied in order by
   `ClickHouseMigrator`, recorded in `seismic.schema_migrations`. Every statement
   is `IF NOT EXISTS`, so `bootstrap` is idempotent and a crash mid-migration can
   be retried safely.
7. Treat the raw table as an append log and the `*_latest` view as the logical
   table. Replaying the same sample twice keeps `events_latest` stable while the
   raw `events` row count may grow; this difference is reported, never asserted
   to be zero.

## Consequences

- `events_latest` exposes exactly one row per event id, with the newest revision
  winning even when an older revision arrives later. Replaying twice leaves the
  logical count unchanged.
- Identical batches are deduplicated at insert time, so retries after a crash
  between ClickHouse and the Kafka commit do not multiply rows.
- Background merges do the physical replacement; readers pay the `FINAL` cost.
  At the sample's scale this is negligible and keeps the demo honest.
- A revision that changes event time remains in the same partition, so version
  resolution is not lost.
- The migration ledger records versions only after all statements of a file
  succeed. Adding a column or index later requires a new numbered file, never an
  edit to an applied one.
- The adapter translates every driver exception into the D10 transient/permanent
  taxonomy before it reaches application code; the sink never imports
  `clickhouse_connect`.

## Alternatives considered

- **DuckDB with a file on a shared volume.** Rejected: single-writer locking,
  no live HTTP service for Grafana, and no insert-deduplication window.
- **PostgreSQL.** Rejected: not columnar, weaker analytical scans over the
  aggregates and less interesting for a data-engineering demonstration.
- **`ReplacingMergeTree` with `PARTITION BY toYYYYMM(event_time)`.** Rejected:
  revisions that change the month would bypass replacement; the data volume does
  not justify partitions.
- **Application-level "select then insert/update".** Rejected: racy, slower and
  unnecessary when the engine already versions rows and deduplicates batches.
