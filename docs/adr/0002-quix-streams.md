# ADR 0002: Quix Streams as the stateful stream processor

- **Status:** Accepted
- **Date:** 2026-11-20

## Context

The processor must demonstrate production-shaped stream processing: stateful
exact-duplicate detection keyed by `event_id`, event-time tumbling windows with a
grace period, a configurable alert branch, and records that can be compared
exactly against an independent reference implementation. It runs locally inside
the Compose stack on CPU only, with a 4 GB Docker memory budget, Python 3.12 and
no cloud account.

Candidate libraries were evaluated on 2026-11-19. Evidence:

| Candidate | Release evidence | Windowing and state | Schema Registry / Avro | Verdict |
|---|---|---|---|---|
| **Quix Streams 3.28.1** | PyPI upload 2026-10-15; Apache-2.0; Python `>=3.11,<4`; releases every 2-4 weeks (3.25.0 Jul 24, 3.26.0 Sep 14, 3.27.0 Sep 30, 3.28.0/3.28.1 Oct 15); repository active (pushed 2026-11-18) | Built-in tumbling/hopping/sliding/session windows; RocksDB state with changelog topics; stateful `apply(stateful=True)` | Bundled `AvroSerializer`/`AvroDeserializer` and `SchemaRegistryClientConfig` | **Selected** |
| Bytewax 0.21.1 | Uploaded 2024-11-25; no release for roughly 12 months at check time | Python dataflow windows, state via recovery | No built-in subject/version management | Rejected as stale |
| faust-streaming 0.15.4 | Uploaded 2026-10-28; maintained community fork | asyncio-object model, weaker declarative window API | No built-in Schema Registry/Avro adapters | Rejected: more glue code, less proven |
| Apache Flink | Mature, widely used | Rich event-time windows and state | Needs external Avro/registry integration | Rejected: JVM and cluster-sized footprint for a laptop demo |

The chosen library pins `confluent-kafka<2.13`, `pydantic<2.14` and
`pydantic-settings<2.16`; the lockfile respects those pins instead of forcing
newer versions. Linux wheels and Windows-native wheels for `confluent-kafka`
2.16.2 and `rocksdict` 0.3.29 (cp312) are published, so the unit-test matrix can
install natively; the runtime stack itself runs in Linux containers.

## Decision

1. Use **Quix Streams 3.28.1** (`quixstreams[avro]`) for the processor only.
   Ingest and the sink remain on plain `confluent-kafka`, as D9 requires.
2. Build the `Application` with consumer group `seismic-processor`,
   `auto_offset_reset="earliest"`, `processing_guarantee="at-least-once"`, a
   configurable `state_dir` (the `processor-state` volume in Compose), and
   `timestamp_extractor` returning `event_time_ms` (D12).
3. Deduplicate with a stateful `apply` keyed by the Kafka event key. The state
   stores `updated_at_ms` per `event_id`; `Deduplicator.decide` returns NEW for an
   unseen id, REVISION for a strictly larger `updated_at_ms`, and DUPLICATE
   otherwise. Duplicates are filtered out; revisions flow to alerts but never to
   windows.
4. Aggregate NEW events in tumbling windows (`SEISMIC_WINDOW_MINUTES`, default
   60; grace `SEISMIC_WINDOW_GRACE_MINUTES`, default 10) grouped by `region`,
   using `Count`, `Max`, `Mean` and `Sum` over the Gutenberg-Richter energy, and
   emit every update with `.current()` so the last message per
   `(region, window_start_ms)` is the final aggregate.
5. Serialize `window_stats` and `alert` with Quix Avro serializers configured
   with `auto_register_schemas=False` and `use_latest_version=True`, so only
   `bootstrap` ever registers contracts (D4).
6. Convert only `KeyError`/`ValueError` processing failures into skipped rows;
   undecodable bytes are logged, counted as
   `seismic_processor_skipped_total{reason="undecodable"}` and skipped, so poison
   bytes cannot stop the pipeline.
7. Derive window-emission timestamps from the window end, never from the wall
   clock, keeping aggregates reproducible at any replay speed.

## Consequences

- Window results are deterministic and can be compared field-by-field with
  `tests/support/reference.py`, which walks the raw USGS features with plain
  Python and does not import Quix Streams.
- Stateful dedup keeps one `updated_at_ms` per seen event id. The state is
  unbounded in theory, which is documented as a limitation for v0.1.0.
- `.current()` emits one message per event and open window. Downstream readers
  must use the last version per key; the ClickHouse sink's `ReplacingMergeTree`
  provides that semantics in T9.
- The library adds `rocksdict` and a state changelog topic to the stack, which is
  acceptable within the memory budget and gives crash recovery for free.
- Quix's API is wrapped behind `adapters/stream/topology.py`; replacing it would
  only require a new adapter, because dedup, energy and alert semantics live in
  `application/` and `domain/`.
- Using Quix for the processor while the sink stays a custom loop means two
  Kafka client stacks, but each is used where it is strongest (D9).

## Alternatives considered

- **Quix Streams for ingest and the sink too.** Rejected: storage must remain an
  `EventStore` port with explicit pause/resume, retries and batch bisection, which
  is application policy (D9/D10).
- **Bytewax.** Rejected: no release for roughly a year at evaluation time and no
  built-in Schema Registry integration.
- **faust-streaming.** Rejected: maintained, but its asyncio-object API provides
  no built-in Avro/Schema Registry support and a weaker declarative window API.
- **Apache Flink.** Rejected: JVM plus cluster footprint is disproportionate for
  a single-node local demo.
- **Hand-written windows over raw `confluent-kafka`.** Rejected: it would
  reimplement state stores, changelogs and event-time closure, adding risk
  without demonstrating a different capability.
