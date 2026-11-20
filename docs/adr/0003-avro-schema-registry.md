# ADR 0003: Avro contracts and Schema Registry compatibility

- **Status:** Accepted
- **Date:** 2026-11-20
- **Related:** [Architecture](../ARCHITECTURE.md) · [ADR 0001](0001-redpanda.md) · [ADR 0005](0005-delivery-semantics-effectively-once.md)

## Context

Four Kafka topics carry data between independent services (`ingest`,
`processor`, `sink`). The payloads must be versioned, self-describing and
readable by services built at different times, without embedding schemas in the
messages or relying on hand-written JSON field names. The local broker already
provides a Confluent-compatible Schema Registry (ADR 0001), and the project has
zero-account, offline requirements: the contracts must live in the repository and
be testable without a running registry.

## Decision

1. Use Avro for all four value contracts: `earthquake_event.v1`,
   `window_stats.v1`, `alert.v1` and `dead_letter.v1`. Keys stay plain UTF-8
   strings (`event_id`, `<region>|<window_start_ms>`, `dlq_id`).
2. Store the schemas as `.avsc` files inside the installed package
   (`seismic_stream.adapters.kafka.schemas`) and load them with
   `importlib.resources`, so the wheel and the tests read identical bytes.
3. Name subjects with the `TopicNameStrategy` convention: `<topic>-value`.
4. Enforce `BACKWARD` compatibility on every subject. Consumers are upgraded
   before producers; a new field must therefore be optional with a default.
5. Register schemas only from `seismic-stream bootstrap`. Service codecs use
   `auto.register.schemas=false` and `use.latest.version=true`, so a service can
   never silently create a new version.
6. Do not use Avro logical types: milliseconds stay `long` fields named `*_ms`,
   which keeps decoders returning integers instead of timezone-aware objects.
7. Prove evolution offline with fastavro (v2 compatible schema reads v1 data; a
   required field without default raises `SchemaResolutionError`) and online
   with the registry's `test_compatibility` API in the integration suite.

## Consequences

- A schema change is a reviewed repository change plus a compatibility test, not
  an implicit runtime side effect.
- Producers and consumers share one contract source; the codecs are thin
  mappings to frozen domain dataclasses.
- The v1 contract cannot later add a required field without a new subject
  version and a migration plan; this is exactly the compatibility guarantee the
  project wants to demonstrate.
- Schema Registry availability is required at runtime for deserialization (the
  writer schema id is resolved through it), which is acceptable because the
  broker and registry are part of the same Compose stack.
- The `.avsc` files are small and shipped in the wheel, so `uv tool install` and
  the container image can run without a checkout.

## Alternatives considered

- **Plain JSON without a registry.** Rejected: no compatibility guarantee, no
  schema id, and field drift would surface only at runtime.
- **JSON Schema with the registry.** Rejected: weaker numeric/union typing and
  less natural mapping to the fixed domain records; Avro is the native format
  for the chosen Quix Streams/Kafka stack.
- **Protobuf.** Rejected: requires generated code and a compiler step in the
  image build; Avro's dict mapping keeps the adapters thin and the contracts
  readable in tests.
- **Embedding the writer schema in every message.** Rejected: payload overhead
  and a second source of truth; the registry already stores schemas by id.
- **Registering lazily from each service.** Rejected: a typo or a local
  experiment would publish a new schema version from any process. A single
  idempotent bootstrap makes registration an explicit deployment step.
- **`FULL` compatibility.** Rejected as stricter than needed for additive
  consumer-first evolution and harder to demonstrate with the single-writer
  topology.
