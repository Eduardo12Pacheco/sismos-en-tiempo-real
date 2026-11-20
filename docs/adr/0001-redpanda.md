# ADR 0001: Redpanda as the local Kafka-API broker

- **Status:** Accepted
- **Date:** 2026-11-20
- **Related:** [Architecture](../ARCHITECTURE.md) · [Operations](../OPERATIONS.md) · [ADR 0003](0003-avro-schema-registry.md)

## Context

The project is a local-first streaming pipeline: one `docker compose up` on a
laptop (8 GB of Docker memory budget, 4 GB targeted) must provide a Kafka-API
broker, a Schema Registry and a metrics endpoint without cloud services, API
keys or a multi-node cluster. The demo has to start quickly, survive restarts
through named volumes, and expose the same producer/consumer APIs used by Quix
Streams and confluent-kafka.

The pipeline publishes roughly 1.7k events per recorded week and replays them at
up to `--speed max`. Ordering matters more than throughput: event-time windows
must be reproducible, so a single partition is preferable.

## Decision

1. Use `redpandadata/redpanda:v26.2.7`, a single node started with
   `--mode dev-container --smp 1 --memory 1G`, as the only broker.
2. Enable the built-in Schema Registry on the same image instead of running a
   separate Confluent Schema Registry container. This halves the memory and
   startup cost and gives one health dependency.
3. Keep one partition and replication factor 1 for all four topics. Retention is
   7 days for `seismic.events.v1` and `seismic.aggregates.v1`, and 30 days for
   `seismic.alerts.v1` and `seismic.dlq.v1`.
4. Publish two Kafka listeners: `internal://redpanda:9092` for Compose services
   and `external://127.0.0.1:19092` for host-side tools and tests. The Schema
   Registry mirrors this with the internal listener on 8081 and the test-only
   external listener on host port 13081 (L4 port range).
5. Bind host test ports to `127.0.0.1` only, and keep the `backend` network
   `internal: true` so application containers have no egress by construction.

## Consequences

- One container provides broker, registry and public broker metrics
  (`:9644/public_metrics`), keeping the stack within the memory budget.
- A single partition preserves event-time order end to end, which is what makes
  the window reference tests exact. Parallelism would require repartitioning and
  a new ADR.
- Replication factor 1 means no high availability: acceptable for a local demo,
  explicitly listed as a limitation.
- Redpanda-specific operational commands (`rpk`) appear in OPERATIONS.md, but
  application code only uses the Kafka protocol and the Confluent-compatible
  Schema Registry API, so the broker can be replaced by Kafka without touching
  domain or application code.
- The dev-container mode disables some production checks; it must never be used
  as a production deployment recipe.

## Alternatives considered

- **Apache Kafka with KRaft plus a separate Schema Registry.** Rejected for
  v0.1.0: two JVM-based services and more memory/startup time for a single-node
  local demo.
- **Kafka with ZooKeeper.** Rejected: extra legacy service with no benefit for a
  new project.
- **Redpanda Cloud or a managed registry.** Rejected by the local-first,
  zero-account constraint.
- **Multiple brokers / replication factor 3.** Rejected: triples memory use and
  adds no demonstrable capability for this volume; scale-out is documented
  instead.
- **Multiple partitions from day one.** Rejected: keyed window state and
  reproducibility would become harder without measurable throughput benefit.
