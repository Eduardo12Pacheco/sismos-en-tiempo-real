# ADR 0005: Effectively-once delivery without Kafka transactions

- **Status:** Accepted
- **Date:** 2026-11-20

## Context

Every stage can crash or be retried: the producer can redeliver after a broker
acknowledgement is lost, the processor writes state and output separately, and
the sink commits Kafka offsets after writing to ClickHouse. Kafka transactions
would only cover Kafka-to-Kafka writes; the final store is ClickHouse, outside
the transaction, so a transaction alone cannot make the end-to-end effect atomic.

The project must state honestly what delivery guarantee it provides and what
duplicates can still be observed.

## Decision

1. Run every consumer with `enable.auto.commit=false` and
   `auto.offset.reset=earliest`, and commit only after the batch's effects are
   durable (processor output produced, or sink rows and dead letters written).
2. Accept at-least-once delivery and make every receiver idempotent:
   - the processor keeps one `updated_at_ms` per `event_id` in Quix state and
     drops duplicates;
   - ClickHouse tables are versioned `ReplacingMergeTree` tables read through
     `FINAL` views;
   - every insert carries a Kafka-derived `insert_deduplication_token`;
   - dead letters have a deterministic `dlq_id = sha256(stage|payload)[:32]`.
3. Do **not** use Kafka transactions or Quix `exactly-once` mode. The sink is a
   custom consumer outside Quix (D9), and transactions would add latency and
   operational complexity without covering the ClickHouse write.
4. Commit the highest consumed offset plus one per partition only after the DLQ
   producer has flushed, so a poison row cannot be lost between the DLQ and the
   offset commit.
5. Document transient duplicates in the raw tables as expected: a crash after a
   ClickHouse insert but before the Kafka commit re-inserts the same rows with
   the same token, which ClickHouse then deduplicates; if a merge already
   replaced rows, `FINAL` still returns one logical row.

## Consequences

- The observable guarantee is **effectively-once**: logical reads
  (`events_latest`, `alerts_latest`, `dead_letters_latest`, final aggregate per
  key) are stable under replays and retries, while raw tables may temporarily
  contain duplicate history.
- `seismic-stream verify` can assert id sets and lag, and report the raw-versus-
  latest row delta instead of pretending it is zero.
- At-least-once commit ordering means a crash can replay a batch; that is safe
  because every receiver is idempotent. It cannot lose an acknowledged message.
- The processor's dedup state is local and unbounded in theory; loss of the
  `processor-state` volume would re-emit aggregates, which the versioned
  ClickHouse aggregate table absorbs.
- No transaction coordinator, no `transactional.id`, no two-phase commit: fewer
  moving parts for a local demo, at the cost of a guarantee that is explicitly
  scope-limited.

## Alternatives considered

- **Kafka transactions end to end.** Rejected: ClickHouse is the final sink and
  is not part of a Kafka transaction; reads would still need `read_committed`
  coordination, and Quix exactly-once adds latency without covering storage.
- **Exactly-once by deduplicating only in ClickHouse.** Rejected: duplicates
  would inflate raw rows and insert costs even if the views hid them.
- **At-most-once (commit before write).** Rejected: a crash could silently lose
  events, which violates S11/S12.
