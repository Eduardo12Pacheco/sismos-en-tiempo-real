# ADR 0006: Dead-letter channel, consumer pause and batch bisection

- **Status:** Accepted
- **Date:** 2026-11-20

## Context

A streaming pipeline must survive bad data and an unavailable database without
losing good data or spinning forever. The sink receives four Avro topics, writes
to ClickHouse over HTTP, and can face two very different failures:

- **transient**: connection refused, timeout, HTTP 5xx or a ClickHouse overload
  code (159, 202, 209, 210, 241, 252);
- **permanent**: a row the server rejects because of its type or value.

Treating both as retryable would block forever on one poison row. Treating both
as poison would discard an entire batch during a database restart.

## Decision

1. Classify every storage error in the adapter (`classify_clickhouse_error`)
   into `TransientStoreError` or `PermanentStoreError`; the application layer
   never imports a driver exception type.
2. On a transient error, pause every assigned partition, set
   `seismic_sink_paused=1`, count `seismic_sink_retries_total`, sleep
   `min(SEISMIC_RETRY_MAX_SECONDS, SEISMIC_RETRY_BASE_SECONDS * 2^attempt)` with
   full jitter through the injected `Clock`, and retry the same batch. Do not
   commit offsets until the batch succeeds. Resume and clear the gauge after
   success. `/readyz` turns unready after 30 s of continuous failure (S11).
3. On a permanent error, bisect the batch: try the parent, then halves, until a
   single row is rejected. Store every accepted row and emit only the rejected
   rows as `sink_persist` dead letters. Sub-batch insert tokens append
   `:<lo>-<hi>` so accepted halves are not confused with the parent attempt.
4. Route undecodable Kafka bytes to `sink_decode` with the raw payload and the
   source topic/partition/offset, then continue. Never dead-letter a dead letter;
   an undecodable DLQ record is logged and skipped.
5. Use `dlq_id = sha256(stage|payload)[:32]`, publish to
   `seismic.dlq.v1` keyed by that id, persist the envelope in `dead_letters`, and
   count `seismic_dlq_messages_total{stage,error_type}`.
6. Flush the DLQ producer before committing the source offsets, so a rejected
   row cannot be acknowledged without its dead letter being durable.
7. Handle SIGTERM by finishing the current batch, flushing the DLQ and committing
   before exiting 0; an idle sink also exits cleanly after `--timeout`.

## Consequences

- A ClickHouse restart causes backpressure rather than data loss: lag rises while
  the consumer is paused and returns to zero after recovery.
- One bad row cannot poison a whole batch; healthy rows in the same batch are
  stored and only the poison row is dead-lettered.
- Bisection is O(log n) extra writes per poison row and bounded by the batch size,
  which is small (`SEISMIC_SINK_BATCH_SIZE`, default 500).
- Retry jitter is deterministic in tests because the RNG and clock are injected.
- The retry loop waits indefinitely for a transient failure by design; operators
  see `seismic_sink_paused` and `/readyz`, and can stop the service with SIGTERM,
  which still finishes the current batch before exiting.
- DLQ envelopes are deterministic, so replaying the same poison bytes produces
  the same id and ClickHouse replaces rather than duplicates the dead letter.

## Alternatives considered

- **Retry every error.** Rejected: a permanently rejected row would block the
  pipeline forever.
- **DLQ the whole batch on any permanent error.** Rejected: it would discard good
  rows that shared the batch with one poison row.
- **Crash and restart on transient errors.** Rejected: restarts thrash the
  consumer and lose the in-flight batch boundary; pause/resume is explicit and
  observable.
- **A Kafka transaction around the DLQ publish and offset commit.** Rejected:
  the sink is a custom loop and the DLQ producer is shared; flush-before-commit
  gives the required ordering with less machinery (ADR 0005).
