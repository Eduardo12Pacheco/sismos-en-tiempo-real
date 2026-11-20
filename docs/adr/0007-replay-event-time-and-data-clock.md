# ADR 0007: Replay, event time and the data clock

- **Status:** Accepted
- **Date:** 2026-11-20
- **Related:** [Architecture](../ARCHITECTURE.md) · [Data](../DATA.md) · [ADR 0008](0008-grafana-dashboards-as-code.md)

## Context

The project must demonstrate a complete streaming pipeline with no external
services, no network access and reproducible results. A recorded USGS week is
bundled, but playing a week at real speed would make demos and end-to-end tests
useless. At the same time, event-time windowing must remain exactly reproducible
regardless of how fast the replay runs.

Live mode writes events whose `event_time` is close to wall-clock `now`. Replay
writes events whose `event_time` is days in the past, so a dashboard driven by
wall-clock time would show empty panels until the replay catches up.

## Decision

1. **Recorded sample as the default source.** `ReplaySource` reads the bundled
   GeoJSON FeatureCollection (or NDJSON) and yields `SourceRecord`s sorted by
   `(properties.time, id)`; features without a usable timestamp keep file order
   at the end. Sorting makes an unpaced replay deterministic.
2. **Time compression with a single pacer.** `ReplayPacer` anchors on the first
   timed record at `(wall0, event0)` and sleeps
   `max(0, wall0 + (event_ms - event0) / 1000 / speed - clock.now())`.
   `speed` is a float greater than zero; `--speed max` maps to `None`, which
   never sleeps. For increasing event times the total scheduled sleep is exactly
   `(last_event_ms - first_event_ms) / 1000 / speed`; the pacer clamps at zero
   and exposes `schedule_lag_seconds` for the `seismic_replay_schedule_lag_seconds`
   metric.
3. **Chaos only adds records.** `ChaoticSource` decorates any `EventSource` and,
   after each source record, may append a malformed copy, an exact duplicate or
   a revision produced from a seeded `random.Random`. It never removes, drops or
   reorders source records, so the set of valid stored ids does not depend on
   the chaos plan. `ChaosPolicy.plan()` walks the same code path as the source,
   which lets tests derive expected injection counts.
4. **Event time, not processing time.** The Quix Streams topology extracts
   `event_time_ms` for windowing and uses tumbling windows with a grace period.
   Wall-clock speed therefore changes only how long a replay takes, never the
   aggregates it produces.
5. **Single partition.** All topics have one partition so event-time order is
   preserved end to end and window results are exactly reproducible at any
   speed. ADR-scale scale-out (more partitions, keyed parallel windows) is
   documented as future work rather than implemented.
6. **Data clock for dashboards.** Seismic panels compute
   `shift_seconds = dateDiff('second', max(event_time), now())` from ClickHouse
   and display `event_time + shift_seconds`, so the newest stored event reads as
   "now". A stat panel shows the real newest event time and the shift. In live
   mode the shift is approximately zero, so the same dashboards work for both
   modes.
7. **No replay loop.** Replay ends when the file is exhausted. Looping would
   duplicate event ids and obscure delivery guarantees; rerunning the command is
   explicit and idempotent by construction on the storage side.

## Consequences

- Demos and e2e tests run in minutes; `SEISMIC_REPLAY_SPEED=1440` replays the
  week in roughly seven minutes and `max` is bounded only by processing speed.
- Aggregates are independent of replay speed, which makes the reference
  implementation in tests meaningful.
- The pacer is deterministic with an injected `Clock` and needs no sleeping in
  unit tests.
- Revisions are not re-counted in windows (the processor excludes revisions),
  and window results stay provisional until the grace period elapses. This is
  documented as a limitation.
- Event-time ordering relies on the single partition; increasing parallelism
  requires a new partitioning decision and a new ADR.

## Alternatives considered

- **Replay at real speed.** Rejected: a week-long run is unusable for demos and
  CI.
- **Sleep according to processing timestamps.** Rejected: window results would
  depend on machine load and replay speed.
- **Loop the file to keep the dashboard busy.** Rejected: duplicated ids would
  interfere with deduplication and delivery-guarantee tests.
- **Shift data at ingest.** Rejected: it would mutate event time and prevent
  comparing stored rows with the source recording. Shifting only at query time
  keeps the stored data faithful.
- **Multiple partitions with per-key windows.** Rejected for v0.1.0: the sample
  is small, one partition keeps reproducibility trivial and the scale-out path is
  documented instead.
