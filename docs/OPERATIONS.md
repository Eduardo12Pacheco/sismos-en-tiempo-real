# Operations

This is the runbook for the local `realtime-seismic-stream` stack: prerequisites,
commands, configuration, ports, metrics, chaos drills, live mode, troubleshooting
and uninstallation. The architecture and contracts are described in
[ARCHITECTURE.md](ARCHITECTURE.md); data provenance is in [DATA.md](DATA.md).

## Prerequisites

| Requirement | Linux | macOS | Windows |
|---|---|---|---|
| Container runtime | Docker Engine 24+ with Compose v2 | Docker Desktop with Compose v2 | Docker Desktop with Linux containers, WSL2 backend |
| Compose | `docker compose` >= 2.20 | >= 2.20 | >= 2.20 |
| Memory for Docker | 4 GiB | 4 GiB | 4 GiB |
| Free disk | 5 GiB | 5 GiB | 5 GiB |
| Optional development tooling | Python 3.12 and [uv](https://docs.astral.sh/uv/) | same | same |

The stack does not need a GPU, an API key, a cloud account, or internet access
after the images are pulled: replay mode is fully offline. Windows users must
run Docker Desktop in **Linux containers** mode (not Windows containers) and
have WSL2 installed; `.\install.ps1 -Check -Json` reports the detected mode.

## Quickstart

Linux and macOS:

```bash
git clone https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real.git
cd sismos-en-tiempo-real
./install.sh
```

Windows PowerShell:

```powershell
git clone https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real.git
Set-Location sismos-en-tiempo-real
.\install.ps1
```

Open `http://localhost:13000` once Grafana is healthy. The installer checks the
prerequisites, creates `.env` from `.env.example` if missing, builds the shared
application image, starts Redpanda, ClickHouse, Prometheus, Grafana, the
processor, the sink and the monitor, then launches the replay consumer. The
default replay speed is `1440` (one day of event time per minute, about seven
minutes for the bundled week).

Re-running the installer is idempotent: it rebuilds only what changed and never
removes data volumes.

## Commands

### Installer

| Command | Purpose |
|---|---|
| `./install.sh --check` | Check prerequisites only; exit 1 on a failed check |
| `./install.sh --check --json` | Machine-readable prerequisite report |
| `./install.sh` | Build and start the stack, then replay |
| `./install.sh --replay` | Start the stack and keep the replay in the foreground |
| `./install.sh --replay --chaos` | Replay with malformed records, duplicates and revisions |
| `./install.sh --replay --speed 3600` | Fast replay; use `max` to disable pacing |
| `./install.sh --down` | Stop containers and keep volumes |
| `./install.sh --purge --yes` | Remove containers, volumes, local images and `.env` |

`install.ps1` accepts the same options with PowerShell casing: `-Check -Json`,
`-Replay`, `-Speed`, `-Chaos`, `-Down`, `-Purge`, `-Yes`.

### Containerized CLI

The `cli` service is under the `tools` profile and reuses the application image:

```bash
docker compose run --rm cli doctor
docker compose run --rm cli verify --timeout 180
docker compose run --rm cli replay-info --json
```

### Host CLI

With `uv sync --frozen`, the same commands run on the host against the
loopback-published broker and database:

```bash
uv run seismic-stream bootstrap
uv run seismic-stream ingest --source replay --speed max
uv run seismic-stream process
uv run seismic-stream sink
uv run seismic-stream monitor
uv run seismic-stream doctor
uv run seismic-stream verify --timeout 180
uv run seismic-stream replay-info --file data/samples/usgs_2026-11-02_2026-11-09_m1.geojson
```

`ingest` also accepts `--file`, `--chaos/--no-chaos`, `--seed`, `--limit` and
`--dry-run`; `process` accepts `--timeout` and `--count`; `sink` accepts
`--timeout`; `verify` accepts `--file`, `--timeout` and `--json`. Every command
exits 0 on success, 1 on a runtime or verification failure, and 2 on a usage or
configuration error.

### Developer commands

| Command | Purpose |
|---|---|
| `make help` | List every Make target |
| `make sync` | Install the locked environment |
| `make lint` / `make fmt` | Ruff lint and format check / autofix |
| `make types` | `mypy --strict src` |
| `make arch` | Import-linter contracts |
| `make test` | Unit, property and architecture tests |
| `make test-int` / `make test-e2e` / `make test-ui` | Docker-backed suites |
| `make cov` | Full suite with the 85% coverage gate |
| `make ci` | Local equivalent of every CI job |
| `make down` | Stop the stack, keep volumes |
| `make replay` / `make demo-chaos` | Restart the ingest service |

## Configuration

Every variable is optional and read by `pydantic-settings` with the `SEISMIC_`
prefix. The defaults target the local Compose stack. Invalid values fail fast at
startup with exit code 2 and a message naming the offending variable.

| Variable | Default | Description |
|---|---|---|
| `SEISMIC_KAFKA_BOOTSTRAP` | `localhost:19092` | Kafka API endpoint; inside Compose it is `redpanda:9092`. |
| `SEISMIC_SCHEMA_REGISTRY_URL` | `http://localhost:13081` | Confluent-compatible Schema Registry URL. |
| `SEISMIC_CLICKHOUSE_HOST` | `localhost` | ClickHouse host; inside Compose it is `clickhouse`. |
| `SEISMIC_CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port. |
| `SEISMIC_CLICKHOUSE_USER` | `seismic` | ClickHouse user. |
| `SEISMIC_CLICKHOUSE_PASSWORD` | `seismic-local` | Development-only password; replace outside local use. |
| `SEISMIC_CLICKHOUSE_DATABASE` | `seismic` | Database that holds the migrations and tables. |
| `SEISMIC_INGEST_SOURCE` | `replay` | `replay` reads the bundled sample; `usgs` polls the live feed. |
| `SEISMIC_REPLAY_FILE` | `data/samples/usgs_2026-11-02_2026-11-09_m1.geojson` | GeoJSON FeatureCollection or NDJSON of features. |
| `SEISMIC_REPLAY_SPEED` | `1440` | Positive compression factor, or `max` to disable pacing. |
| `SEISMIC_REPLAY_CHAOS` | `false` | Enables deterministic malformed, duplicate and revision records. |
| `SEISMIC_CHAOS_SEED` | `42` | Seed for the chaos plan. |
| `SEISMIC_CHAOS_MALFORMED_RATE` | `0.02` | Probability of a malformed copy, in `[0, 1]`. |
| `SEISMIC_CHAOS_DUPLICATE_RATE` | `0.05` | Probability of an exact duplicate, in `[0, 1]`. |
| `SEISMIC_CHAOS_REVISION_RATE` | `0.02` | Probability of a revision, in `[0, 1]`. |
| `SEISMIC_LIVE_FEED` | `all_hour` | USGS summary feed name; see the allowed list below. |
| `SEISMIC_LIVE_POLL_SECONDS` | `60` | Poll interval; values below 60 are rejected. |
| `SEISMIC_INCLUDE_EVENT_TYPES` | `earthquake` | Comma-separated USGS event types to publish. |
| `SEISMIC_ALERT_WARNING_MAG` | `6.0` | WARNING threshold, inclusive. |
| `SEISMIC_ALERT_WATCH_MAG` | `5.0` | WATCH threshold, inclusive. |
| `SEISMIC_ALERT_ECUADOR_MAG` | `4.0` | WATCH threshold inside the Ecuador bounding box. |
| `SEISMIC_WINDOW_MINUTES` | `60` | Event-time tumbling window length. |
| `SEISMIC_WINDOW_GRACE_MINUTES` | `10` | Allowed lateness after a window closes. |
| `SEISMIC_STATE_DIR` | `./.state` | Quix Streams local state directory; `/var/lib/seismic/state` in Compose. |
| `SEISMIC_SINK_BATCH_SIZE` | `500` | Maximum records drained per sink batch. |
| `SEISMIC_SINK_BATCH_MAX_WAIT_SECONDS` | `1.0` | Maximum wait to fill a sink batch. |
| `SEISMIC_RETRY_BASE_SECONDS` | `0.5` | Base delay for the capped exponential retry policy. |
| `SEISMIC_RETRY_MAX_SECONDS` | `30` | Maximum retry delay. |
| `SEISMIC_PRODUCER_QUEUE_MAX_MESSAGES` | `10000` | librdkafka producer queue bound; full queues trigger backpressure waits. |
| `SEISMIC_METRICS_PORT` | `9101` | `/metrics`, `/healthz`, `/readyz` port; each service injects its own default. |
| `SEISMIC_MONITOR_GROUPS` | `seismic-processor,seismic-sink` | Comma-separated consumer groups watched by `monitor`. |
| `SEISMIC_MONITOR_INTERVAL_SECONDS` | `5` | Lag sampling interval. |
| `SEISMIC_LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `SEISMIC_LOG_FORMAT` | `json` | `json` for structured logs, `console` for local development. |

`SEISMIC_IMAGE` in `.env.example` is read by Compose (not by `Settings`) and
selects the locally built application image tag.

Allowed live feeds: `all_hour`, `all_day`, `all_week`, `2.5_hour`, `2.5_day`,
`4.5_hour`, `4.5_day`, `significant_hour`, `significant_day`.

## Ports

| Endpoint | Host binding | Container port | Notes |
|---|---|---|---|
| Grafana | `127.0.0.1:13000` | 3000 | The only published service in the default stack. |
| Redpanda Kafka | none in the default stack | 9092 internal, 19092 external listener | Test override publishes `127.0.0.1:19092`. |
| Schema Registry | none in the default stack | 8081 internal, 18081 external listener | Test override publishes `127.0.0.1:13081`. |
| Redpanda admin metrics | none | 9644 | Prometheus scrapes `/public_metrics`. |
| ClickHouse HTTP | none in the default stack | 8123 | Test override publishes `127.0.0.1:13123`. |
| Prometheus | none | 9090 | Internal scrape and query API. |
| ingest metrics | none | 9101 | `/metrics`, `/healthz`, `/readyz`. |
| processor metrics | none | 9102 | `/metrics`, `/healthz`, `/readyz`. |
| sink metrics | none | 9103 | `/metrics`, `/healthz`, `/readyz`. |
| monitor metrics | none | 9104 | `/metrics`, `/healthz`, `/readyz`. |

All host bindings use loopback (`127.0.0.1`) and stay inside the portfolio range
13000-13999 / 19000-19999.

## Metric catalog

Each service registers only the metrics it owns; `/metrics` returns OpenMetrics
text. The names, types and labels below are the single source of truth in
`METRIC_CATALOG` and are drift-tested against this table.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `seismic_build_info` | gauge | `service`, `version` | Build information for the running service. |
| `seismic_ingest_records_total` | counter | `source`, `outcome` | Source records read by ingest, by `published`, `filtered` or `invalid`. |
| `seismic_ingest_backpressure_waits_total` | counter | none | Producer queue-full waits during ingest. |
| `seismic_replay_schedule_lag_seconds` | gauge | none | How far behind schedule the replay pacer is running. |
| `seismic_replay_progress_ratio` | gauge | none | Replay progress as a fraction of the source records. |
| `seismic_processor_events_total` | counter | `outcome` | Processor decisions: `new`, `revision` or `duplicate`. |
| `seismic_processor_alerts_total` | counter | `level` | Alerts emitted, by `WARNING` or `WATCH`. |
| `seismic_processor_window_updates_total` | counter | none | Window statistics updates emitted. |
| `seismic_processor_skipped_total` | counter | `reason` | Records skipped as `undecodable` or `processing_error`. |
| `seismic_sink_rows_total` | counter | `table` | Rows stored by the sink, per ClickHouse table. |
| `seismic_sink_batches_total` | counter | `table`, `outcome` | Sink batches processed, per table and outcome. |
| `seismic_sink_insert_seconds` | histogram | `table` | Duration of ClickHouse inserts. |
| `seismic_sink_retries_total` | counter | none | Transient sink retries. |
| `seismic_sink_paused` | gauge | none | `1` while the consumer assignment is paused. |
| `seismic_sink_end_to_end_seconds` | histogram | none | Ingest-to-storage latency. |
| `seismic_dlq_messages_total` | counter | `stage`, `error_type` | Dead letters, by stage and error type. |
| `seismic_consumer_lag_messages` | gauge | `group`, `topic` | Consumer lag in messages. |
| `seismic_topic_end_offset` | gauge | `topic` | Current end offset of a topic. |

The `ingest` metrics port disappears between replays because the service is a
one-shot container; Prometheus marks that target down until the next replay,
which is expected.

## Chaos and outage drills

Deterministic fault injection adds malformed records, exact duplicates and
revisions right after their source record. It only adds records, so the set of
valid ids never depends on the chaos plan.

```bash
./install.sh --replay --chaos
docker compose run --rm cli verify --timeout 180
docker compose exec -T clickhouse clickhouse-client --user seismic --password seismic-local \
  -q "SELECT stage, error_type, count() FROM seismic.dead_letters_latest GROUP BY stage, error_type"
```

By default the same seed (`42`) and the same sample produce the same plan. To
watch backpressure and recovery, stop ClickHouse while a replay is running and
start it again:

```bash
docker compose stop clickhouse
sleep 10
docker compose start clickhouse
docker compose run --rm cli verify --timeout 180
```

The sink pauses its consumer, `seismic_sink_paused` becomes 1 and consumer lag
rises; the sink retries with capped exponential backoff without committing
offsets, then resumes and catches up. `/readyz` turns 503 after 30 seconds of
continuous failure. No event is lost.

## Broker recipes with rpk

`rpk` runs inside the Redpanda container, so it works on every host OS:

```bash
docker compose exec redpanda rpk topic list
docker compose exec redpanda rpk topic consume seismic.dlq.v1 -n 5
docker compose exec redpanda rpk topic consume seismic.events.v1 -n 1 --offset start
docker compose exec redpanda rpk group list
docker compose exec redpanda rpk group describe seismic-sink
```

Inspect schema registration from the host:

```bash
curl -s http://127.0.0.1:13081/subjects
curl -s http://127.0.0.1:13081/config/seismic.events.v1-value
```

The Schema Registry port is published by `compose.test.yaml`; without that
override, run the equivalent `curl` from inside the backend network.

## Live mode

Live mode is optional and requires internet egress only for the `ingest`
container. It polls the documented USGS GeoJSON summary feed (at most once per
60 seconds, with conditional `If-Modified-Since` requests and exponential
backoff on errors):

```bash
docker compose -f compose.yaml -f compose.live.yaml up -d --wait
docker compose logs -f ingest
```

Or with the host CLI: `SEISMIC_INGEST_SOURCE=usgs SEISMIC_LIVE_FEED=all_hour uv run seismic-stream ingest`.
Everything else stays on the internal `backend` network. Tests never call the
live feed; they use fixtures derived from the recorded sample.

## Troubleshooting

| Symptom | Check | Fix |
|---|---|---|
| Grafana not reachable | `docker compose ps` | Wait for the healthcheck; confirm port 13000 is free. |
| Service unhealthy | `docker compose logs <service> --tail 50` | Inspect the JSON log line; `doctor` reports missing topics, schemas or tables. |
| `verify` reports missing ids | `docker compose logs ingest sink --tail 100` | Wait for the replay to finish, then re-run `doctor`; check sink lag in Grafana. |
| Sink paused for long | `docker compose ps clickhouse`, ClickHouse logs | ClickHouse is down or out of disk; the sink resumes automatically. |
| Invalid configuration | startup error naming `SEISMIC_*` | Fix the value in the shell or `.env`; exit code is 2. |
| Stale replay data | `docker compose exec redpanda rpk topic list` | Run `./install.sh --purge --yes` for a clean local state. |
| Windows Docker mode error | `docker info --format '{{.OSType}}'` | Switch Docker Desktop to Linux containers. |
| Replay too slow | elapsed time versus the speed estimate | Use `--speed max` for tests or a faster installer speed. |

Useful diagnostics:

```bash
docker compose ps
docker compose run --rm cli doctor
docker compose run --rm cli verify --timeout 180
docker compose exec -T prometheus wget -qO- http://localhost:9090/api/v1/targets
```

## Uninstall

```bash
./install.sh --down        # stop containers, keep volumes and .env
./install.sh --purge --yes # remove containers, volumes, local images and .env
```

On Windows use `.\install.ps1 -Down` and `.\install.ps1 -Purge -Yes`. The purge
removes the named volumes, so ClickHouse and Redpanda history is lost; it never
touches other Compose projects.
