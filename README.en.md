# sismos-en-tiempo-real
[Español](README.md) · **English**

<p align="center">
  <img src="docs/assets/readme/hero.en.svg" width="100%" alt="sismos-en-tiempo-real: local real-time seismic streaming with real USGS earthquakes, Redpanda, ClickHouse and Grafana">
</p>

<p align="center">
  <strong>Local real-time seismic streaming pipeline.</strong> A single <code>./install.sh</code> builds the whole stack, replays ~1,800 real earthquakes from the USGS (U.S. Geological Survey) and opens live dashboards on your machine.
</p>

<p align="center">

[![CI](https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real/actions/workflows/ci.yml/badge.svg)](https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real/actions/workflows/ci.yml) [![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) [![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](pyproject.toml) [![Docker Compose](https://img.shields.io/badge/docker%20compose-%E2%89%A52.20-blue.svg)](compose.yaml)

</p>

![Seismic Live dashboard during a time-compressed replay](docs/assets/hero.gif)

## What it is

Data engineering demo: one real USGS week is published as Avro events to Redpanda, Quix Streams processes it and ClickHouse stores it for Grafana.

**Status:** portfolio project in active development; version 0.1.0 (see [CHANGELOG](CHANGELOG.md)).

## Why it's different

- **Schema-first streaming:** four versioned Avro contracts in Redpanda's Schema Registry, with `BACKWARD` compatibility checked by contract tests and `auto.register.schemas=false` at runtime.
- **Stateful processing:** exact-duplicate drops, event revisions, 60-minute event-time tumbling windows per region and a severity alert branch, implemented with Quix Streams.
- **Idempotent columnar sink:** ClickHouse `ReplacingMergeTree` upserts plus insert-deduplication tokens, DLQ isolation of poison rows by batch bisection, and pause/resume on outages.
- **Observability:** Prometheus metrics, `/healthz` and `/readyz` on every service, a lag monitor and two provisioned Grafana dashboards whose panels follow the stored data clock.
- **Offline by construction:** the default replay needs no internet once the images are downloaded, and no GPU, API key or cloud account; live USGS polling is an optional flag. Integration tests run against the real Compose stack.

## How it works

```mermaid
flowchart LR
  REC[Recorded USGS sample] --> ING[ingest: validate and pace]
  ING -->|Avro| EV[(seismic.events.v1)]
  ING -->|invalid| DLQ[(seismic.dlq.v1)]
  EV --> PROC[processor: dedup and windows]
  PROC --> AGG[(seismic.aggregates.v1)]
  PROC --> ALR[(seismic.alerts.v1)]
  EV --> SINK[sink: idempotent upserts]
  AGG --> SINK
  ALR --> SINK
  DLQ --> SINK
  SINK --> CH[(ClickHouse)]
  CH --> GRAF[Grafana dashboards]
  ING -. metrics .-> PROM[Prometheus]
  PROC -. metrics .-> PROM
  SINK -. metrics .-> PROM
  PROM --> GRAF
```

Full component, port and data-flow details are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). **Pinned versions:** Redpanda `v26.2.7` · Quix Streams `3.28.1` · ClickHouse `26.8` (LTS line) · Grafana `13.3.1` + ClickHouse plugin `4.23.5` · Prometheus `v3.16.0` · Python `3.12` · Docker Compose `>= 2.20`.

### Live mode (optional)

```sh
SEISMIC_INGEST_SOURCE=usgs docker compose -f compose.yaml -f compose.live.yaml up -d
```

Polls the USGS summary feed at most every 60 seconds, sends `If-Modified-Since`, identifies itself in the `User-Agent`, backs off on rate limits and only emits unseen `(id, updated)` revisions.

## Quickstart

### Linux and macOS

```sh
git clone https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real.git
cd sismos-en-tiempo-real
./install.sh
```

Open <http://localhost:13000>. The default replay compresses one week of event time into ~7 minutes.

### Windows (PowerShell 7+, Docker Desktop with Linux containers)

```powershell
git clone https://github.com/Eduardo12Pacheco/sismos-en-tiempo-real.git
cd sismos-en-tiempo-real
./install.ps1
```

Open <http://localhost:13000>. Prerequisites: Docker Desktop with the WSL2 backend and ≥4 GiB of RAM.

### Demo scenarios

```sh
./install.sh --replay --speed 3600    # faster replay for screenshots (~3 min)
./install.sh --replay --chaos         # malformed records, duplicates and revisions -> DLQ
docker compose stop clickhouse        # watch the sink pause and recover, no event lost
./install.sh --down                   # stop the stack, keep volumes
```

## Results

Measured on this machine with `make bench` (three clean runs, median; see [docs/BENCHMARKS.md](docs/BENCHMARKS.md) for the full report and exact commands).

| Metric | Value |
| --- | --- |
| Stored earthquakes | 1739 |
| Wall time | 6.7 s |
| Throughput | 259.7 events/s |
| End-to-end latency | p50 230.4 ms / p95 487.5 ms |
| Machine | Intel i5-12400T, 31.1 GiB RAM |
| Measured on | 2026-11-20 |

These numbers describe one laptop and are not a service-level objective; reproduce them with `make bench`.

![Pipeline Health while the chaotic replay injects dead letters](docs/assets/pipeline-health-chaos.png)
![Seismic Live dashboard, dark theme](docs/assets/seismic-live-dark.png)
![Seismic Live dashboard, light theme](docs/assets/seismic-live-light.png)
![Pipeline Health from a clean replay](docs/assets/pipeline-health.png)

## Limitations

- Single Redpanda node, one partition per topic, no replication, authentication or TLS.
- Windows and macOS need Docker Desktop; the stack is not natively installable there.
- Event-time windows are provisional: late revisions are not re-counted in already emitted aggregates.
- Processor dedup state is unbounded; the design targets a one-week replay, not a 24/7 feed.
- "Ecuador region" is an inclusive bounding box (including Galápagos), not a tectonic boundary.
- IGEPN was evaluated but not integrated: its only machine-readable endpoint is undocumented, stale and has no stated terms.
- Energy is the Gutenberg-Richter approximation; this is a data-engineering demo, not seismology.
- No Kafka transactions or end-to-end exactly-once; the sink is effectively-once via idempotency.
- Re-recording the USGS sample later may produce a different file and checksum.

## Documentation

[Architecture and patterns](docs/ARCHITECTURE.md) · [Data sources, licence and lineage](docs/DATA.md) · [Operations, settings and troubleshooting](docs/OPERATIONS.md) · [Benchmarks](docs/BENCHMARKS.md) · [Architecture decision records](docs/adr/0001-redpanda.md) · [Contributing](CONTRIBUTING.md) · [Security](SECURITY.md) · [Changelog](CHANGELOG.md)

## License

Code released under the [MIT License](LICENSE), © 2026 Eduardo Pacheco. Bundled earthquake data: **Credit: U.S. Geological Survey** (U.S. Public Domain; see [docs/DATA.md](docs/DATA.md)).

## Author

Eduardo Pacheco · https://github.com/Eduardo12Pacheco
