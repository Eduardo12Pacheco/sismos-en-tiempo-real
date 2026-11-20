# ADR 0008: Grafana dashboards as code and an offline Geomap

- **Status:** Accepted
- **Date:** 2026-11-20

## Context

S16 requires two finished dashboards (`seismic-live`, `pipeline-health`) that
work with the replay, whose events are dated in the past. Grafana must render
them without an internet connection, without a tile server and without manual
provisioning. The dashboard JSON is reviewed like code and must not drift from a
generator or from the running Grafana version.

Three choices needed evidence from the real stack:

1. How ClickHouse's Grafana datasource reaches the database (native port 9000
   vs HTTP port 8123) and which numeric `format` values the SQL target uses.
2. How the Geomap panel gets a world map without downloading tiles.
3. How the ClickHouse plugin is installed so the image is reproducible.

## Decision

1. **Dashboards are generated, never hand-edited.** `scripts/build_dashboards.py`
   contains every panel, query, layout, variable and link. It writes
   `docker/grafana/dashboards/{seismic-live,pipeline-health}.json` deterministically
   and `--check` fails when the committed files differ. The file provider has
   `allowUiUpdates: false`, so the JSON is the only source of truth.
2. **ClickHouse over HTTP on port 8123 with the `grafana-clickhouse-datasource`
   plugin 4.23.5.** The health endpoint
   `/api/datasources/uid/clickhouse/health` returns `"status": "OK"` with
   `jsonData.host=clickhouse`, `port=8123`, `protocol=http`,
   `defaultDatabase=seismic`. Native (9000) was documented as the alternative;
   HTTP was chosen because it uses the same protocol and port as the Python
   `clickhouse-connect` client, so there is one network path to validate.
   SQL targets use `editorType: "sql"`, `queryType: "table" | "timeseries"`,
   `format: 1` for tables and `format: 0` for time series, and
   `pluginVersion: "4.23.5"` (D17). Every query returned frames with no error
   through `/api/ds/query` and every panel tagged `expect_data` returned rows
   after a replay.
3. **Data clock at query time.** Replay events are days old. Each time column is
   shifted by `dateDiff('second', max(event_time), now())` inside the query, so
   the newest stored event maps to "now" and the `now-24h..now` range fills up.
   The "Data clock" stat shows the real newest event (`toUnixTimestamp64Milli`,
   unit `dateTimeAsIso`) and the shift in seconds (`dateDiff`, unit `s`). Stored
   rows are never mutated, so live mode (shift approximately zero) and replay
   share the same dashboards, and the dashboard cannot lie about the recorded
   event time.
4. **Offline Geomap with Grafana's bundled countries GeoJSON.** The Geomap
   basemap layer type is `geojson` with
   `src: "public/maps/countries.geojson"`; no tile basemap (`osm`, `carto`,
   `esri`, `xyz`, `maplibre`) is configured. Earthquakes are a `markers` layer
   located by `latitude`/`longitude`, sized by `magnitude` and colored by
   `depth_km`. The dashboard therefore renders a map with zero external
   requests, and the Playwright check asserts the map canvas has non-blank
   pixels in both themes.
5. **Plugin installed at image build time with `grafana cli`.** The Dockerfile
   starts from `grafana/grafana:13.3.1` and runs
   `grafana cli --pluginsDir "${GF_PATHS_PLUGINS}" plugins install grafana-clickhouse-datasource 4.23.5`
   as root before switching back to the `grafana` user. `docker compose run
   --rm --no-deps grafana grafana cli plugins ls` reports
   `grafana-clickhouse-datasource @ 4.23.5`. The documented fallback
   `GF_PLUGINS_PREINSTALL` was not needed.
6. **Credentials via provisioning environment interpolation.** The datasource
   provisioning file uses `$__env{SEISMIC_CLICKHOUSE_USER}` and
   `$__env{SEISMIC_CLICKHOUSE_PASSWORD}` in `secureJsonData`; Compose injects
   the same variables the application services use. No password is committed.
   Grafana is exposed only on `127.0.0.1:13000` with anonymous Viewer access,
   analytics/update checks disabled and the dark theme as the default.
7. **Recorded `schemaVersion`.** Grafana 13.3.1 stamped `schemaVersion: 42` when
   it loaded both dashboards; the generator uses that constant so future
   Grafana upgrades are an explicit diff rather than silent drift.

## Consequences

- A dashboard change is a reviewed Python diff plus regenerated JSON; the unit
  test enforces unique panel ids, non-overlapping `gridPos`, titles,
  descriptions, units, `noValue`, datasource uids, the D17 target shape and the
  operationally required signals.
- The Geomap does not show country labels or terrain because no tiles are
  fetched. That is the intended offline trade-off; the borders come from
  Grafana's bundled asset and the markers carry the information.
- Query-time shifting keeps the data immutable, but a table row's displayed
  timestamp is not the raw stored timestamp. The "Data clock" panel exposes the
  real newest event and the shift so the operator can always recover the
  original time.
- Dashboards are read-only in the UI. Ad-hoc panels must be built in an
  unsaved dashboard; accepted changes go through the generator.
- Grafana's feature-toggle audit line is filtered with
  `GF_LOG_FILTERS=featuremgmt:error`; without that filter a benign info line
  contains both "error" and "provision" substrings and pollutes the
  provisioning error check.

## Alternatives considered

- **Hand-written JSON committed directly.** Rejected: the panel rules (unique
  ids, layout, units, datasource shape) are easy to break and impossible to
  review consistently.
- **A third-party dashboard SDK.** Rejected by D17: plain dicts keep the
  generator dependency-free and the JSON diff predictable.
- **Grafana native ClickHouse datasource.** It does not exist in Grafana
  13.3.1; the signed community plugin is required.
- **Native protocol on port 9000.** Viable, but a second protocol/port with no
  functional gain for read-only SQL. HTTP 8123 is shared with the application
  and passed the health check.
- **A tile basemap (OSM/CARTO).** Rejected: it needs internet, leaks requests
  from a local-first demo and fails offline. The bundled GeoJSON is sufficient
  for a world overview.
- **`GF_PLUGINS_PREINSTALL` instead of a build step.** Rejected as the primary
  path because it downloads at container start (slower, less reproducible); the
  build-time CLI installation was verified and is recorded above.
- **Shifting values at ingest or storing shifted timestamps.** Rejected by ADR
  0007: it would corrupt the recorded event time and break source-to-storage
  comparisons.
