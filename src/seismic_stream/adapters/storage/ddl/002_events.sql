CREATE TABLE IF NOT EXISTS seismic.events
(
    event_id String,
    source LowCardinality(String),
    event_type LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    magnitude Float64,
    magnitude_type Nullable(String),
    place String,
    region LowCardinality(String),
    latitude Float64,
    longitude Float64,
    depth_km Float64,
    status LowCardinality(String),
    tsunami Bool,
    significance Int32,
    network Nullable(String),
    detail_url Nullable(String),
    in_ecuador_region Bool,
    ingested_at Nullable(DateTime64(3, 'UTC')),
    stored_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY event_id
SETTINGS non_replicated_deduplication_window = 1000
