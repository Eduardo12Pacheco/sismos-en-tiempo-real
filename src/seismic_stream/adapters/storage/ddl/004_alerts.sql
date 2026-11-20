CREATE TABLE IF NOT EXISTS seismic.alerts
(
    event_id String,
    level LowCardinality(String),
    reason String,
    magnitude Float64,
    place String,
    event_time DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    tsunami Bool,
    in_ecuador_region Bool,
    stored_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY event_id
SETTINGS non_replicated_deduplication_window = 1000
