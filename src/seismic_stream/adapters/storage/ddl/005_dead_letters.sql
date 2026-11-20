CREATE TABLE IF NOT EXISTS seismic.dead_letters
(
    dlq_id String,
    stage LowCardinality(String),
    error_type String,
    error_message String,
    source_topic Nullable(String),
    source_partition Nullable(Int32),
    source_offset Nullable(Int64),
    payload String,
    failed_at DateTime64(3, 'UTC'),
    attempts UInt32,
    stored_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(failed_at)
ORDER BY dlq_id
SETTINGS non_replicated_deduplication_window = 1000
