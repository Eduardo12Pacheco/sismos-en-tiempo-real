CREATE TABLE IF NOT EXISTS seismic.window_stats
(
    region LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    event_count UInt64,
    max_magnitude Float64,
    mean_magnitude Float64,
    mean_depth_km Float64,
    total_energy_joules Float64,
    emitted_at DateTime64(3, 'UTC'),
    stored_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(event_count)
ORDER BY (region, window_start)
SETTINGS non_replicated_deduplication_window = 1000
