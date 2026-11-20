CREATE VIEW IF NOT EXISTS seismic.events_latest AS
SELECT * FROM seismic.events FINAL;

CREATE VIEW IF NOT EXISTS seismic.window_stats_latest AS
SELECT * FROM seismic.window_stats FINAL;

CREATE VIEW IF NOT EXISTS seismic.alerts_latest AS
SELECT * FROM seismic.alerts FINAL;

CREATE VIEW IF NOT EXISTS seismic.dead_letters_latest AS
SELECT * FROM seismic.dead_letters FINAL;

CREATE VIEW IF NOT EXISTS seismic.data_clock AS
SELECT
    max(event_time) AS data_now,
    now64(3) AS wall_now,
    dateDiff('second', data_now, wall_now) AS shift_seconds
FROM seismic.events
