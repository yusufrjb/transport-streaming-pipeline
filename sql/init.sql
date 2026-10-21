CREATE TABLE IF NOT EXISTS gps_events (
    event_id TEXT PRIMARY KEY,
    vehicle_id TEXT NOT NULL,
    event_time TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    speed_kmh DOUBLE PRECISION,
    route_id TEXT,
    sequence BIGINT,
    is_valid BOOLEAN NOT NULL DEFAULT FALSE,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gps_events_time ON gps_events(event_time);
CREATE INDEX IF NOT EXISTS idx_gps_events_vehicle_time ON gps_events(vehicle_id, event_time);

CREATE OR REPLACE VIEW v_route_minute_metrics AS
SELECT
    date_trunc('minute', event_time) AS event_minute,
    route_id,
    COUNT(*) AS event_count,
    COUNT(DISTINCT vehicle_id) AS vehicles_seen,
    ROUND(AVG(speed_kmh)::numeric, 2) AS avg_speed_kmh,
    COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_event_count
FROM gps_events
WHERE event_time IS NOT NULL
GROUP BY 1, 2;

CREATE OR REPLACE VIEW v_data_quality AS
SELECT
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE is_valid) AS valid_events,
    COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_events,
    ROUND(
      100.0 * COUNT(*) FILTER (WHERE NOT is_valid) / NULLIF(COUNT(*), 0),
      2
    ) AS invalid_percent,
    MAX(ingested_at) AS last_ingested_at
FROM gps_events;
