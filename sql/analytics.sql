
CREATE OR REPLACE VIEW v_vehicle_metrics AS
SELECT
    vehicle_id,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE is_valid) AS valid_events,
    COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_events,
    ROUND(
        AVG(speed_kmh) FILTER (
            WHERE is_valid AND speed_kmh >= 0
        )::numeric, 2
    ) AS avg_speed_kmh,
    ROUND(
        MAX(speed_kmh) FILTER (
            WHERE is_valid AND speed_kmh >= 0
        )::numeric, 2
    ) AS max_speed_kmh
FROM gps_events
GROUP BY vehicle_id;


CREATE OR REPLACE VIEW v_route_metrics AS
SELECT
    route_id,
    COUNT(*) AS total_events,
    COUNT(*) FILTER (WHERE is_valid) AS valid_events,
    COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_events,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE is_valid)
        / NULLIF(COUNT(*), 0),
        2
    ) AS valid_percentage,
    ROUND(
        AVG(speed_kmh) FILTER (
            WHERE is_valid AND speed_kmh >= 0
        )::numeric,
        2
    ) AS avg_speed_kmh,
    COUNT(DISTINCT vehicle_id) FILTER (
        WHERE is_valid
    ) AS valid_vehicle_count
FROM gps_events
WHERE route_id IN ('R-01', 'R-02', 'R-03', 'R-04', 'R-05')
GROUP BY route_id;

DROP VIEW IF EXISTS v_data_quality;

CREATE VIEW v_data_quality AS
SELECT
    COUNT(*) AS total_events,
    COUNT(DISTINCT event_id) AS unique_events,
    COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_events,
    COUNT(*) FILTER (WHERE speed_kmh < 0) AS negative_speed_events,
    COUNT(*) FILTER (
        WHERE latitude NOT BETWEEN -90 AND 90
           OR longitude NOT BETWEEN -180 AND 180
    ) AS invalid_coordinate_events,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE NOT is_valid)
        / NULLIF(COUNT(*), 0), 2
    ) AS invalid_percentage
FROM gps_events;