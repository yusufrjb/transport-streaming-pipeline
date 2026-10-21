
"""Integration tests for PostgreSQL analytics views."""

import subprocess
import unittest


def query_postgres(sql):
    """Execute a SQL query inside the PostgreSQL container."""
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-X", "-A", "-t", "-F", "|",
            "-v", "ON_ERROR_STOP=1",
            "-U", "transport", "-d", "transport",
            "-c", sql,
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    return result.stdout.strip()


class AnalyticsViewTests(unittest.TestCase):

    def test_data_quality_totals_match_source_table(self):
        result = query_postgres("""
            SELECT
                v.total_events = t.total_events
                AND v.unique_events = t.unique_events
            FROM v_data_quality v
            CROSS JOIN (
                SELECT
                    COUNT(*) AS total_events,
                    COUNT(DISTINCT event_id) AS unique_events
                FROM gps_events
            ) t;
        """)
        self.assertEqual(result, "t")

    def test_invalid_event_count_matches_source_table(self):
        result = query_postgres("""
            SELECT v.invalid_events = (
                SELECT COUNT(*)
                FROM gps_events
                WHERE NOT is_valid
            )
            FROM v_data_quality v;
        """)
        self.assertEqual(result, "t")

    def test_route_view_contains_only_operational_routes(self):
        result = query_postgres("""
            SELECT COALESCE(
                string_agg(route_id, ',' ORDER BY route_id),
                ''
            )
            FROM v_route_metrics;
        """)
        self.assertEqual(
            result,
            "R-01,R-02,R-03,R-04,R-05",
        )

    def test_route_metrics_are_consistent(self):
        result = query_postgres("""
            SELECT COALESCE(bool_and(
                total_events = valid_events + invalid_events
                AND valid_percentage BETWEEN 0 AND 100
                AND valid_vehicle_count >= 0
            ), FALSE)
            FROM v_route_metrics;
        """)
        self.assertEqual(result, "t")


if __name__ == "__main__":
    unittest.main()
