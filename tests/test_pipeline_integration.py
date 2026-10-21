"""Integration tests for the Kafka -> Spark -> PostgreSQL pipeline.

Prerequisites:
- Docker Compose services are running.
- The Kafka topic vehicle-gps exists.
- PostgreSQL is accessible using the project's local credentials.
"""

import json
import os
import subprocess
import time
import unittest
import uuid

import psycopg2


DB_CONFIG = {
    "host": os.getenv("TEST_DB_HOST", "localhost"),
    "port": int(os.getenv("TEST_DB_PORT", "5432")),
    "dbname": os.getenv("TEST_DB_NAME", "transport"),
    "user": os.getenv("TEST_DB_USER", "transport"),
    "password": os.getenv("TEST_DB_PASSWORD", "transport_dev_only"),
}

KAFKA_COMMAND = [
    "docker", "compose", "exec", "-T", "kafka",
    "/opt/kafka/bin/kafka-console-producer.sh",
    "--bootstrap-server", "kafka:9092",
    "--topic", "vehicle-gps",
]


def make_event(event_id, speed):
    return {
        "event_id": event_id,
        "vehicle_id": "BUS-TEST",
        "event_time": "2026-10-21T07:15:00Z",
        "latitude": -7.2575,
        "longitude": 112.7521,
        "speed_kmh": speed,
        "route_id": "R-TEST",
        "sequence": 1,
    }


def publish_event(event):
    result = subprocess.run(
        KAFKA_COMMAND,
        input=json.dumps(event) + "\n",
        text=True,
        capture_output=True,
        timeout=15,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)

def query_postgres(sql, *params):
    """Run a read-only SQL query through psql in the PostgreSQL container."""
    result = subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-X", "-A", "-t",
            "-U", "transport", "-d", "transport",
            "-c", sql,
        ],
        text=True,
        capture_output=True,
        timeout=15,
        check=True,
    )
    return result.stdout.strip()


def wait_for_event(event_id, timeout=60):
    deadline = time.time() + timeout

    while time.time() < deadline:
        sql = (
            "SELECT speed_kmh, is_valid FROM gps_events "
            f"WHERE event_id = '{event_id}'"
        )
        output = query_postgres(sql)

        if output:
            speed, is_valid = output.split("|")
            return float(speed), is_valid.lower() == "t"

        time.sleep(2)

    raise AssertionError(
        f"Event {event_id} was not found in PostgreSQL within {timeout}s"
    )


class PipelineIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Fail early if PostgreSQL is unavailable.
        query_postgres("SELECT 1")

    def test_valid_event_reaches_postgres_once(self):
        event_id = f"it-valid-{uuid.uuid4().hex}"

        event = make_event(event_id, 30.0)
        publish_event(event)

        speed, is_valid = wait_for_event(event_id)
        self.assertEqual(speed, 30.0)
        self.assertTrue(is_valid)

        # Republish the exact same event to test idempotent storage.
        publish_event(event)
        time.sleep(10)

        count = int(query_postgres(
            "SELECT COUNT(*) FROM gps_events "
            f"WHERE event_id = '{event_id}'"
        ))

        self.assertEqual(count, 1)

    def test_negative_speed_is_stored_as_invalid(self):
        event_id = f"it-invalid-{uuid.uuid4().hex}"
        publish_event(make_event(event_id, -12.0))

        speed, is_valid = wait_for_event(event_id)
        self.assertEqual(speed, -12.0)
        self.assertFalse(is_valid)


if __name__ == "__main__":
    unittest.main()
