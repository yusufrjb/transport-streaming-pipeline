
"""Unit tests for the shared GPS event contract."""
import unittest
from pathlib import Path

from app.event_contract import validate_event


def sample_event():
    return {
        "event_id": "test-1",
        "vehicle_id": "BUS-001",
        "event_time": "2026-01-01T00:00:00+00:00",
        "latitude": -7.25,
        "longitude": 112.75,
        "speed_kmh": 32.5,
        "route_id": "R-01",
        "sequence": 1,
    }


class EventContractTests(unittest.TestCase):
    def test_valid_event(self):
        self.assertTrue(validate_event(sample_event()))

    def test_negative_speed_is_invalid(self):
        event = sample_event()
        event["speed_kmh"] = -12
        self.assertFalse(validate_event(event))

    def test_speed_above_limit_is_invalid(self):
        event = sample_event()
        event["speed_kmh"] = 151
        self.assertFalse(validate_event(event))

    def test_invalid_coordinates_are_rejected(self):
        event = sample_event()
        event["latitude"] = -91
        self.assertFalse(validate_event(event))

        event = sample_event()
        event["longitude"] = 181
        self.assertFalse(validate_event(event))

    def test_missing_required_field_is_rejected(self):
        event = sample_event()
        del event["vehicle_id"]
        self.assertFalse(validate_event(event))

    def test_null_required_field_is_rejected(self):
        event = sample_event()
        event["sequence"] = None
        self.assertFalse(validate_event(event))

    def test_invalid_timestamp_is_rejected(self):
        event = sample_event()
        event["event_time"] = "not-a-timestamp"
        self.assertFalse(validate_event(event))

    def test_sql_initialization_exists(self):
        self.assertTrue(Path("sql/init.sql").exists())


if __name__ == "__main__":
    unittest.main()
