
"""Shared validation rules for GPS events."""
from datetime import datetime
import math

REQUIRED_FIELDS = {
    "event_id",
    "vehicle_id",
    "event_time",
    "latitude",
    "longitude",
    "speed_kmh",
    "route_id",
    "sequence",
}


def validate_event(event):
    """Return True when an event satisfies the GPS data contract."""
    if hasattr(event, "asDict"):
        event = event.asDict()

    if not isinstance(event, dict):
        return False

    if not REQUIRED_FIELDS.issubset(event):
        return False

    if any(event[field] is None for field in REQUIRED_FIELDS):
        return False

    if not all(
        isinstance(event[field], str) and event[field].strip()
        for field in ("event_id", "vehicle_id", "route_id")
    ):
        return False

    timestamp = event["event_time"]
    if isinstance(timestamp, str):
        try:
            datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return False
    elif not isinstance(timestamp, datetime):
        return False

    try:
        latitude = float(event["latitude"])
        longitude = float(event["longitude"])
        speed = float(event["speed_kmh"])
    except (TypeError, ValueError):
        return False

    if not all(math.isfinite(value) for value in (latitude, longitude, speed)):
        return False

    return (
        -90 <= latitude <= 90
        and -180 <= longitude <= 180
        and 0 <= speed <= 150
    )
