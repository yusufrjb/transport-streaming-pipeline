"""Generate synthetic GPS events and publish them to Kafka."""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-gps")
RATE = max(1, int(os.getenv("EVENTS_PER_SECOND", "2")))
VEHICLES = max(1, int(os.getenv("VEHICLE_COUNT", "20")))

# Approximate central Surabaya coordinates; generated points are simulated.
CENTER_LAT, CENTER_LON = -7.2575, 112.7521

def connect_producer():
    for attempt in range(1, 31):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                acks="all",
                retries=5,
                linger_ms=50,
            )
        except NoBrokersAvailable:
            print(f"Kafka not ready ({attempt}/30), retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError(f"Could not connect to Kafka at {BOOTSTRAP}")

def main():
    producer = connect_producer()
    print(f"Publishing simulated GPS events to {TOPIC} at about {RATE}/sec", flush=True)
    sequence = 0
    try:
        while True:
            for _ in range(RATE):
                sequence += 1
                vehicle_num = random.randint(1, VEHICLES)
                event = {
                    "event_id": str(uuid.uuid4()),
                    "vehicle_id": f"BUS-{vehicle_num:03d}",
                    "event_time": datetime.now(timezone.utc).isoformat(),
                    "latitude": round(CENTER_LAT + random.uniform(-0.035, 0.035), 6),
                    "longitude": round(CENTER_LON + random.uniform(-0.035, 0.035), 6),
                    "speed_kmh": round(random.uniform(0, 65), 2),
                    "route_id": f"R-{random.randint(1, 5):02d}",
                    "sequence": sequence,
                }
                # Every 100th event is deliberately invalid for data-quality testing.
                if sequence % 100 == 0:
                    event["speed_kmh"] = -12.0
                producer.send(TOPIC, key=event["vehicle_id"].encode(), value=event)
            producer.flush(timeout=10)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Producer stopped", flush=True)
    finally:
        producer.close()

if __name__ == "__main__":
    main()
