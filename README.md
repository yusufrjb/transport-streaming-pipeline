# Real-Time Transport Data Pipeline

An end-to-end data engineering project that streams synthetic vehicle GPS events through **Apache Kafka**, processes them with **Spark Structured Streaming**, and stores the results in **PostgreSQL** for SQL-based analytics.

The pipeline validates incoming events, retains invalid records for auditing, prevents duplicate inserts, and provides analytics views for data quality, vehicle activity, and route performance.

> **Data disclaimer:** All GPS events are synthetic and simulated around Surabaya, Indonesia. They do not represent real vehicles or live traffic conditions.

## Architecture

```text
Python GPS Producer
        |
        v
Apache Kafka
vehicle-gps (3 partitions)
        |
        v
Spark Structured Streaming
- JSON parsing and validation
- Event quality flagging
- Persistent checkpointing
        |
        v
PostgreSQL
- Event storage
- Data quality analytics
- Vehicle and route metrics
```

## Tech Stack

- **Python** — synthetic event generation and automated tests
- **Apache Kafka 3.9.1** — event streaming in KRaft mode
- **Apache Spark 3.5.8** — stream processing
- **PostgreSQL 16** — persistent storage and SQL analytics
- **Docker Compose** — local service orchestration
- **Python unittest** — automated testing

## Key Features

- Continuous GPS event generation for multiple vehicles and routes.
- Three-partition Kafka topic for event ingestion.
- Streaming JSON parsing and data validation.
- Invalid-event retention with an `is_valid` flag.
- Duplicate prevention using `event_id` and `ON CONFLICT DO NOTHING`.
- Persistent Spark checkpoints.
- Four SQL analytics views:
  - `v_data_quality`
  - `v_vehicle_metrics`
  - `v_route_metrics`
  - `v_route_minute_metrics`
- Automated unit, integration, and analytics-view tests.

## Getting Started

**Prerequisites:** Docker Desktop or Docker Engine with Docker Compose, Python, and approximately 5–8 GB of free disk space.

Start the pipeline from the project directory:

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f spark
```

The first startup may take longer while Spark downloads its Kafka connector.

## SQL Analytics

Connect to PostgreSQL:

```bash
docker compose exec postgres psql -U transport -d transport
```

**Data quality**

```sql
SELECT * FROM v_data_quality;
```

**Route performance**

```sql
SELECT *
FROM v_route_metrics
ORDER BY route_id;
```

**Vehicle activity**

```sql
SELECT *
FROM v_vehicle_metrics
ORDER BY total_events DESC;
```

**Route activity by minute**

```sql
SELECT *
FROM v_route_minute_metrics
ORDER BY event_minute DESC, route_id
LIMIT 20;
```

The operational route metrics cover `R-01` through `R-05`. Test records under `R-TEST` remain in the source table but are excluded from the operational route view.

## Run Tests

With the required Docker services running:

```bash
python -m unittest discover -s tests -v
```

The test suite contains 14 tests covering event validation, integration, duplicate protection, and analytics-view consistency.

Run the tests again after code or SQL changes to verify the current implementation.

## Design Considerations

- Invalid events are retained for auditing instead of being silently discarded.
- PostgreSQL primary-key conflict handling prevents duplicate event inserts during retries.
- Spark checkpoints persist streaming progress across container restarts.
- Kafka runs as a single broker with a replication factor of one; this is a local portfolio environment, not a highly available production deployment.
- Event-time watermarking and explicit late-event handling are not yet implemented.
