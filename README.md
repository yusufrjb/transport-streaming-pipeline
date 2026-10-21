# Real-Time Transport Data Pipeline

An end-to-end streaming data engineering portfolio project that processes synthetic vehicle GPS events using Apache Kafka, Spark Structured Streaming, and PostgreSQL.

The pipeline ingests GPS events, validates records, retains invalid events for auditing, prevents duplicate inserts, and provides SQL analytics for data quality, vehicle activity, and route performance.

> **Data disclaimer:** All GPS events are synthetic and simulated around Surabaya, Indonesia. They do not represent real vehicles or live traffic conditions.

## Architecture

```text
Synthetic GPS Producer (Python)
             |
             v
      Kafka: vehicle-gps
       3 partitions
             |
             v
 Spark Structured Streaming
  - JSON parsing
  - Event validation
  - Validity flagging
  - Persistent checkpointing
             |
             v
       PostgreSQL
  - gps_events
  - v_data_quality
  - v_vehicle_metrics
  - v_route_metrics
  - v_route_minute_metrics
```

## Technology Stack

- **Python** — synthetic GPS event generation and automated tests
- **Apache Kafka 3.9.1** — event streaming in KRaft mode
- **Apache Spark 3.5.8** — Structured Streaming and event processing
- **PostgreSQL 16** — persistent event storage and SQL analytics
- **Docker Compose** — local service orchestration
- **Python unittest** — unit, integration, and analytics-view tests

## Key Features

- Continuous synthetic GPS event generation for multiple vehicles and routes.
- Kafka topic configured with three partitions.
- Streaming JSON parsing and event validation.
- Invalid events retained in PostgreSQL with an `is_valid` flag.
- Duplicate insert prevention using `event_id` as the primary key and `ON CONFLICT DO NOTHING`.
- Persistent Spark checkpoint volume.
- SQL views for data quality, vehicle metrics, route metrics, and route-level minute metrics.
- Automated tests for event validation, pipeline integration, deduplication, and SQL analytics consistency.

## Prerequisites

- Docker Desktop on Windows, or Docker Engine with the Compose plugin on Linux.
- At least 4 GB of RAM available to Docker; 6 GB is more comfortable.
- Internet access for initial image and Spark connector downloads.
- Approximately 5–8 GB of free disk space.

## Run the Pipeline

Run these commands from the project directory:

```bash
docker compose up --build -d
docker compose ps
docker compose logs -f spark
```

On the first run, Spark may download its Kafka connector from Maven Central. Wait for startup to complete and check the logs for successful batch processing.

The Compose configuration starts the Kafka and PostgreSQL services, creates the Kafka topic, and starts the producer and Spark processor.

## Verify Kafka

Inspect the topic configuration:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe --topic vehicle-gps
```

The topic is configured with three partitions and a replication factor of one.

## SQL Analytics

Open a PostgreSQL shell:

```bash
docker compose exec postgres psql -U transport -d transport
```

### 1. Data quality

```sql
SELECT * FROM v_data_quality;
```

Reports total events, unique event IDs, invalid events, negative-speed events, invalid-coordinate events, events with missing required fields, and the invalid-event percentage.

### 2. Route metrics

```sql
SELECT *
FROM v_route_metrics
ORDER BY route_id;
```

Reports total, valid, and invalid events; valid-event percentage; average valid speed; and the number of vehicles with at least one valid event.

The operational view includes routes `R-01` through `R-05`. Test data under `R-TEST` is excluded from this view but retained in the source table.

### 3. Vehicle metrics

```sql
SELECT *
FROM v_vehicle_metrics
ORDER BY total_events DESC;
```

Provides event counts, validity counts, average speed, and maximum valid speed per vehicle.

### 4. Route activity over time

```sql
SELECT *
FROM v_route_minute_metrics
ORDER BY event_minute DESC, route_id
LIMIT 20;
```

Summarizes route activity by minute, including event counts, observed vehicles, average speed, and invalid-event counts.

## Run Automated Tests

Ensure the Docker Compose services are running, then execute:

```bash
python -m unittest discover -s tests -v
```

The test suite currently contains **14 tests**:

- 8 unit tests for the event contract and validation rules.
- 2 integration tests covering valid event delivery, duplicate protection, and invalid-speed handling.
- 4 analytics-view tests checking source-table consistency, invalid-event counts, operational route filtering, and metric consistency.

The current test suite passes in the local development environment. Run it again after making code or SQL changes.

## Database Connection

For a host SQL client:

- **Host:** `localhost`
- **Port:** `5432`
- **Database:** `transport`
- **User:** `transport`
- **Password:** `transport_dev_only` (local development only)

Do not reuse the development password in a production environment.

## Resource Configuration

The Compose file sets memory limits for the main services:

| Service | Memory limit |
|---|---:|
| Kafka | 1,200 MB |
| PostgreSQL | 512 MB |
| Producer | 256 MB |
| Spark | 2,500 MB |

Docker itself also requires memory. If the machine becomes sluggish, reduce `EVENTS_PER_SECOND` in `compose.yaml` or stop unrelated containers.

## Stop and Reset

Stop the containers while preserving persisted data:

```bash
docker compose down
```

To remove the project's Kafka, PostgreSQL, and Spark checkpoint volumes:

```bash
docker compose down -v
```

**Warning:** The second command deletes data stored in those Docker volumes. Do not use it unless you intentionally want to reset the environment.

## Implementation Notes and Limitations

- This is a local learning and portfolio environment, not a highly available production deployment.
- Kafka runs as a single broker with a replication factor of one.
- PostgreSQL uses `ON CONFLICT (event_id) DO NOTHING` to prevent duplicate primary keys during retries.
- Spark checkpointing persists streaming progress in a Docker volume.
- Invalid events are retained for auditing rather than silently discarded.
- Synthetic invalid-speed events are generated to exercise data-quality validation.
- Late-event handling with event-time watermarks is not implemented.
- The local database password is for development only.
- The current test suite does not constitute a complete failure-recovery or production-load test.

The Spark Kafka connector is pinned to Spark 3.5.8 and Scala 2.12 compatibility:

`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8`

## Project Roadmap

- [x] Build a synthetic GPS producer and Kafka topic.
- [x] Process Kafka events with Spark Structured Streaming.
- [x] Persist events in PostgreSQL.
- [x] Validate records and flag invalid events.
- [x] Prevent duplicate inserts using the event primary key.
- [x] Build SQL analytics views for data quality, vehicles, routes, and minute-level route activity.
- [x] Add automated tests for event validation, pipeline integration, and analytics views.
- [ ] Add explicit late-event handling and streaming recovery tests.
- [ ] Add Airflow orchestration and dbt analytics models.
- [ ] Add a dashboard, architecture diagram, and benchmark results.

## Future Improvements

Potential extensions include secrets management, a schema registry, a dead-letter queue, event-time watermarking, service monitoring, multi-broker Kafka deployment, Airflow orchestration, dbt transformations, and a monitoring dashboard.
