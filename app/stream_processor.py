"""Read GPS events from Kafka, validate and write them to PostgreSQL."""
import os
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp, struct, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, BooleanType
)
from event_contract import validate_event

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "vehicle-gps")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")
PG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "transport"),
    "user": os.getenv("POSTGRES_USER", "transport"),
    "password": os.getenv("POSTGRES_PASSWORD", "transport_dev_only"),
}

schema = StructType([
    StructField("event_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("event_time", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("route_id", StringType()),
    StructField("sequence", LongType()),
])

def write_batch(batch_df, batch_id):
    """Idempotent upsert keyed by event_id; safe for Spark batch retries."""
    rows = batch_df.collect()
    if not rows:
        print(f"Batch {batch_id}: no rows", flush=True)
        return

    conn = psycopg2.connect(**PG)
    try:
        with conn, conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO gps_events
                    (event_id, vehicle_id, event_time, latitude, longitude,
                     speed_kmh, route_id, sequence, is_valid, ingested_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    (r.event_id, r.vehicle_id, r.event_time, r.latitude,
                     r.longitude, r.speed_kmh, r.route_id, r.sequence, r.is_valid)
                )
        print(f"Batch {batch_id}: wrote {len(rows)} rows", flush=True)
    finally:
        conn.close()

def main():
    spark = (
        SparkSession.builder
        .appName("transport-gps-stream")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.addPyFile("/app/app/event_contract.py")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(from_json(col("json"), schema).alias("d"))
        .select("d.*")
        .withColumn("event_time", to_timestamp("event_time"))
        .withColumn("ingested_at", current_timestamp())
    )

    # Keep invalid records for audit using the shared event contract.
    validate_event_udf = udf(validate_event, BooleanType())

    checked = parsed.withColumn(
        "is_valid",
        validate_event_udf(
            struct(*[col(field) for field in schema.fieldNames()])
        )
    )

    query = (
        checked.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start()
    )
    print(f"Spark streaming started: Kafka topic={TOPIC}", flush=True)
    query.awaitTermination()

if __name__ == "__main__":
    main()
