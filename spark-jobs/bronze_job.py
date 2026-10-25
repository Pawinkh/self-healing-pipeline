"""
Bronze: Kafka -> Delta, untouched.

Keeps Kafka's value as a plain string plus Kafka's own metadata. No JSON
parsing, no typing -- that's Silver's job. This preserves the raw envelope
exactly as the producer sent it, which matters if a downstream parsing
failure ever needs diagnosing: the original value has to still exist
somewhere.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.streaming import StreamingQuery

from common import build_spark

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPICS = os.environ.get(
    "KAFKA_TOPICS",
    os.environ.get("KAFKA_TOPIC", "crashes.crash1,crashes.crash2,crashes.crash3"),
)
BRONZE_PATH = os.environ.get("BRONZE_PATH", "/data/delta/bronze")
CHECKPOINT_PATH = os.environ.get("BRONZE_CHECKPOINT_PATH", "/data/checkpoints/bronze")


def start(spark: SparkSession, upstream: Optional[StreamingQuery] = None) -> StreamingQuery:
    """Start the Bronze query on `spark`. Bronze reads Kafka, so `upstream` is unused."""
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    landed = raw_stream.selectExpr(
        "CAST(key AS STRING) as kafka_key",
        "CAST(value AS STRING) as raw_value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp",
    ).withColumn("ingested_at", current_timestamp())

    query = (
        landed.writeStream.format("delta")
        .queryName("bronze")
        .trigger(processingTime="10 seconds") #Stop RAM from exploding
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(BRONZE_PATH)
    )

    print(f"[bronze] Streaming '{KAFKA_TOPICS}' -> {BRONZE_PATH}")
    return query


def main():
    start(build_spark("BronzeLanding")).awaitTermination()


if __name__ == "__main__":
    main()
