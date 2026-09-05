"""
Bronze: Kafka -> Delta, untouched.

Keeps Kafka's value as a plain string plus Kafka's own metadata. No JSON
parsing, no typing -- that's Silver's job. This preserves the raw envelope
exactly as the producer sent it, which matters if a downstream parsing
failure ever needs diagnosing: the original value has to still exist
somewhere.
"""

import os

from pyspark.sql.functions import current_timestamp

from common import build_spark

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cars.raw")
DELTA_TABLE_PATH = os.environ.get("DELTA_TABLE_PATH", "/data/delta/bronze")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/data/checkpoints/bronze")


def main():
    spark = build_spark("BronzeLanding")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
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
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(DELTA_TABLE_PATH)
    )

    print(f"[bronze] Streaming '{KAFKA_TOPIC}' -> {DELTA_TABLE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
