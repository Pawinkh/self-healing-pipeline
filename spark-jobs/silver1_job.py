"""
Silver 1: Parse JSON envelope + Initial formatting & cleaning via SQL.

Reads raw JSON strings from Bronze, unpacks the envelope and payload schema,
and registers a temp view 'bronze_flat'. Runs SILVER1_TRANSFORM_SQL which can
be modified for formatting and cleaning logic.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common import build_spark, wait_for_delta_table

BRONZE_PATH = os.environ.get("BRONZE_PATH", "/data/delta/bronze")
SILVER1_PATH = os.environ.get("SILVER1_PATH", "/data/delta/silver1")
CHECKPOINT_PATH = os.environ.get(
    "SILVER1_CHECKPOINT_PATH", "/data/checkpoints/silver1"
)

CRASH_COLUMNS = [
    "crash_record_id",
    "crash_date",
    "posted_speed_limit",
    "traffic_control_device",
    "device_condition",
    "weather_condition",
    "lighting_condition",
    "first_crash_type",
    "trafficway_type",
    "alignment",
    "roadway_surface_cond",
    "road_defect",
    "report_type",
    "crash_type",
    "private_property_i",
    "hit_and_run_i",
    "damage",
    "date_police_notified",
    "prim_contributory_cause",
    "sec_contributory_cause",
    "street_no",
    "street_direction",
    "street_name",
    "beat_of_occurrence",
    "num_units",
    "crash_month",
    "most_severe_injury",
    "injuries_total",
    "injuries_fatal",
    "injuries_incapacitating",
    "injuries_non_incapacitating",
    "injuries_reported_not_evident",
    "injuries_no_indication",
    "injuries_unknown",
    "crash_hour",
    "crash_day_of_week",
    "idot_control_no",
    "latitude",
    "longitude",
    "location",
    "intersection_related_i",
    "crash_date_est_i",
    "photos_taken_i",
    "statements_taken_i",
    "work_zone_i",
    "work_zone_type",
    "lane_cnt",
    "workers_present_i",
    "dooring_i",
]

PAYLOAD_SCHEMA = StructType([StructField(c, StringType(), True) for c in CRASH_COLUMNS])

ENVELOPE_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), True),
        StructField("batch_id", LongType(), True),
        StructField("table_name", StringType(), True),
        StructField("ingestion_ts", StringType(), True),
        StructField("payload", PAYLOAD_SCHEMA, True),
    ]
    + [StructField(c, StringType(), True) for c in CRASH_COLUMNS]
)

# SQL for Silver 1 (Formatting & Cleaning).
# Modify this query to apply any custom type casts, trims, or cleanups.
SILVER1_TRANSFORM_SQL = """
SELECT
    record_id,
    batch_id,
    table_name,
    ingestion_ts,
    topic,
    kafka_timestamp,
    NULLIF(TRIM(crash_record_id), '') AS crash_record_id,
    NULLIF(TRIM(crash_date), '') AS crash_date,
    NULLIF(TRIM(posted_speed_limit), '') AS posted_speed_limit,
    NULLIF(UPPER(TRIM(traffic_control_device)), '') AS traffic_control_device,
    NULLIF(UPPER(TRIM(device_condition)), '') AS device_condition,
    NULLIF(UPPER(TRIM(weather_condition)), '') AS weather_condition,
    NULLIF(UPPER(TRIM(lighting_condition)), '') AS lighting_condition,
    NULLIF(UPPER(TRIM(first_crash_type)), '') AS first_crash_type,
    NULLIF(UPPER(TRIM(trafficway_type)), '') AS trafficway_type,
    NULLIF(UPPER(TRIM(alignment)), '') AS alignment,
    NULLIF(UPPER(TRIM(roadway_surface_cond)), '') AS roadway_surface_cond,
    NULLIF(UPPER(TRIM(road_defect)), '') AS road_defect,
    NULLIF(UPPER(TRIM(report_type)), '') AS report_type,
    NULLIF(UPPER(TRIM(crash_type)), '') AS crash_type,
    NULLIF(UPPER(TRIM(private_property_i)), '') AS private_property_i,
    NULLIF(UPPER(TRIM(hit_and_run_i)), '') AS hit_and_run_i,
    NULLIF(UPPER(TRIM(damage)), '') AS damage,
    NULLIF(TRIM(date_police_notified), '') AS date_police_notified,
    NULLIF(LOWER(TRIM(prim_contributory_cause)), '') AS prim_contributory_cause,
    NULLIF(LOWER(TRIM(sec_contributory_cause)), '') AS sec_contributory_cause,
    NULLIF(TRIM(street_no), '') AS street_no,
    NULLIF(UPPER(TRIM(street_direction)), '') AS street_direction,
    NULLIF(INITCAP(TRIM(street_name)), '') AS street_name,
    NULLIF(TRIM(beat_of_occurrence), '') AS beat_of_occurrence,
    NULLIF(TRIM(num_units), '') AS num_units,
    NULLIF(TRIM(crash_month), '') AS crash_month,
    NULLIF(UPPER(TRIM(most_severe_injury)), '') AS most_severe_injury,
    NULLIF(TRIM(injuries_total), '') AS injuries_total,
    NULLIF(TRIM(injuries_fatal), '') AS injuries_fatal,
    NULLIF(TRIM(injuries_incapacitating), '') AS injuries_incapacitating,
    NULLIF(TRIM(injuries_non_incapacitating), '') AS injuries_non_incapacitating,
    NULLIF(TRIM(injuries_reported_not_evident), '') AS injuries_reported_not_evident,
    NULLIF(TRIM(injuries_no_indication), '') AS injuries_no_indication,
    NULLIF(TRIM(injuries_unknown), '') AS injuries_unknown,
    NULLIF(TRIM(crash_hour), '') AS crash_hour,
    NULLIF(TRIM(crash_day_of_week), '') AS crash_day_of_week,
    NULLIF(TRIM(idot_control_no), '') AS idot_control_no,
    NULLIF(TRIM(latitude), '') AS latitude,
    NULLIF(TRIM(longitude), '') AS longitude,
    NULLIF(TRIM(location), '') AS location,
    NULLIF(UPPER(TRIM(intersection_related_i)), '') AS intersection_related_i,
    NULLIF(UPPER(TRIM(crash_date_est_i)), '') AS crash_date_est_i,
    NULLIF(UPPER(TRIM(photos_taken_i)), '') AS photos_taken_i,
    NULLIF(UPPER(TRIM(statements_taken_i)), '') AS statements_taken_i,
    NULLIF(UPPER(TRIM(work_zone_i)), '') AS work_zone_i,
    NULLIF(UPPER(TRIM(work_zone_type)), '') AS work_zone_type,
    NULLIF(TRIM(lane_cnt), '') AS lane_cnt,
    NULLIF(UPPER(TRIM(workers_present_i)), '') AS workers_present_i,
    NULLIF(UPPER(TRIM(dooring_i)), '') AS dooring_i
FROM bronze_flat
"""


def start(spark: SparkSession, upstream: Optional[StreamingQuery] = None) -> StreamingQuery:
    """Start the Silver 1 query on `spark`; `upstream` is the Bronze query if it shares this driver."""
    wait_for_delta_table(BRONZE_PATH, upstream=upstream)

    bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

    flattened = bronze_stream.select(
        col("topic"),
        col("kafka_timestamp"),
        from_json(col("raw_value"), ENVELOPE_SCHEMA).alias("env"),
    ).select(
        col("topic"),
        col("kafka_timestamp"),
        col("env.record_id").alias("record_id"),
        col("env.batch_id").alias("batch_id"),
        col("env.table_name").alias("table_name"),
        col("env.ingestion_ts").alias("ingestion_ts"),
        *[
            coalesce(col(f"env.payload.{c}"), col(f"env.{c}")).alias(c)
            for c in CRASH_COLUMNS
        ],
    )

    flattened.createOrReplaceTempView("bronze_flat")
    silver1 = spark.sql(SILVER1_TRANSFORM_SQL)

    query = (
        silver1.writeStream.format("delta")
        .queryName("silver1")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(SILVER1_PATH)
    )

    print(f"[silver1] Streaming {BRONZE_PATH} -> {SILVER1_PATH}")
    return query


def main():
    start(build_spark("Silver1CleanTransform")).awaitTermination()


if __name__ == "__main__":
    main()
