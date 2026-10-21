"""
Gold: business-ready fields, via SQL.

Reads from Silver 2, registers a temp view 'silver2', and runs
GOLD_TRANSFORM_SQL to produce the business-ready Gold Delta table.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from common import build_spark, wait_for_delta_table

SILVER2_PATH = os.environ.get("SILVER2_PATH", "/data/delta/silver2")
GOLD_PATH = os.environ.get("GOLD_PATH", "/data/delta/gold")
CHECKPOINT_PATH = os.environ.get(
    "GOLD_CHECKPOINT_PATH", "/data/checkpoints/gold"
)

# Business-level transform SQL: the columns an analyst reports on, typed for
# aggregation (Silver keeps counts/coordinates as strings).
#
# Deliberately left out (still available in Silver 2):
#   - pipeline metadata: batch_id, table_name, ingestion_ts, topic, kafka_timestamp
#   - admin / redundant: idot_control_no, report_type, date_police_notified,
#     street_no, location (same point as latitude/longitude)
#   - rarely populated (<2% of rows, or always 0): crash_date_est_i, photos_taken_i,
#     statements_taken_i, work_zone_i, work_zone_type, workers_present_i, dooring_i,
#     lane_cnt, injuries_unknown
#
# crash_month / crash_day_of_week are kept even though they look derivable from
# crash_date: they come straight from the source, while crash_date is re-parsed in
# Silver 2 and can disagree with them.
GOLD_TRANSFORM_SQL = """
SELECT
    -- Keys: crash_record_id is the business key; record_id traces back through Silver/Bronze
    crash_record_id,
    record_id,

    -- When
    CAST(crash_date AS DATE) AS crash_date,
    CAST(CAST(crash_hour AS DOUBLE) AS INT) AS crash_hour,
    crash_day_of_week,
    crash_month,

    -- Where
    beat_of_occurrence,
    street_direction,
    street_name,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,

    -- Road & environment
    posted_speed_limit,
    traffic_control_device,
    device_condition,
    weather_condition,
    lighting_condition,
    roadway_surface_cond,
    road_defect,
    trafficway_type,
    alignment,
    intersection_related_i,

    -- Crash
    crash_type,
    first_crash_type,
    prim_contributory_cause,
    sec_contributory_cause,
    CAST(num_units AS INT) AS num_units,
    hit_and_run_i,
    private_property_i,
    damage,

    -- Severity
    most_severe_injury,
    CAST(CAST(injuries_total AS DOUBLE) AS INT) AS injuries_total,
    CAST(CAST(injuries_fatal AS DOUBLE) AS INT) AS injuries_fatal,
    CAST(CAST(injuries_incapacitating AS DOUBLE) AS INT) AS injuries_incapacitating,
    CAST(CAST(injuries_non_incapacitating AS DOUBLE) AS INT) AS injuries_non_incapacitating,
    CAST(CAST(injuries_reported_not_evident AS DOUBLE) AS INT) AS injuries_reported_not_evident,
    CAST(CAST(injuries_no_indication AS DOUBLE) AS INT) AS injuries_no_indication
FROM silver2
"""


def start(spark: SparkSession, upstream: Optional[StreamingQuery] = None) -> StreamingQuery:
    """Start the Gold query on `spark`; `upstream` is the Silver 2 query if it shares this driver."""
    wait_for_delta_table(SILVER2_PATH, upstream=upstream)

    silver2_stream = spark.readStream.format("delta").load(SILVER2_PATH)
    silver2_stream.createOrReplaceTempView("silver2")

    gold = spark.sql(GOLD_TRANSFORM_SQL)

    query = (
        gold.writeStream.format("delta")
        .queryName("gold")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(GOLD_PATH)
    )

    print(f"[gold] Streaming {SILVER2_PATH} -> {GOLD_PATH}")
    return query


def main():
    start(build_spark("GoldCrashTransform")).awaitTermination()


if __name__ == "__main__":
    main()
