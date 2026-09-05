"""
Silver: parse + clean, via SQL.

Two parts, kept deliberately separate:

1. PAYLOAD_SCHEMA / ENVELOPE_SCHEMA -- tells Spark what shape to expect
   inside Bronze's raw JSON string. This is the one place in the whole
   pipeline that has to know the mock dataset's column names. When the
   real dataset swaps in, this schema is the only thing that might need
   updating (if column names change) -- bronze_job.py and the producer
   never need to change at all.

2. SILVER_TRANSFORM_SQL -- the actual cleaning logic, as one plain SQL
   string. This is what you'll be editing/replacing. It runs via
   `spark.sql(...)` against a temp view, so any valid Spark SQL works here.
"""

import os

from pyspark.sql.functions import col, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common import build_spark, wait_for_delta_table

BRONZE_PATH = os.environ.get("BRONZE_PATH", "/data/delta/bronze")
SILVER_PATH = os.environ.get("SILVER_PATH", "/data/delta/silver_cars")
CHECKPOINT_PATH = os.environ.get(
    "CHECKPOINT_PATH", "/data/checkpoints/silver_cars"
)

# Matches the mock CSV's header exactly. Swap in the real dataset by
# pointing the producer at a new CSV_PATH -- if its columns are named the
# same way, nothing here needs to change.
PAYLOAD_SCHEMA = StructType(
    [
        StructField("Company Names", StringType()),
        StructField("Cars Names", StringType()),
        StructField("Engines", StringType()),
        StructField("CC/Battery Capacity", StringType()),
        StructField("HorsePower", StringType()),
        StructField("Total Speed", StringType()),
        StructField("Performance(0 - 100)KM/H", StringType()),
        StructField("Cars Prices", StringType()),
        StructField("Fuel Types", StringType()),
        StructField("Seats", StringType()),
        StructField("Torque", StringType()),
    ]
)

ENVELOPE_SCHEMA = StructType(
    [
        StructField("record_id", StringType()),
        StructField("batch_id", LongType()),
        StructField("ingestion_ts", StringType()),
        StructField("payload", PAYLOAD_SCHEMA),
    ]
)

# Initial cleaning pass -- adjust freely. A couple of things worth noting
# for when you extend this:
#   - regexp_extract returns '' (not NULL) on no match, so numeric CASTs
#     of a non-match come out NULL, which is what we want.
#   - `Cars Prices` has one deliberately-messy value in the mock data
#     ("unknown") to prove this doesn't crash the batch -- it just nulls.
SILVER_TRANSFORM_SQL = r"""
SELECT
    record_id,
    batch_id,
    ingestion_ts,
    trim(`Company Names`) AS company_name,
    trim(`Cars Names`)    AS car_name,

    CAST(regexp_extract(HorsePower, '([0-9]+)', 1) AS INT) AS horsepower_hp,

    CASE
        WHEN `CC/Battery Capacity` LIKE '%kWh%' THEN NULL
        ELSE CAST(regexp_extract(`CC/Battery Capacity`, '([0-9]+)', 1) AS INT)
    END AS engine_displacement_cc,

    CASE
        WHEN `CC/Battery Capacity` LIKE '%kWh%'
            THEN CAST(regexp_extract(`CC/Battery Capacity`, '([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)
        ELSE NULL
    END AS battery_capacity_kwh,

    CASE
        WHEN lower(`Fuel Types`) LIKE '%electric%' THEN 'EV'
        WHEN lower(`Fuel Types`) LIKE '%hybrid%'   THEN 'Hybrid'
        ELSE 'ICE'
    END AS powertrain_type,

    CAST(regexp_extract(`Total Speed`, '([0-9]+)', 1) AS INT) AS top_speed_kmh,

    CAST(regexp_extract(`Performance(0 - 100)KM/H`, '([0-9]+\\.?[0-9]*)', 1) AS DOUBLE)
        AS accel_0_100_sec,

    CAST(regexp_replace(regexp_extract(`Cars Prices`, '([0-9,]+)', 1), ',', '') AS DOUBLE)
        AS price_usd,

    CAST(regexp_extract(Seats, '([0-9]+)', 1) AS INT) AS seats,

    CAST(regexp_extract(Torque, '([0-9]+)', 1) AS INT) AS torque_nm,

    `Fuel Types` AS fuel_type_raw

FROM bronze_flat
"""


def main():
    spark = build_spark("SilverCarsTransform")

    wait_for_delta_table(BRONZE_PATH)

    bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

    flattened = bronze_stream.select(
        from_json(col("raw_value"), ENVELOPE_SCHEMA).alias("envelope")
    ).select("envelope.record_id", "envelope.batch_id", "envelope.ingestion_ts", "envelope.payload.*")

    flattened.createOrReplaceTempView("bronze_flat")
    silver = spark.sql(SILVER_TRANSFORM_SQL)

    query = (
        silver.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(SILVER_PATH)
    )

    print(f"[silver] Streaming {BRONZE_PATH} -> {SILVER_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
