"""
Gold: business-ready fields, via SQL.

Currently a straight SELECT from one Silver table. When you're ready to
merge two Silver tables (per the plan), this is the file to change: add a
second `spark.readStream...createOrReplaceTempView(...)` for the other
Silver table, and turn GOLD_TRANSFORM_SQL into a JOIN between the two
views instead of a plain SELECT. Streaming-to-streaming joins need a
watermark on both sides at that point -- ask when you get there, it's a
different pattern than this stateless SELECT.
"""

import os

from common import build_spark, wait_for_delta_table

SILVER_PATH = os.environ.get("SILVER_PATH", "/data/delta/silver_cars")
GOLD_PATH = os.environ.get("GOLD_PATH", "/data/delta/gold_car_overview")
CHECKPOINT_PATH = os.environ.get(
    "CHECKPOINT_PATH", "/data/checkpoints/gold_car_overview"
)

# Future: JOIN against a second silver table here (e.g. regional pricing)
# instead of a plain SELECT from one.
GOLD_TRANSFORM_SQL = """
SELECT
    record_id,
    company_name,
    car_name,
    powertrain_type,
    horsepower_hp,
    price_usd,
    top_speed_kmh
FROM silver_cars
"""


def main():
    spark = build_spark("GoldCarOverview")

    wait_for_delta_table(SILVER_PATH)

    silver_stream = spark.readStream.format("delta").load(SILVER_PATH)
    silver_stream.createOrReplaceTempView("silver_cars")

    gold = spark.sql(GOLD_TRANSFORM_SQL)

    query = (
        gold.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(GOLD_PATH)
    )

    print(f"[gold] Streaming {SILVER_PATH} -> {GOLD_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
