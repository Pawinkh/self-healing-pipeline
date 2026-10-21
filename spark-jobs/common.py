"""
Shared helpers for the bronze/silver/gold jobs.

Kept deliberately small: a Spark session builder (Delta + Kafka packages
wired in once) and a guard that waits for an upstream Delta table to exist
before a downstream job tries to stream-read it. On a cold `docker compose
up`, silver_job and gold_job can start before their upstream table has been
created by the first micro-batch of the job before them -- this avoids a
crash-loop while that happens.
"""

import os
import time
from typing import Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

# Must match the pinned pyspark version/Scala build in requirements.txt.
SCALA_VERSION = "2.12"
SPARK_VERSION = "3.5.3"


def build_spark(app_name: str, driver_memory: str = "1g") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
        ],
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def wait_for_delta_table(
    path: str, poll_seconds: int = 5, upstream: Optional[StreamingQuery] = None
) -> None:
    """
    Block until `path` looks like an initialized Delta table.

    `upstream` is the query that will create the table when it runs in this
    same driver (see run_layers.py). If that query dies first, raise instead
    of waiting forever, so the container exits and gets restarted.
    """
    log_marker = os.path.join(path, "_delta_log")
    waited = 0
    while not os.path.isdir(log_marker):
        if upstream is not None and not upstream.isActive:
            raise RuntimeError(
                f"Upstream query '{upstream.name or upstream.id}' stopped before "
                f"creating {path}"
            ) from upstream.exception()
        print(f"Waiting for upstream Delta table at {path} to exist "
              f"(waited {waited}s)...")
        time.sleep(poll_seconds)
        waited += poll_seconds
