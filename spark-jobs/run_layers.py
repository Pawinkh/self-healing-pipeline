"""
Run several pipeline layers as streaming queries inside ONE Spark driver.

Each layer is light (a few hundred rows per micro-batch), so giving every
layer its own container + JVM spent most of the stack's memory on JVM
overhead. Layers start in the order given: each one's start() blocks in
wait_for_delta_table() until its upstream table exists, which the query
started just before it (in this same process) creates.

Usage:
    python run_layers.py bronze silver1
    python run_layers.py silver2 gold

Each <layer>_job.py still runs on its own too (`python silver2_job.py`).
"""

import os
import sys

import bronze_job
import gold_job
import silver1_job
import silver2_job
from common import build_spark

LAYER_STARTERS = {
    "bronze": bronze_job.start,
    "silver1": silver1_job.start,
    "silver2": silver2_job.start,
    "gold": gold_job.start,
}


def main(layers):
    unknown = [name for name in layers if name not in LAYER_STARTERS]
    if not layers or unknown:
        sys.exit(
            f"Usage: python run_layers.py <layer> [<layer> ...] "
            f"(layers: {', '.join(LAYER_STARTERS)}; unknown: {unknown})"
        )

    spark = build_spark(
        "Pipeline-" + "-".join(layers),
        driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "1g"),
    )

    upstream = None
    for name in layers:
        upstream = LAYER_STARTERS[name](spark, upstream=upstream)

    # Raises as soon as any query fails, so `restart: on-failure` restarts the
    # whole container and every query resumes from its own checkpoint.
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    from qwen_classifier.integration import run_job
    run_job(lambda: main(sys.argv[1:]), job_name="Pipeline-" + "-".join(sys.argv[1:]))
