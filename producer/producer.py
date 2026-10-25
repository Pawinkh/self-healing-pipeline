"""
Replays partitioned CSV tables into Kafka as if they were live feeds.

Deliberately schema-agnostic: it doesn't know or care what columns the CSV
has, so swapping datasets doesn't require schema changes here. Each
row is wrapped in an envelope with batch, table, and ingestion metadata.

Streams row-by-row via generators so that large multi-gigabyte or partitioned
datasets can be replayed with minimal memory consumption.
"""

import csv
import glob
import itertools
import json
import os
import re
import time
import uuid
from datetime import datetime, timezone

try:
    # pyrefly: ignore [missing-import]
    from confluent_kafka import Producer
except ImportError:
    Producer = None

DATA_DIR = os.environ.get("DATA_DIR", "/data" if os.path.exists("/data") else "data")
TABLES_ENV = os.environ.get("TABLES", "Crash1,Crash2,Crash3")
TABLES = [t.strip() for t in TABLES_ENV.split(",") if t.strip()]

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_PREFIX = os.environ.get("KAFKA_TOPIC_PREFIX", "crashes")
TOPIC_PER_TABLE = os.environ.get("TOPIC_PER_TABLE", "true").lower() == "true"
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crashes.raw")

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "2000"))
BATCH_INTERVAL_SECONDS = float(os.environ.get("BATCH_INTERVAL_SECONDS", "3"))
LOOP_FOREVER = os.environ.get("LOOP_FOREVER", "true").lower() == "true"
STREAMING_MODE = os.environ.get("STREAMING_MODE", "interleaved").lower()


def natural_sort_key(s: str):
    """Sort strings with embedded numbers naturally (e.g. 1, 2, 10)."""
    return [int(text) if text.isdigit() else text.lower() for text in re.split(r"(\d+)", s)]


def find_table_files(data_dir: str, table_name: str) -> list[str]:
    """
    Find all partition CSV files for a given table.
    Supports either:
      1. data_dir/table_name/*.csv (organized in subdirectories)
      2. data_dir/table_name-*.csv or data_dir/table_name*.csv (flat in data_dir)
    """
    table_subdir = os.path.join(data_dir, table_name)
    files = []
    if os.path.isdir(table_subdir):
        files = glob.glob(os.path.join(table_subdir, "*.csv"))

    if not files:
        # Fallback to flat files in data_dir matching table name prefix
        files = glob.glob(os.path.join(data_dir, f"{table_name}-*.csv"))
    if not files:
        files = glob.glob(os.path.join(data_dir, f"{table_name}*.csv"))

    files.sort(key=natural_sort_key)
    return files


def get_topic_for_table(table_name: str) -> str:
    """Return the target Kafka topic for a given table."""
    if TOPIC_PER_TABLE:
        return f"{KAFKA_TOPIC_PREFIX}.{table_name.lower()}"
    return KAFKA_TOPIC


def row_generator(table_name: str, file_paths: list[str], loop_forever: bool):
    """
    Memory-efficient generator that yields rows one by one from partitioned CSVs.
    Streams sequentially through all partition files, then loops if loop_forever is True.
    """
    iteration = 0
    while True:
        iteration += 1
        for file_path in file_paths:
            print(f"[{table_name}] Streaming partition '{os.path.basename(file_path)}' (cycle {iteration})")
            with open(file_path, "r", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    yield row

        if not loop_forever:
            break


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")


def stream_interleaved(producer: Producer, table_files: dict[str, list[str]]):
    """
    Streams batches round-robin across tables so all tables publish concurrently.
    """
    print(f"Starting interleaved streaming across tables: {list(table_files.keys())}")
    generators = {
        tbl: row_generator(tbl, files, LOOP_FOREVER)
        for tbl, files in table_files.items()
    }

    batch_counters = {tbl: 0 for tbl in table_files}
    active_tables = set(table_files.keys())

    while active_tables:
        tables_to_remove = set()
        for tbl in list(active_tables):
            gen = generators[tbl]
            batch = list(itertools.islice(gen, BATCH_SIZE))

            if not batch:
                print(f"[{tbl}] Reached end of dataset.")
                tables_to_remove.add(tbl)
                continue

            batch_counters[tbl] += 1
            batch_id = batch_counters[tbl]
            topic = get_topic_for_table(tbl)

            for row in batch:
                envelope = {
                    "record_id": str(uuid.uuid4()),
                    "batch_id": batch_id,
                    "table_name": tbl,
                    "ingestion_ts": datetime.now(timezone.utc).isoformat(),
                    "payload": row,
                }
                producer.produce(
                    topic,
                    key=envelope["record_id"],
                    value=json.dumps(envelope),
                    callback=delivery_report,
                )

            producer.poll(0)
            print(f"[{tbl}] Batch {batch_id}: published {len(batch)} rows to '{topic}'")

        producer.flush()
        active_tables -= tables_to_remove

        if active_tables:
            time.sleep(BATCH_INTERVAL_SECONDS)


def stream_sequential(producer: Producer, table_files: dict[str, list[str]]):
    """
    Streams all partition files for one table before moving to the next.
    """
    print(f"Starting sequential streaming for tables: {list(table_files.keys())}")
    for tbl, files in table_files.items():
        topic = get_topic_for_table(tbl)
        gen = row_generator(tbl, files, LOOP_FOREVER)
        batch_id = 0

        while True:
            batch = list(itertools.islice(gen, BATCH_SIZE))
            if not batch:
                print(f"[{tbl}] Completed stream.")
                break

            batch_id += 1
            for row in batch:
                envelope = {
                    "record_id": str(uuid.uuid4()),
                    "batch_id": batch_id,
                    "table_name": tbl,
                    "ingestion_ts": datetime.now(timezone.utc).isoformat(),
                    "payload": row,
                }
                producer.produce(
                    topic,
                    key=envelope["record_id"],
                    value=json.dumps(envelope),
                    callback=delivery_report,
                )

            producer.poll(0)
            producer.flush()
            print(f"[{tbl}] Batch {batch_id}: published {len(batch)} rows to '{topic}'")
            time.sleep(BATCH_INTERVAL_SECONDS)


def main():
    table_files: dict[str, list[str]] = {}
    for tbl in TABLES:
        files = find_table_files(DATA_DIR, tbl)
        if not files:
            raise FileNotFoundError(
                f"No CSV files found for table '{tbl}' in '{DATA_DIR}'."
            )
        table_files[tbl] = files
        print(f"Found table '{tbl}' with {len(files)} partition(s): {[os.path.basename(f) for f in files]}")

    if Producer is None:
        raise ImportError(
            "confluent_kafka is required to run the producer. "
            "Install it with 'pip install confluent-kafka' or run within Docker."
        )

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    if STREAMING_MODE == "sequential":
        stream_sequential(producer, table_files)
    else:
        stream_interleaved(producer, table_files)


if __name__ == "__main__":
    main()
