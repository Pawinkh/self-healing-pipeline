"""
Replays a static CSV into Kafka as if it were a live feed.

Deliberately schema-agnostic: it doesn't know or care what columns the CSV
has, so swapping datasets later doesn't require touching this file. Each
row is wrapped in an envelope with batch/ingestion metadata, which is what
a future triage step would use for deduping and severity classification.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
# pyrefly: ignore [missing-import]
from confluent_kafka import Producer

CSV_PATH = os.environ.get("CSV_PATH", "/data/mock_cars.csv")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "cars.raw")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
BATCH_INTERVAL_SECONDS = float(os.environ.get("BATCH_INTERVAL_SECONDS", "5"))
LOOP_FOREVER = os.environ.get("LOOP_FOREVER", "true").lower() == "true"


def load_dataset() -> pd.DataFrame:
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(
            f"No CSV found at {CSV_PATH}. Download the dataset and place it "
            f"there first -- see README.md."
        )
    # dtype=str keeps every column exactly as it appears in the file. No
    # type inference here on purpose: parsing/cleaning belongs downstream,
    # not in the thing simulating the raw feed.
    return pd.read_csv(CSV_PATH, dtype=str).fillna("")


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")


def main():
    df = load_dataset()
    records = df.to_dict(orient="records")
    print(f"Loaded {len(records)} rows from {CSV_PATH}")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    batch_id = 0
    idx = 0
    while True:
        batch_id += 1
        batch = records[idx : idx + BATCH_SIZE]

        if not batch:
            if not LOOP_FOREVER:
                print("Reached end of dataset, LOOP_FOREVER=false, stopping.")
                break
            idx = 0
            continue

        for row in batch:
            envelope = {
                "record_id": str(uuid.uuid4()),
                "batch_id": batch_id,
                "ingestion_ts": datetime.now(timezone.utc).isoformat(),
                "payload": row,
            }
            producer.produce(
                KAFKA_TOPIC,
                key=envelope["record_id"],
                value=json.dumps(envelope),
                callback=delivery_report,
            )

        producer.flush()
        print(f"Batch {batch_id}: published {len(batch)} rows to '{KAFKA_TOPIC}'")

        idx += BATCH_SIZE
        time.sleep(BATCH_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
