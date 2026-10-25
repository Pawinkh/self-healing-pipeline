"""Kafka -> Neo4j lineage writer (spec KG1).

Reads the OpenLineage events the Spark listener publishes to Kafka, keeps the
ones that describe pipeline table writes (normalize.py) and merges them into the
graph (graph.py). Offsets are committed only after Neo4j accepted the event, and
every write is a MERGE, so a crash or redelivery never duplicates anything.
"""
import json
import os
import time

from confluent_kafka import Consumer, KafkaError
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError, ServiceUnavailable, TransientError

from graph import write_update
from normalize import DEFAULT_DELTA_ROOT, normalize

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("OPENLINEAGE_TOPIC", "openlineage.events")
GROUP_ID = os.environ.get("LINEAGE_CONSUMER_GROUP", "lineage-writer")
DELTA_ROOT = os.environ.get("DELTA_ROOT", DEFAULT_DELTA_ROOT)

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "selfhealing")

MAX_BACKOFF_SECONDS = 30


def _write_with_retry(driver, update) -> None:
    """Neo4j being down or busy must not lose lineage: wait and retry the same event."""
    delay = 1
    while True:
        try:
            write_update(driver, update)
            return
        except (ServiceUnavailable, TransientError) as exc:
            print(f"[lineage] Neo4j unavailable ({type(exc).__name__}); retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, MAX_BACKOFF_SECONDS)


def main() -> None:
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    print(f"[lineage] Consuming {TOPIC} from {KAFKA_BOOTSTRAP} -> {NEO4J_URI} (delta root {DELTA_ROOT})")

    seen = written = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                # The topic does not exist until the first Spark job publishes to it.
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"[lineage] Kafka error: {msg.error()}")
                continue

            seen += 1
            try:
                update = normalize(json.loads(msg.value()), DELTA_ROOT)
            except (ValueError, TypeError):
                print(f"[lineage] Skipping unparseable event at offset {msg.offset()}")
                update = None

            if update is not None:
                try:
                    _write_with_retry(driver, update)
                except Neo4jError as exc:  # a bad event must not wedge the stream
                    print(f"[lineage] Skipping event at offset {msg.offset()}: {exc}")
                else:
                    written += 1
                    if written % 25 == 1:
                        print(
                            f"[lineage] {update.job_name} {update.status} "
                            f"({written} of {seen} events written)"
                        )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        driver.close()


if __name__ == "__main__":
    main()
