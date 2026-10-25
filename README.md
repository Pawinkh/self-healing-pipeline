# Self-healing pipeline — Bronze/Silver1/Silver2/Gold Lakehouse

Kafka → Spark Structured Streaming jobs (Bronze, Silver1, Silver2, Gold) → Delta
Lake, running as a multi-hop pipeline: each layer streams from the previous
layer's Delta table, following the Medallion Lakehouse pattern.

```
producer --> kafka --> bronze-job --> delta/bronze
                                          |
                                          v
                                     silver1-job --> delta/silver1 (Format & Cleaning)
                                                         |
                                                         v
                                                    silver2-job --> delta/silver2 (Standardization)
                                                                        |
                                                                        v
                                                                    gold-job --> delta/gold (Analytics)
```

## Prerequisites

- Docker + Docker Compose v2
- ~4 GB RAM free (Kafka + two Spark JVMs)

## Setup

```bash
docker compose up --build
```

The producer replays the partitioned datasets (`data/Crash1`, `data/Crash2`,
`data/Crash3`) across their respective parts through Kafka topics (`crashes.crash1`,
`crashes.crash2`, `crashes.crash3`) into Bronze automatically.

First run takes a minute or two (Spark images install a JVM, connector
jars get pulled). Subsequent runs are fast.

## Watch it work

- **Kafka UI** — http://localhost:8080 — browse `crashes.crash1`, `crashes.crash2`, `crashes.crash3`
- **Producer logs** — `[Crash1] Batch N: published 50 rows to 'crashes.crash1'`
- **bronze-silver1-job logs** — one Spark driver running two queries:
  - `[bronze]` prints subscribed topics and streams micro-batches into `delta/bronze`
  - `[silver1]` unpacks JSON into 49 fields and runs formatting/cleaning SQL -> `delta/silver1`
- **silver2-gold-job logs** — one Spark driver running two queries:
  - `[silver2]` streams from Silver 1 and applies standardization SQL -> `delta/silver2`
  - `[gold]` streams from Silver 2 into business-ready table -> `delta/gold`
- **lineage-consumer logs** — `[lineage] write_gold COMPLETED (n of m events written)`
- **Neo4j browser** — http://localhost:7474 (user `neo4j`, password `NEO4J_PASSWORD`, default
  `selfhealing`) — try `MATCH p=(:Table)-[:FEEDS*]->(:Table) RETURN p`

## Verify data landed, from the host (no Spark needed)

```bash
pip install deltalake pandas
python verify_delta.py bronze
python verify_delta.py silver1
python verify_delta.py silver2
python verify_delta.py silver2_quarantine
python verify_delta.py ref_mappings
python verify_delta.py gold
```

To stop: `docker compose down`. To wipe all state and start clean:
`docker compose down && rm -rf delta checkpoints`.

## What's in each piece

- **`producer/`** — reads partitioned CSVs, publishes rows to Kafka
  in batches on an interval, wrapped in an envelope
  (`record_id`, `batch_id`, `table_name`, `ingestion_ts`, `payload`, `**row`).
- **`spark-jobs/common.py`** — shared Spark session builder and a
  wait-for-upstream-table guard, used by all jobs.
- **`spark-jobs/run_layers.py`** — runs several layers as streaming queries in
  one Spark driver (`python run_layers.py bronze silver1`). Compose uses it to
  run the four layers in two containers instead of four JVMs. Each
  `<layer>_job.py` exposes `start(spark)` and still runs on its own too.
- **`spark-jobs/bronze_job.py`** — Kafka → Delta, untouched raw landing. Raw string
  values plus Kafka's own metadata.
- **`spark-jobs/silver1_job.py`** — Bronze → Silver 1. Unpacks JSON envelope and
  provides `SILVER1_TRANSFORM_SQL` for formatting and cleaning.
- **`spark-jobs/silver2_job.py`** — Silver 1 → Silver 2. Provides
  `SILVER2_TRANSFORM_SQL` for data standardization.
- **`spark-jobs/gold_job.py`** — Silver 2 → Gold. Provides
  `GOLD_TRANSFORM_SQL` for business-ready fields and metrics.
- **`lineage/`** — pipeline lineage in Neo4j. See [Lineage](#lineage) below.
- **`delta/{bronze,silver1,silver2,gold}`** — the four Lakehouse tables.
  Inspect with `verify_delta.py`.
- **`checkpoints/`** — Structured Streaming's checkpoint state per job.

## Lineage

Every Spark job carries the `openlineage-spark` listener (wired in
`spark-jobs/common.py`). It publishes OpenLineage events to the Kafka topic
`openlineage.events`; `lineage-consumer` reads them and writes the graph into Neo4j
(spec KG1):

```
Spark jobs --OpenLineage events--> kafka: openlineage.events --> lineage-consumer --> Neo4j
```

- `lineage/normalize.py` keeps only the events that write a pipeline table and maps
  paths to table names (`/data/delta/silver2` becomes `silver2`). The raw stream is
  mostly Spark-internal plans and `_delta_log` reads, which carry no lineage.
- `lineage/graph.py` merges `(:Job)`, `(:Run)`, `(:Table)`, `(:SchemaVersion)` and
  `(:Table)-[:FEEDS]->(:Table)` into Neo4j. Every statement is a `MERGE`, so a replayed
  event changes nothing. The `:Table` nodes are the ones incidents attach to.
- `graph.downstream_tables()` / `upstream_tables()` answer "what does a failure in
  `silver2` reach" for the Deterministic Lookup step.

The listener must be **openlineage-spark 1.53.0 or newer** (pinned to 1.54.0). Earlier
versions do not report the Gold write, so the `silver2 -> gold` edge is missing.

Set `OPENLINEAGE_ENABLED=false` on a Spark service to turn the listener off.

Neo4j only applies `lineage/schema.cypher` when `neo4j-init` runs. If your Neo4j
container already exists, apply the new constraints once with
`docker compose up neo4j-init`.

Unit tests (no Kafka or Neo4j needed): `cd lineage && python -m pytest tests`.

## Swapping in the real dataset later

1. Drop the real CSV into `data/` (e.g. `data/cars_dataset_2025.csv`).
2. Change `CSV_PATH` under the `producer` service in `docker-compose.yml`
   to point at it.
3. If its column names match the mock CSV's header exactly, nothing else
   changes. If they differ, update `PAYLOAD_SCHEMA` in `silver_job.py` to
   match — that's the only place column names are hardcoded.

## Deliberately deferred (next steps)

- **Two-table Silver join in Gold** — marked with a comment in
  `gold_job.py`. Needs a watermark on both streaming sides once you get
  there; ask if you want to work through that pattern.
- **Multi-source / schema-drift simulation** — vary formatting per
  simulated source, deliberately rename/drop a field mid-stream.
- **Fault injection harness** — a corruption step between the CSV and the
  producer.
- **Detect/triage layer** — Great Expectations checkpoints on `silver_cars`,
  plus the severity-classification logic from the architecture doc.

## Notes

- Kafka runs in KRaft mode (no ZooKeeper) via the official `apache/kafka`
  image.
- `pyspark==3.5.3` / `delta-spark==3.3.1` is a verified-compatible pairing.
- Each Spark service's `depends_on: condition: service_started` only ensures
  container start order for readable logs -- the real safety comes from
  `wait_for_delta_table()` in `common.py`, which polls until the upstream
  table actually exists before starting a stream read on it.
- Every field in the mock CSV mirrors the real dataset's messy formatting
  (units embedded in strings, comma-formatted prices) on purpose, so the
  Silver transform you write against it transfers directly.
