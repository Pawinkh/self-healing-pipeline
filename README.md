# Self-healing pipeline — Bronze/Silver/Gold starter

Kafka → three Spark Structured Streaming jobs (Bronze, Silver, Gold) → Delta
Lake, running as a multi-hop pipeline: each layer streams from the previous
layer's Delta table, the same pattern real Lakehouses use. Ships with a
small bundled mock dataset so it runs immediately with no download step.

```
producer --> kafka --> bronze-job --> delta/bronze
                                          |
                                          v
                                     silver-job --> delta/silver_cars
                                                        |
                                                        v
                                                   gold-job --> delta/gold_car_overview
```

## Prerequisites

- Docker + Docker Compose v2
- ~6-8 GB RAM free (Kafka + three separate Spark JVMs, one per layer)

## Setup

```bash
docker compose up --build
```

That's it — no dataset to download. The producer replays the bundled
`data/mock_cars.csv` (15 rows, using the real dataset's exact column
names) through Kafka into Bronze, Silver, and Gold automatically.

First run takes a minute or two (Spark images install a JVM, connector
jars get pulled). Subsequent runs are fast.

## Watch it work

- **Kafka UI** — http://localhost:8080 — browse the `cars.raw` topic
- **Producer logs** — `Batch N: published 5 rows to 'cars.raw'` every 5s
- **bronze-job / silver-job / gold-job logs** — each prints its source and
  destination path once, then streams silently as micro-batches arrive

## Verify data landed, from the host (no Spark needed)

```bash
pip install deltalake pandas
python verify_delta.py bronze
python verify_delta.py silver_cars
python verify_delta.py gold_car_overview
```

`silver_cars` should show parsed numeric fields (`horsepower_hp`,
`price_usd`, `powertrain_type`, ...) instead of raw strings — including one
row (Ford F-150) with `price_usd = NULL`, since its source value was the
deliberately-messy `"unknown"`, proving the transform nulls out cleanly
instead of crashing the batch.

To stop: `docker compose down`. To wipe all state and start clean:
`docker compose down && rm -rf delta checkpoints`.

## What's in each piece

- **`producer/`** — reads a CSV (schema-agnostic), publishes rows to Kafka
  in small batches on an interval, wrapped in an envelope
  (`record_id`, `batch_id`, `ingestion_ts`, `payload`). Unchanged from
  before -- doesn't know or care what columns exist.
- **`spark-jobs/common.py`** — shared Spark session builder and a
  wait-for-upstream-table guard, used by all three jobs below.
- **`spark-jobs/bronze_job.py`** — Kafka → Delta, untouched. Raw string
  values plus Kafka's own metadata. No parsing.
- **`spark-jobs/silver_job.py`** — the file to edit. `PAYLOAD_SCHEMA` at
  the top defines what the JSON payload looks like (matches the mock
  CSV's header exactly); `SILVER_TRANSFORM_SQL` is one plain SQL string
  that does the actual cleaning, run via `spark.sql(...)`.
- **`spark-jobs/gold_job.py`** — also editable SQL (`GOLD_TRANSFORM_SQL`).
  Currently a plain `SELECT` from Silver; the comment marks exactly where
  a join against a second Silver table goes later.
- **`delta/{bronze,silver_cars,gold_car_overview}`** — the three tables.
  Inspect with `verify_delta.py`.
- **`checkpoints/`** — Structured Streaming's checkpoint state per job.
  Don't hand-edit; delete the relevant subfolder for a clean restart of
  just that layer.

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
- **Lineage capture** — `openlineage-spark` as a Spark listener, pointed
  at a webhook that writes into Neo4j.
- **Detect/triage layer** — Great Expectations checkpoints on `silver_cars`,
  plus the severity-classification logic from the architecture doc.

## Notes

- Kafka runs in KRaft mode (no ZooKeeper) via the official `apache/kafka`
  image.
- `pyspark==3.5.3` / `delta-spark==3.3.1` is a verified-compatible pairing.
- Each Spark job's `depends_on: condition: service_started` only ensures
  container start order for readable logs -- the real safety comes from
  `wait_for_delta_table()` in `common.py`, which polls until the upstream
  table actually exists before starting a stream read on it.
- Every field in the mock CSV mirrors the real dataset's messy formatting
  (units embedded in strings, comma-formatted prices) on purpose, so the
  Silver transform you write against it transfers directly.
