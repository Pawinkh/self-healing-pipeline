"""Write normalized lineage into Neo4j (spec KG1, TA1).

Graph model (schema constraints in schema.cypher):

  (:Job {name})                  one per target table, e.g. "write_silver2"
  (:Run {run_id})-[:RUN_OF]->(:Job)
  (:Run|:Job)-[:READS|WRITES]->(:Table {name})
  (:Table)-[:FEEDS {job}]->(:Table)        input -> output, the edge blast radius walks
  (:Table)-[:HAS_SCHEMA]->(:SchemaVersion)  one per distinct column list; Table.schema_hash
                                            points at the latest

`Table` is the same node (:Incident)-[:AFFECTS]->(:Table) uses, merged on `name`.
Every statement is a MERGE, so replaying an event (Kafka redelivery) changes nothing.
"""
from typing import List

from normalize import LineageUpdate, TableRef

_TERMINAL = ["COMPLETED", "FAILED", "ABORTED"]

_JOB_AND_RUN = """
MERGE (j:Job {name: $job_name})
  ON CREATE SET j.namespace = $namespace, j.first_seen = datetime($event_time)
SET j.last_seen = datetime($event_time)
MERGE (r:Run {run_id: $run_id})
  ON CREATE SET r.started_at = datetime($event_time)
SET r.parent_run_id = coalesce($parent_run_id, r.parent_run_id),
    r.app_name      = coalesce($app_name, r.app_name),
    r.error_message = coalesce($error_message, r.error_message),
    // a START that arrives after the COMPLETE must not reopen the run
    r.status        = CASE WHEN r.status IN $terminal AND $status = 'RUNNING'
                           THEN r.status ELSE $status END,
    r.ended_at      = CASE WHEN $status IN $terminal THEN datetime($event_time) ELSE r.ended_at END
MERGE (r)-[:RUN_OF]->(j)
"""

_TABLES = """
MATCH (j:Job {name: $job_name}), (r:Run {run_id: $run_id})
UNWIND $tables AS t
MERGE (n:Table {name: t.name})
  ON CREATE SET n.first_seen = datetime($event_time)
SET n.kind = coalesce(n.kind, t.kind)
FOREACH (_ IN CASE WHEN $direction = 'WRITES' THEN [1] ELSE [] END |
  MERGE (j)-[:WRITES]->(n) MERGE (r)-[:WRITES]->(n)
  SET n.last_written = datetime($event_time))
FOREACH (_ IN CASE WHEN $direction = 'READS' THEN [1] ELSE [] END |
  MERGE (j)-[:READS]->(n) MERGE (r)-[:READS]->(n))
"""

_FEEDS = """
UNWIND $pairs AS p
MATCH (a:Table {name: p.src}), (b:Table {name: p.dst})
MERGE (a)-[f:FEEDS]->(b)
SET f.job = $job_name, f.last_run_id = $run_id, f.last_seen = datetime($event_time)
"""

_SCHEMA = """
UNWIND $schemas AS s
MATCH (t:Table {name: s.name})
MERGE (v:SchemaVersion {version_id: s.name + ':' + s.hash})
  ON CREATE SET v.table_name = s.name, v.hash = s.hash, v.fields = s.fields,
                v.field_count = size(s.fields), v.first_seen = datetime($event_time)
SET v.last_seen = CASE WHEN v.last_seen IS NULL OR v.last_seen < datetime($event_time)
                       THEN datetime($event_time) ELSE v.last_seen END
MERGE (t)-[:HAS_SCHEMA]->(v)
// Table.schema_hash follows the newest event, so replayed old events can't roll it back
FOREACH (_ IN CASE WHEN t.schema_seen_at IS NULL OR t.schema_seen_at <= datetime($event_time)
                   THEN [1] ELSE [] END |
  SET t.schema_hash = s.hash, t.schema_seen_at = datetime($event_time))
"""


def _tables_param(tables) -> List[dict]:
    return [{"name": t.name, "kind": t.kind} for t in tables]


def _schemas_param(update: LineageUpdate) -> List[dict]:
    return [
        {"name": t.name, "hash": t.schema_hash, "fields": list(t.schema)}
        for t in (*update.inputs, *update.outputs)
        if t.schema
    ]


def write_update(driver, update: LineageUpdate) -> None:
    """Apply one event in a single transaction."""
    common = {
        "job_name": update.job_name,
        "run_id": update.run_id,
        "event_time": update.event_time,
    }
    pairs = [{"src": i.name, "dst": o.name} for i in update.inputs for o in update.outputs]
    schemas = _schemas_param(update)

    def work(tx):
        tx.run(
            _JOB_AND_RUN,
            **common,
            namespace=update.namespace,
            parent_run_id=update.parent_run_id,
            app_name=update.app_name,
            error_message=update.error_message,
            status=update.status,
            terminal=_TERMINAL,
        )
        tx.run(_TABLES, **common, tables=_tables_param(update.outputs), direction="WRITES")
        if update.inputs:
            tx.run(_TABLES, **common, tables=_tables_param(update.inputs), direction="READS")
            tx.run(_FEEDS, **common, pairs=pairs)
        if schemas:
            tx.run(_SCHEMA, event_time=update.event_time, schemas=schemas)

    with driver.session() as session:
        session.execute_write(work)


def downstream_tables(driver, table_name: str) -> List[str]:
    """Every table fed, directly or transitively, by `table_name` (blast radius, DL1)."""
    records, _, _ = driver.execute_query(
        """
        MATCH (:Table {name: $name})-[:FEEDS*1..]->(d:Table)
        RETURN DISTINCT d.name AS name ORDER BY name
        """,
        name=table_name,
    )
    return [r["name"] for r in records]


def upstream_tables(driver, table_name: str) -> List[str]:
    """Every table `table_name` is built from, directly or transitively."""
    records, _, _ = driver.execute_query(
        """
        MATCH (u:Table)-[:FEEDS*1..]->(:Table {name: $name})
        RETURN DISTINCT u.name AS name ORDER BY name
        """,
        name=table_name,
    )
    return [r["name"] for r in records]
