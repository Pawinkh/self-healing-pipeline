"""Turn raw OpenLineage events from the Spark listener into pipeline lineage (spec KG1, TA1).

The listener describes what Spark did, not what our pipeline means: datasets are
file paths (`/data/delta/silver2`), most events are internal plans
(`scan_existing_rdd_...`, `project`) and a short run produces hundreds of them.
This module is the filter between that and the graph, and it is pure (no Kafka,
no Neo4j) so it can be tested with captured events.

Only events that write at least one pipeline table are kept; reads and internal
plans carry no lineage on their own. Datasets map to tables like this:
  - a directory directly under DELTA_ROOT becomes (:Table {name: "<dir name>"}), so
    /data/delta/silver2 -> "silver2", the name (:Incident)-[:AFFECTS]->(:Table) uses
  - a Kafka topic becomes (:Table {kind: "kafka_topic"})
  - anything else (`_delta_log` snapshots, parquet part files, ...) is dropped
"""
import hashlib
from dataclasses import dataclass
from typing import List, Optional, Tuple

DEFAULT_DELTA_ROOT = "/data/delta"

# OpenLineage RunEvent.eventType -> Run.status
RUN_STATUS = {
    "START": "RUNNING",
    "RUNNING": "RUNNING",
    "COMPLETE": "COMPLETED",
    "FAIL": "FAILED",
    "ABORT": "ABORTED",
}


@dataclass(frozen=True)
class TableRef:
    name: str
    kind: str  # "delta" | "kafka_topic"
    schema: Optional[Tuple[str, ...]] = None  # ("col:type", ...) in column order

    @property
    def schema_hash(self) -> Optional[str]:
        if not self.schema:
            return None
        return hashlib.sha1("|".join(self.schema).encode("utf-8")).hexdigest()[:16]


@dataclass(frozen=True)
class LineageUpdate:
    namespace: str
    job_name: str
    run_id: str
    parent_run_id: Optional[str]
    app_name: Optional[str]
    status: str
    event_time: str
    inputs: Tuple[TableRef, ...]
    outputs: Tuple[TableRef, ...]
    error_message: Optional[str] = None


def table_from_dataset(dataset: dict, delta_root: str = DEFAULT_DELTA_ROOT) -> Optional[TableRef]:
    namespace = dataset.get("namespace") or ""
    name = dataset.get("name") or ""
    fields = (dataset.get("facets") or {}).get("schema", {}).get("fields") or []
    schema = tuple(f"{f.get('name')}:{f.get('type')}" for f in fields) or None

    if namespace.startswith("kafka://"):
        return TableRef(name=name, kind="kafka_topic", schema=schema) if name else None

    root = delta_root.rstrip("/") + "/"
    if name.startswith(root):
        table = name[len(root):]
        # Only the table directory itself: `<root>/silver2/_delta_log` etc. are Spark internals.
        if table and "/" not in table:
            return TableRef(name=table, kind="delta", schema=schema)
    return None


def _tables(datasets: List[dict], delta_root: str) -> Tuple[TableRef, ...]:
    seen = {}
    for ds in datasets or []:
        ref = table_from_dataset(ds, delta_root)
        if ref is not None:
            seen.setdefault(ref.name, ref)
    return tuple(seen[name] for name in sorted(seen))


def normalize(event: dict, delta_root: str = DEFAULT_DELTA_ROOT) -> Optional[LineageUpdate]:
    """Return the lineage in one OpenLineage RunEvent, or None if it is not a pipeline write."""
    status = RUN_STATUS.get(event.get("eventType") or "")
    run = event.get("run") or {}
    run_id = run.get("runId")
    event_time = event.get("eventTime")
    if status is None or not run_id or not event_time:
        return None

    outputs = _tables(event.get("outputs"), delta_root)
    if not outputs:  # reads and internal plans carry no lineage on their own
        return None
    inputs = _tables(event.get("inputs"), delta_root)

    run_facets = run.get("facets") or {}
    parent = run_facets.get("parent") or {}
    error = (run_facets.get("errorMessage") or {}).get("message")
    job = event.get("job") or {}

    return LineageUpdate(
        namespace=job.get("namespace") or "",
        # One job per target table; Spark's own job names embed the app name and plan node.
        job_name=f"write_{outputs[0].name}",
        run_id=run_id,
        parent_run_id=(parent.get("run") or {}).get("runId"),
        app_name=(parent.get("job") or {}).get("name"),
        status=status,
        event_time=event_time,
        inputs=inputs,
        outputs=outputs,
        error_message=error,
    )
