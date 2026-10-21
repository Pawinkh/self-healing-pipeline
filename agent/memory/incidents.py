"""Incident memory in Neo4j (spec KG2, DL2, EL3, TA3).

The graph schema lives in lineage/schema.cypher. This module is the only Python
code that reads or writes (:Incident) nodes, so the LangGraph nodes (lookup,
diagnose, learn) share one contract.

Embeddings are passed in, not computed here: the embedding model is chosen in
task 3.5, and its output size must match `vector.dimensions` in schema.cypher.
"""
import json
import os
import uuid
from pathlib import Path
from typing import Optional

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "selfhealing")

VECTOR_INDEX = "incident_embedding"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "lineage" / "schema.cypher"


def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def signature_key(table_name: str, column_name: Optional[str], raw_value: Optional[str]) -> str:
    """Exact-match key for a failure: same table, column and normalised raw value."""
    value = (raw_value or "").strip().upper()
    return f"{table_name}|{column_name or ''}|{value}"


def apply_schema(driver) -> None:
    """Apply lineage/schema.cypher. Normally done by the neo4j-init compose service."""
    text = SCHEMA_PATH.read_text(encoding="utf-8")
    lines = (ln for ln in text.splitlines() if not ln.lstrip().startswith("//"))
    for statement in "\n".join(lines).split(";"):
        if statement.strip():
            driver.execute_query(statement)


def write_incident(
    driver,
    *,
    table_name: str,
    column_name: Optional[str],
    raw_value: Optional[str],
    failure_type: str,
    diagnosis: str,
    action: str,
    action_params: dict,
    outcome: str,
    resolved_by: str,
    confidence: Optional[float],
    verified: bool,
    rows_affected: int,
    embedding: list,
    signature_text: str,
    incident_id: Optional[str] = None,
) -> str:
    """Create or update an incident (Learn node). Returns its incident_id."""
    incident_id = incident_id or str(uuid.uuid4())
    driver.execute_query(
        """
        MERGE (i:Incident {incident_id: $incident_id})
          ON CREATE SET i.created_at = datetime()
        SET i.signature_key  = $signature_key,
            i.signature_text = $signature_text,
            i.failure_type   = $failure_type,
            i.table_name     = $table_name,
            i.column_name    = $column_name,
            i.raw_value      = $raw_value,
            i.diagnosis      = $diagnosis,
            i.action         = $action,
            i.action_params  = $action_params,
            i.outcome        = $outcome,
            i.resolved_by    = $resolved_by,
            i.confidence     = $confidence,
            i.verified       = $verified,
            i.rows_affected  = $rows_affected,
            i.resolved_at    = datetime()
        WITH i
        CALL db.create.setNodeVectorProperty(i, 'embedding', $embedding)
        WITH i
        MERGE (t:Table {name: $table_name})
        MERGE (i)-[:AFFECTS]->(t)
        """,
        incident_id=incident_id,
        signature_key=signature_key(table_name, column_name, raw_value),
        signature_text=signature_text,
        failure_type=failure_type,
        table_name=table_name,
        column_name=column_name,
        raw_value=raw_value,
        diagnosis=diagnosis,
        action=action,
        action_params=json.dumps(action_params, sort_keys=True),
        outcome=outcome,
        resolved_by=resolved_by,
        confidence=confidence,
        verified=verified,
        rows_affected=rows_affected,
        embedding=embedding,
    )
    return incident_id


_RETURN_FIELDS = """
    i.incident_id AS incident_id, i.signature_key AS signature_key,
    i.failure_type AS failure_type, i.table_name AS table_name,
    i.column_name AS column_name, i.raw_value AS raw_value,
    i.diagnosis AS diagnosis, i.action AS action, i.action_params AS action_params,
    i.outcome AS outcome, i.resolved_by AS resolved_by, i.confidence AS confidence
"""


def find_exact(driver, table_name: str, column_name: Optional[str], raw_value: Optional[str]) -> Optional[dict]:
    """Latest verified incident with the same signature, or None. Needs no embedding."""
    records, _, _ = driver.execute_query(
        f"""
        MATCH (i:Incident {{signature_key: $key}})
        WHERE i.verified = true AND i.outcome = 'SUCCESS'
        RETURN {_RETURN_FIELDS}, 1.0 AS score
        ORDER BY i.resolved_at DESC LIMIT 1
        """,
        key=signature_key(table_name, column_name, raw_value),
    )
    return records[0].data() if records else None


def find_similar(driver, embedding: list, *, k: int = 5, min_score: float = 0.0) -> list:
    """Verified past incidents ranked by similarity (DL2 / AP1).

    `score` is Neo4j's cosine score on a [0, 1] scale ((1 + cos) / 2), so compare it
    with the Deterministic Lookup threshold on that scale. Unverified or failed
    incidents are excluded, so only proven resolutions can be reused.
    """
    records, _, _ = driver.execute_query(
        f"""
        CALL db.index.vector.queryNodes('{VECTOR_INDEX}', $k, $embedding)
        YIELD node AS i, score
        WHERE i.verified = true AND i.outcome = 'SUCCESS' AND score >= $min_score
        RETURN {_RETURN_FIELDS}, score
        ORDER BY score DESC
        """,
        k=k,
        embedding=embedding,
        min_score=min_score,
    )
    return [r.data() for r in records]
