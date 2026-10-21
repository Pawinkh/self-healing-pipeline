// Neo4j schema for the self-healing pipeline knowledge graph.
// Applied by the `neo4j-init` service in docker-compose.yml (cypher-shell -f).
// Every statement is idempotent (IF NOT EXISTS), so re-running is safe.
//
// ---------------------------------------------------------------------------
// Incident corpus (spec KG2, DL2, EL3, TA3)  -- task 3.4
// ---------------------------------------------------------------------------
// One (:Incident) node per resolved (or escalated) quarantine failure group.
// Written by the Learn node, read by the Deterministic Lookup and Diagnosis nodes.
//
//   (:Incident {
//     incident_id     STRING   unique, UUID
//     signature_key   STRING   "<table>|<column>|<UPPER(TRIM(raw_value))>", exact-match key
//     signature_text  STRING   text that was embedded (table, column, raw value, failure type)
//     failure_type    STRING   e.g. UNRESOLVED_VALUE | AMBIGUOUS_VALUE | SCHEMA_DRIFT | CORRUPT_RECORD
//     table_name      STRING   affected Silver table, e.g. "silver2"
//     column_name     STRING   failing column, e.g. "crash_month" (NULL for table-level failures)
//     raw_value       STRING   value as it landed in quarantine, e.g. "Jule"
//     diagnosis       STRING   root-cause text (LLM hypothesis or engineer note)
//     action          STRING   one of the pre-approved actions, e.g. ADD_VARIANT
//     action_params   STRING   JSON, e.g. {"canonical_value": "JUNE"}
//     outcome         STRING   SUCCESS | FAILED_VALIDATION | REJECTED
//     resolved_by     STRING   LOOKUP | LLM | ENGINEER
//     confidence      FLOAT    0..1, LLM self-reported (NULL when resolved_by = ENGINEER)
//     verified        BOOLEAN  true only when validation passed or an engineer approved;
//                              the lookup reuses only verified incidents
//     rows_affected   INTEGER  quarantine rows in the group
//     created_at      DATETIME
//     resolved_at     DATETIME
//     embedding       LIST<FLOAT>  vector over signature_text, see index below
//   })
//
//   (:Incident)-[:AFFECTS]->(:Table {name})   table the failure was detected in; lets
//                                             the blast radius query start from an incident
//
// `Table` nodes belong to the lineage writer (task 3.3); incidents MERGE on `name` only.

CREATE CONSTRAINT incident_id_unique IF NOT EXISTS
FOR (i:Incident) REQUIRE i.incident_id IS UNIQUE;

// Cheap exact-signature lookup before paying for a vector search.
CREATE INDEX incident_signature_key IF NOT EXISTS
FOR (i:Incident) ON (i.signature_key);

CREATE INDEX incident_verified IF NOT EXISTS
FOR (i:Incident) ON (i.verified);

// Similarity search over past incidents (DL2).
// Dimension 768 matches nomic-embed-text; it is fixed at creation time, so if the
// embedding model changes (task 3.5) drop this index and recreate it with the new size.
// Cosine scores returned by db.index.vector.queryNodes are normalised to [0, 1]
// ((1 + cos) / 2), so a "similarity threshold" is measured on that scale.
CREATE VECTOR INDEX incident_embedding IF NOT EXISTS
FOR (i:Incident) ON (i.embedding)
OPTIONS {indexConfig: {
  `vector.dimensions`: 768,
  `vector.similarity_function`: 'cosine'
}};
