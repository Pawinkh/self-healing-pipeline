"""Unit tests for lineage/normalize.py (no Kafka, no Neo4j).

Run from the lineage/ directory:  python -m pytest tests

The fixture holds real events captured from the Spark listener (openlineage-spark
1.54.0) on the running pipeline, with facets trimmed to the schema and parent run.
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from normalize import TableRef, normalize, table_from_dataset  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "spark_events.jsonl"


def load_events():
    return [json.loads(line) for line in FIXTURE.read_text(encoding="utf-8").splitlines() if line]


def edges(update):
    return [(i.name, o.name) for i in update.inputs for o in update.outputs]


def test_every_pipeline_hop_is_captured():
    updates = [u for u in map(normalize, load_events()) if u is not None]
    seen = {edge for u in updates if u.status == "COMPLETED" for edge in edges(u)}
    assert seen == {
        ("crashes.crash1", "bronze"),
        ("bronze", "silver1"),
        ("silver1", "silver2"),
        ("silver1", "silver2_quarantine"),
        ("silver2", "gold"),
    }


def test_paths_become_table_names_and_kafka_topics_keep_their_kind():
    by_target = {u.outputs[0].name: u for u in map(normalize, load_events()) if u}
    assert by_target["gold"].job_name == "write_gold"
    assert [(t.name, t.kind) for t in by_target["gold"].inputs] == [("silver2", "delta")]
    topic = by_target["bronze"].inputs[0]
    assert (topic.name, topic.kind) == ("crashes.crash1", "kafka_topic")


def test_events_without_a_pipeline_output_are_dropped():
    events = load_events()
    reads_only = [e for e in events if not e["outputs"]]
    log_only = [
        e for e in events
        if e["outputs"] and all(o["name"].endswith("_delta_log") for o in e["outputs"])
    ]
    assert reads_only and log_only  # the fixture really contains both kinds
    assert all(normalize(e) is None for e in reads_only + log_only)


def test_write_without_inputs_is_kept():
    updates = [normalize(e) for e in load_events()]
    audit = next(u for u in updates if u and u.outputs[0].name == "silver2_fuzzy_audit")
    assert audit.inputs == ()


def test_event_types_map_to_run_status():
    silver2 = [u for u in map(normalize, load_events()) if u and u.outputs[0].name == "silver2"]
    assert [u.status for u in silver2] == ["RUNNING", "COMPLETED"]


def test_parent_run_and_app_name_come_from_the_parent_facet():
    update = next(u for u in map(normalize, load_events()) if u and u.outputs[0].name == "gold")
    assert update.parent_run_id
    assert update.app_name and update.app_name != update.job_name


def test_delta_root_is_configurable():
    ds = {"namespace": "file", "name": "/lake/silver2"}
    assert table_from_dataset(ds, delta_root="/lake") == TableRef("silver2", "delta")
    assert table_from_dataset(ds, delta_root="/data/delta") is None


def test_only_the_table_directory_itself_counts():
    for name in (
        "/data/delta/silver2/_delta_log",
        "/data/delta/silver2/part-0000.parquet",
        "/data/delta",
        "/elsewhere/silver2",
    ):
        assert table_from_dataset({"namespace": "file", "name": name}) is None


def test_schema_hash_follows_the_column_list():
    def ds(*cols):
        fields = [{"name": n, "type": t} for n, t in cols]
        return {"namespace": "file", "name": "/data/delta/silver2", "facets": {"schema": {"fields": fields}}}

    a = table_from_dataset(ds(("record_id", "string"), ("crash_month", "integer")))
    same = table_from_dataset(ds(("record_id", "string"), ("crash_month", "integer")))
    renamed = table_from_dataset(ds(("record_id", "string"), ("crash_mon", "integer")))
    retyped = table_from_dataset(ds(("record_id", "string"), ("crash_month", "string")))
    no_schema = table_from_dataset({"namespace": "file", "name": "/data/delta/silver2"})

    assert a.schema_hash == same.schema_hash
    assert len({a.schema_hash, renamed.schema_hash, retyped.schema_hash}) == 3
    assert no_schema.schema_hash is None


def test_failed_run_carries_the_error_message():
    event = {
        "eventType": "FAIL",
        "eventTime": "2026-10-21T15:00:00.000Z",
        "job": {"namespace": "self-healing-pipeline", "name": "x"},
        "run": {"runId": "r1", "facets": {"errorMessage": {"message": "boom"}}},
        "inputs": [{"namespace": "file", "name": "/data/delta/silver1"}],
        "outputs": [{"namespace": "file", "name": "/data/delta/silver2"}],
    }
    update = normalize(event)
    assert (update.status, update.error_message) == ("FAILED", "boom")


def test_malformed_events_are_ignored():
    assert normalize({}) is None
    assert normalize({"eventType": "COMPLETE", "run": {}}) is None
    assert normalize({"eventType": "OTHER", "run": {"runId": "r"}, "eventTime": "t"}) is None
