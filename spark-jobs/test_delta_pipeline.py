"""
Integration and validation suite for Silver 2 + ref_mappings in Docker Delta environment.
Tests:
  1. _is_delta_table detection
  2. Table initializations (init_ref_mappings, init_fuzzy_audit)
  3. Idempotent Delta writes (_txn_write)
  4. Micro-batch routing:
     - clean row -> silver2
     - invalid/unmapped row -> quarantine (with raw_record & columns_to_remediate)
     - fuzzy row -> fuzzy_audit (with record_id)
  5. Vocabulary promotion via Delta MERGE
  6. In-memory overlay validation (overlay_mappings)
  7. Transform failure -> DEAD_LETTER_PATH parking
  8. Real data slice from /data/delta/silver1
"""
import os
import shutil
import sys
import traceback
from datetime import datetime, timezone
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from common import build_spark
import ref_mappings
from ref_mappings import (
    _is_delta_table,
    init_ref_mappings,
    init_fuzzy_audit,
    load_mappings_dict,
    promote_fuzzy_variants,
    overlay_mappings,
    REF_MAPPINGS_SCHEMA,
    FUZZY_AUDIT_SCHEMA,
)
import silver2_job
from silver2_job import (
    standardize_silver2,
    make_batch_processor,
    _txn_write,
)

TEST_BASE = "/data/delta/test_suite"
TEST_REF = f"{TEST_BASE}/ref_mappings"
TEST_AUDIT = f"{TEST_BASE}/fuzzy_audit"
TEST_SILVER2 = f"{TEST_BASE}/silver2"
TEST_QUARANTINE = f"{TEST_BASE}/quarantine"
TEST_DEAD_LETTER = f"{TEST_BASE}/dead_letter"


def cleanup_test_dirs():
    if os.path.exists(TEST_BASE):
        shutil.rmtree(TEST_BASE, ignore_errors=True)


def run_tests():
    print("=" * 70)
    print("STARTING SILVER 2 DELTA INTEGRATION TEST SUITE")
    print("=" * 70)

    spark = build_spark("Silver2TestSuite")
    silver2_job._require_spark_version(spark)
    print(f"Spark Version: {spark.version} (Delta Extension Active)")

    cleanup_test_dirs()

    # -------------------------------------------------------------
    # Test 1: _is_delta_table check
    # -------------------------------------------------------------
    print("\n[TEST 1] Testing _is_delta_table detection...")
    assert not _is_delta_table(spark, TEST_REF), "Non-existent path should not be delta table"
    print("  -> Non-existent path returns False: PASSED")

    # -------------------------------------------------------------
    # Test 2: init_ref_mappings & init_fuzzy_audit
    # -------------------------------------------------------------
    print("\n[TEST 2] Testing table initializations...")
    init_ref_mappings(spark, TEST_REF)
    assert _is_delta_table(spark, TEST_REF), "TEST_REF should now be a delta table"
    ref_count = spark.read.format("delta").load(TEST_REF).count()
    print(f"  -> init_ref_mappings created table with {ref_count} seed rows: PASSED")

    init_fuzzy_audit(spark, TEST_AUDIT)
    assert _is_delta_table(spark, TEST_AUDIT), "TEST_AUDIT should now be a delta table"
    audit_count = spark.read.format("delta").load(TEST_AUDIT).count()
    print(f"  -> init_fuzzy_audit created table with {audit_count} rows: PASSED")

    # Second call should be a no-op
    init_ref_mappings(spark, TEST_REF)
    init_fuzzy_audit(spark, TEST_AUDIT)
    assert spark.read.format("delta").load(TEST_REF).count() == ref_count
    print("  -> Re-initialization idempotency: PASSED")

    # -------------------------------------------------------------
    # Test 3: Idempotent writes (_txn_write)
    # -------------------------------------------------------------
    print("\n[TEST 3] Testing Delta idempotent writes (_txn_write)...")
    sample_df = spark.createDataFrame([("test_1", "A")], ["id", "val"])
    _txn_write(sample_df, f"{TEST_BASE}/txn_test", "test", 101)
    t1_count = spark.read.format("delta").load(f"{TEST_BASE}/txn_test").count()
    assert t1_count == 1, f"Expected 1 row, got {t1_count}"

    # Replay same txnVersion 101 -> Delta should ignore write
    _txn_write(sample_df, f"{TEST_BASE}/txn_test", "test", 101)
    t2_count = spark.read.format("delta").load(f"{TEST_BASE}/txn_test").count()
    assert t2_count == 1, f"Delta failed to deduplicate replayed txnVersion! Got {t2_count} rows"
    print(f"  -> Idempotent write skipped duplicate batch: PASSED (Rows: {t2_count})")

    # -------------------------------------------------------------
    # Test 4: Micro-batch routing (Valid, Quarantine, Fuzzy Audit)
    # -------------------------------------------------------------
    print("\n[TEST 4] Testing micro-batch processing & routing...")
    silver2_job.REF_MAPPINGS_PATH = TEST_REF
    silver2_job.FUZZY_AUDIT_PATH = TEST_AUDIT
    silver2_job.SILVER2_PATH = TEST_SILVER2
    silver2_job.QUARANTINE_PATH = TEST_QUARANTINE
    silver2_job.DEAD_LETTER_PATH = TEST_DEAD_LETTER
    silver2_job.TXN_APP_ID = "test_run"

    # Read base schema from silver1
    silver1_df = spark.read.format("delta").load(silver2_job.SILVER1_PATH)
    cols = silver1_df.columns

    # Grab 3 sample rows as base template
    base_rows = [r.asDict() for r in silver1_df.limit(3).collect()]

    # Row 0: VALID clean record
    base_rows[0]["record_id"] = "REC_VALID_01"
    base_rows[0]["crash_date"] = "03/04/2021"  # Slash date (US: March 4)
    base_rows[0]["date_police_notified"] = "09/05/2023 07:42:00 PM"
    base_rows[0]["weather_condition"] = "CLEAR"
    base_rows[0]["lighting_condition"] = "DAYLIGHT"
    base_rows[0]["street_direction"] = "N"
    base_rows[0]["crash_month"] = "3"
    base_rows[0]["crash_day_of_week"] = "5"
    base_rows[0]["damage"] = "OVER $1,500"
    base_rows[0]["posted_speed_limit"] = " 30 "
    base_rows[0]["num_units"] = "2"
    base_rows[0]["crash_hour"] = "14"

    # Row 1: QUARANTINE unmapped / ambiguous record
    base_rows[1]["record_id"] = "REC_QUAR_02"
    base_rows[1]["weather_condition"] = "HAILSTORM APOCALYPSE"  # unmapped
    base_rows[1]["crash_month"] = "MA"  # ambiguous month (March or May) -> quarantine
    base_rows[1]["posted_speed_limit"] = "99999999999"  # overflow digits -> quarantine

    # Row 2: FUZZY AUDIT record (typo within Levenshtein distance)
    base_rows[2]["record_id"] = "REC_FUZZY_03"
    base_rows[2]["weather_condition"] = "CLOUDY"
    base_rows[2]["lighting_condition"] = "DAYLITE"  # Typo of DAYLIGHT (dist 2)
    base_rows[2]["crash_month"] = "OCTOBR"          # Typo of OCTOBER (dist 1)
    base_rows[2]["street_direction"] = "NORTH"

    test_batch_df = spark.createDataFrame(base_rows, schema=silver1_df.schema)
    processor = make_batch_processor(spark)

    print("  -> Executing batch 1...")
    processor(test_batch_df, 1)

    # Verify Silver 2 valid table
    assert _is_delta_table(spark, TEST_SILVER2), "Silver 2 table was not created"
    s2_df = spark.read.format("delta").load(TEST_SILVER2)
    s2_ids = [r.record_id for r in s2_df.collect()]
    print(f"  -> Silver 2 rows count: {len(s2_ids)}, IDs: {s2_ids}")
    assert "REC_VALID_01" in s2_ids, "REC_VALID_01 missing from Silver 2"
    assert "columns_to_remediate" not in s2_df.columns, "columns_to_remediate leaked into Silver 2"
    assert "raw_record" not in s2_df.columns, "raw_record leaked into Silver 2"
    assert "_fuzzy_audit_entries" not in s2_df.columns, "_fuzzy_audit_entries leaked into Silver 2"

    # Check date parsing result: 03/04/2021 -> 2021-03-04
    rec1 = s2_df.filter(F.col("record_id") == "REC_VALID_01").first()
    assert rec1.crash_date == "2021-03-04", f"Expected 2021-03-04, got {rec1.crash_date}"
    assert rec1.date_police_notified == "2023-09-05 19:42:00", f"Expected 2023-09-05 19:42:00, got {rec1.date_police_notified}"
    assert rec1.posted_speed_limit == 30, f"Expected 30, got {rec1.posted_speed_limit}"
    assert rec1.crash_hour == "14.00", f"Expected 14.00, got {rec1.crash_hour}"
    print("  -> Slash date & PM timestamp & trimmed speed limit & crash_hour (hh.00) validated in Silver 2: PASSED")

    # Verify Quarantine table
    assert _is_delta_table(spark, TEST_QUARANTINE), "Quarantine table was not created"
    quar_df = spark.read.format("delta").load(TEST_QUARANTINE)
    quar_rows = quar_df.collect()
    quar_ids = [r.record_id for r in quar_rows]
    print(f"  -> Quarantine rows count: {len(quar_ids)}, IDs: {quar_ids}")
    assert "REC_QUAR_02" in quar_ids, "REC_QUAR_02 missing from Quarantine"
    assert "raw_record" in quar_df.columns, "raw_record missing from Quarantine table"
    assert "healing_status" in quar_df.columns, "healing_status missing from Quarantine table"

    q_rec = quar_df.filter(F.col("record_id") == "REC_QUAR_02").first()
    assert q_rec.healing_status == "PENDING"
    assert "weather_condition" in q_rec.columns_to_remediate
    assert "crash_month" in q_rec.columns_to_remediate
    assert q_rec.raw_record.record_id == "REC_QUAR_02"
    print("  -> Quarantine record lineage & raw_record preservation: PASSED")

    # Verify Fuzzy Audit table
    assert _is_delta_table(spark, TEST_AUDIT), "Fuzzy audit table was not created"
    audit_df = spark.read.format("delta").load(TEST_AUDIT)
    audit_records = audit_df.collect()
    print(f"  -> Fuzzy audit rows count: {len(audit_records)}")
    for a in audit_records:
        print(f"     Audit Entry: field={a.field}, record_id={a.record_id}, raw='{a.raw}', matched='{a.matched}', dist={a.distance}")
    audit_rec_ids = [a.record_id for a in audit_records]
    assert "REC_FUZZY_03" in audit_rec_ids, "Fuzzy audit entry missing record_id REC_FUZZY_03"
    print("  -> Fuzzy audit record_id traceability: PASSED")

    # -------------------------------------------------------------
    # Test 5: Vocabulary Promotion via Delta MERGE
    # -------------------------------------------------------------
    print("\n[TEST 5] Testing vocabulary promotion via Delta MERGE...")
    promoted_count = promote_fuzzy_variants(spark, TEST_AUDIT, TEST_REF, threshold=1)
    print(f"  -> promote_fuzzy_variants promoted: {promoted_count} rules")
    assert promoted_count > 0, "Expected at least 1 promoted rule"

    # Verify promoted entry in ref table
    ref_df = spark.read.format("delta").load(TEST_REF)
    octobr_rule = ref_df.filter(F.col("raw_synonym") == "OCTOBR").first()
    assert octobr_rule is not None, "OCTOBR rule not found in ref_canonical_mappings"
    assert octobr_rule.canonical_value == "OCTOBER", f"Expected 'OCTOBER', got {octobr_rule.canonical_value}"
    print("  -> Promoted rule OCTOBR -> OCTOBER found in Delta: PASSED")

    # Re-run promotion: Delta MERGE should insert 0 duplicates
    second_promo = promote_fuzzy_variants(spark, TEST_AUDIT, TEST_REF, threshold=1)
    assert second_promo == 0, f"Expected 0 promotions on re-run, got {second_promo}"
    print("  -> Promotion MERGE idempotency (0 duplicates): PASSED")

    # -------------------------------------------------------------
    # Test 6: In-Memory Overlay Validation (overlay_mappings)
    # -------------------------------------------------------------
    print("\n[TEST 6] Testing in-memory overlay mappings...")
    curr_map = load_mappings_dict(spark, TEST_REF)
    proposed_fix = {"weather_condition": {"HAILSTORM APOCALYPSE": "RAIN"}}
    overlaid = overlay_mappings(curr_map, proposed_fix)
    assert overlaid["weather_condition"]["HAILSTORM APOCALYPSE"] == "RAIN"
    # Ensure underlying ref table is untouched
    assert "HAILSTORM APOCALYPSE" not in curr_map.get("weather_condition", {})
    print("  -> overlay_mappings successfully augmented mapping in-memory: PASSED")

    # Replay quarantine row using overlaid mappings
    replayed = standardize_silver2(test_batch_df.filter(F.col("record_id") == "REC_QUAR_02"), overlaid)
    replayed_row = replayed.first()
    assert "weather_condition" not in replayed_row.columns_to_remediate, "HAILSTORM APOCALYPSE should be cleared by overlay"
    print("  -> Quarantine replay with overlay cleared the remediate column: PASSED")

    # -------------------------------------------------------------
    # Test 7: DEAD_LETTER_PATH parking on Transform Failure
    # -------------------------------------------------------------
    print("\n[TEST 7] Testing Transform exception dead-letter parking...")
    # Create corrupted DataFrame with missing required column to trigger failure in standardize
    corrupted_df = test_batch_df.drop("crash_date")

    print("  -> Executing batch 2 (intentionally broken schema)...")
    # Batch processor should NOT raise an unhandled exception to caller
    processor(corrupted_df, 2)

    assert _is_delta_table(spark, TEST_DEAD_LETTER), "Dead letter table was not created"
    dl_df = spark.read.format("delta").load(TEST_DEAD_LETTER)
    dl_rows = dl_df.collect()
    print(f"  -> Dead letter rows count: {len(dl_rows)}")
    assert len(dl_rows) > 0, "No records found in DEAD_LETTER_PATH"

    first_dl = dl_rows[0]
    print(f"     Dead Letter Sample: batch={first_dl.silver2_batch_id}, stage={first_dl.stage}, error_type={first_dl.error_type}, healing_status={first_dl.healing_status}")
    print(f"     Error Message: {first_dl.error_message[:100]}...")
    assert first_dl.stage == "transform"
    assert first_dl.healing_status == "PENDING_LLM"
    assert first_dl.silver2_batch_id == "2"
    assert first_dl.raw_json is not None
    assert first_dl.error_trace is not None
    print("  -> Dead letter capture & triage metadata: PASSED")

    # -------------------------------------------------------------
    # Test 8: Real Data Batch from Silver 1
    # -------------------------------------------------------------
    print("\n[TEST 8] Processing real batch of 50 rows from /data/delta/silver1...")
    real_slice = spark.read.format("delta").load(silver2_job.SILVER1_PATH).limit(50)
    processor(real_slice, 3)

    final_s2 = spark.read.format("delta").load(TEST_SILVER2).count()
    final_quar = spark.read.format("delta").load(TEST_QUARANTINE).count()
    print(f"  -> After real batch 3: Silver 2 total rows={final_s2}, Quarantine total rows={final_quar}")
    assert final_s2 > 1, "Real batch should have added valid rows to Silver 2"
    print("  -> Real data micro-batch execution: PASSED")

    print("\n" + "=" * 70)
    print("ALL 8 INTEGRATION TESTS PASSED SUCCESSFULLY!")
    print("=" * 70)


if __name__ == "__main__":
    try:
        run_tests()
    except Exception as e:
        print(f"\n[FAILED] Test suite failed with exception: {e}")
        traceback.print_exc()
        sys.exit(1)
