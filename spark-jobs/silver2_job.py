"""
Silver 2: Data standardization via pure PySpark Structured Streaming.

Reads from Silver 1's cleaned Delta table, applies canonical lookup mappings
and native date/numeric parsing chains, isolates failed records into a
physically separate Quarantine table, and appends valid records to Silver 2.

Per micro-batch the job runs in two stages:

  1. TRANSFORM  - standardize + materialize. Any Spark exception here (schema
                  drift, analysis errors, ...) is caught and the raw batch is
                  parked in the dead-letter table for the small-LLM triage node.
                  Nothing has been written to Silver 2 / quarantine yet, so
                  dead-lettering the whole batch cannot create duplicates.
  2. WRITE      - valid -> Silver 2, failed -> quarantine, fuzzy fixes -> audit.
                  Each write is an idempotent Delta write (txnAppId/txnVersion),
                  so if the job dies half-way and Spark replays the batch, the
                  writes that already landed are skipped instead of duplicated.
                  Failures here are infrastructure problems and are re-raised.

Quarantine rows carry the ORIGINAL Silver 1 row in `raw_record`. The Validate
step of the healing loop can therefore replay them through `standardize_silver2`
(optionally with `ref_mappings.overlay_mappings` to test a proposed fix before
it is promoted) and just check that `columns_to_remediate` comes back empty.
"""

import os
import traceback
from itertools import chain
from typing import Any, Dict, Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    StringType,
)

from common import build_spark, wait_for_delta_table
from ref_mappings import (
    ANCHOR_TO_CANONICAL,
    CANONICAL_ANCHORS,
    fuzzy_canonical,
    init_fuzzy_audit,
    init_ref_mappings,
    load_mappings_dict,
)

SILVER1_PATH = os.environ.get("SILVER1_PATH", "/data/delta/silver1")
SILVER2_PATH = os.environ.get("SILVER2_PATH", "/data/delta/silver2")
CHECKPOINT_PATH = os.environ.get("SILVER2_CHECKPOINT_PATH", "/data/checkpoints/silver2")
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/data/delta/silver2_quarantine")
DEAD_LETTER_PATH = os.environ.get(
    "DEAD_LETTER_PATH", "/data/delta/silver2_dead_letter"
)
REF_MAPPINGS_PATH = os.environ.get(
    "REF_MAPPINGS_PATH", "/data/delta/ref_canonical_mappings"
)
FUZZY_AUDIT_PATH = os.environ.get(
    "FUZZY_AUDIT_PATH", os.environ.get("AUDIT_PATH", "/data/delta/silver2_fuzzy_audit")
)

# Idempotent-write application id. Delta ignores any write whose txnVersion is
# <= the last one recorded for the same appId, and batch ids restart at 0 when a
# checkpoint is wiped. So: if you ever delete CHECKPOINT_PATH, change this value.
TXN_APP_ID = os.environ.get("TXN_APP_ID", "silver2_job")

# Set to "true" only if Silver 1 is ever updated/merged (not append-only);
# updates are then skipped by the stream instead of crashing it.
SILVER1_SKIP_CHANGE_COMMITS = (
    os.environ.get("SILVER1_SKIP_CHANGE_COMMITS", "false").lower() == "true"
)

# try_to_timestamp / array_compact need Spark 3.4+
MIN_SPARK_VERSION = (3, 4)

MAX_ERROR_CHARS = 4000
MAX_TRACE_CHARS = 8000

# Supported additive date format strings.
# Slash dates are read US-style (month first) because that is the source
# convention; "d/M/yyyy" is only a fallback for values that cannot be month-first
# (e.g. 25/12/2021). Ambiguous values like 03/04/2021 therefore parse as March 4.
DATE_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "yyyy/MM/dd HH:mm:ss",
    "yyyy/MM/dd",
    "M/d/yyyy h:mm:ss a",
    "M/d/yyyy H:mm:ss",
    "M/d/yyyy",
    "d/M/yyyy H:mm:ss",
    "d/M/yyyy",
    "dd-MM-yyyy",
    "yyyyMMdd",
]

# Supported additive timestamp format strings (same month-first convention)
TIMESTAMP_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "yyyy/MM/dd HH:mm:ss",
    "M/d/yyyy h:mm:ss a",
    "M/d/yyyy H:mm:ss",
    "M/d/yyyy",
]


def _try_ts(col_name: str, fmt: str) -> Column:
    """try_to_timestamp never raises on a bad value or format mismatch; it yields NULL."""
    return F.expr(f"try_to_timestamp(`{col_name}`, \"{fmt}\")")


def _build_map_expr(
    field_name: str,
    mapping_dict: Dict[str, Dict[str, str]],
    input_col: Optional[Any] = None,
):
    """Create a Catalyst create_map expression for a given categorical field."""
    field_map = mapping_dict.get(field_name, {})
    if not field_map:
        return F.lit(None).cast(StringType())
    c = F.col(field_name) if input_col is None else input_col
    return F.create_map([F.lit(x) for x in chain(*field_map.items())])[
        F.upper(F.trim(c))
    ]


def _digits_only(raw_col: Column, max_len: int = 3) -> Column:
    """Trimmed value cast to INT, only when it is a short run of digits (no overflow)."""
    t = F.trim(raw_col)
    return F.when(t.rlike(rf"^\d{{1,{max_len}}}$"), t.cast(IntegerType())).otherwise(None)


def _fuzzy_audit_struct(
    field: str, record_id: Column, raw_col: Column, effective_fuzzy: Column
) -> Column:
    """(field, record_id, raw, matched, distance) for one successful fuzzy correction."""
    return F.when(
        effective_fuzzy.isNotNull() & raw_col.isNotNull() & (F.trim(raw_col) != ""),
        F.struct(
            F.lit(field).alias("field"),
            record_id.alias("record_id"),
            raw_col.alias("raw"),
            effective_fuzzy.alias("matched"),
            F.levenshtein(F.upper(F.trim(raw_col)), effective_fuzzy).alias("distance"),
        ),
    ).otherwise(None)


def standardize_silver2(df: DataFrame, mapping_dict: Dict[str, Dict[str, str]]) -> DataFrame:
    """
    Apply native PySpark parsing chains and canonical reference lookups.
    Tracks all mapping and parsing failures in `columns_to_remediate`.

    Pure function of (Silver 1 row, mapping_dict): it is also what the healing
    loop replays quarantined rows through.
    """
    # Explicit bindings to input DataFrame columns to avoid projection alias shadowing
    record_id = df["record_id"]
    raw_crash_date = df["crash_date"]
    raw_date_police_notified = df["date_police_notified"]
    raw_weather = df["weather_condition"]
    raw_lighting = df["lighting_condition"]
    raw_damage = df["damage"]
    raw_hit_and_run = df["hit_and_run_i"]
    raw_street_direction = df["street_direction"]
    raw_crash_hour = df["crash_hour"]
    raw_crash_month = df["crash_month"]
    raw_crash_day = df["crash_day_of_week"]
    raw_num_units = df["num_units"]
    raw_speed_limit = df["posted_speed_limit"]

    # Keep the untouched Silver 1 row so quarantined records can be replayed
    raw_record = F.struct(*[df[c] for c in df.columns])

    # 1. Native Date Parsing Chain
    parsed_crash_date = F.coalesce(
        *[F.to_date(_try_ts("crash_date", fmt)) for fmt in DATE_FORMATS]
    )
    std_crash_date = F.date_format(parsed_crash_date, "yyyy-MM-dd")

    # 2. Native Timestamp Parsing Chain for date_police_notified
    parsed_police_ts = F.coalesce(
        *[_try_ts("date_police_notified", fmt) for fmt in TIMESTAMP_FORMATS]
    )
    std_date_police_notified = F.date_format(parsed_police_ts, "yyyy-MM-dd HH:mm:ss")

    # 3. Categorical Fields via Reference Mapping Table with Fuzzy Canonical Fallback
    # Rule: Runs ONLY after exact lookup fails.
    # Guardrails: length >= 4, distance scaled to length, unique winner margin >= 2,
    # and fixed seed anchors only.

    # weather_condition
    exact_weather = _build_map_expr("weather_condition", mapping_dict, raw_weather)
    fuzzy_weather = fuzzy_canonical(
        raw_weather, CANONICAL_ANCHORS["weather_condition"]
    )
    effective_fuzzy_weather = F.when(exact_weather.isNull(), fuzzy_weather).otherwise(None)
    std_weather = F.coalesce(exact_weather, effective_fuzzy_weather)

    # lighting_condition
    exact_lighting = _build_map_expr("lighting_condition", mapping_dict, raw_lighting)
    fuzzy_lighting = fuzzy_canonical(
        raw_lighting, CANONICAL_ANCHORS["lighting_condition"]
    )
    effective_fuzzy_lighting = F.when(exact_lighting.isNull(), fuzzy_lighting).otherwise(None)
    std_lighting = F.coalesce(exact_lighting, effective_fuzzy_lighting)

    # damage & hit_and_run_i (open/complex categories - no fuzzy canonical matching)
    std_damage = _build_map_expr("damage", mapping_dict, raw_damage)
    std_hit_and_run = _build_map_expr(
        "hit_and_run_i", mapping_dict, raw_hit_and_run
    ).cast(BooleanType())

    # street_direction (fuzzy matches NORTH/SOUTH/EAST/WEST -> maps to N/S/E/W)
    exact_street_direction = _build_map_expr("street_direction", mapping_dict, raw_street_direction)
    fuzzy_dir_anchor = fuzzy_canonical(
        raw_street_direction, CANONICAL_ANCHORS["street_direction"]
    )
    effective_fuzzy_dir = F.when(exact_street_direction.isNull(), fuzzy_dir_anchor).otherwise(None)
    fuzzy_dir_canonical = _build_map_expr(
        "street_direction", ANCHOR_TO_CANONICAL, effective_fuzzy_dir
    )
    std_street_direction = F.coalesce(exact_street_direction, fuzzy_dir_canonical)

    # 4. Numeric & Time Parsing
    # crash_hour: ref lookup, 12h/24h timestamp parse, or direct integer/decimal cast -> format as "hh.00"
    hour_from_ref = _build_map_expr("crash_hour", mapping_dict, raw_crash_hour)
    hour_from_ts = F.hour(
        F.coalesce(
            F.expr("try_to_timestamp(`crash_hour`, 'h a')"),
            F.expr("try_to_timestamp(`crash_hour`, 'hh a')"),
            F.expr("try_to_timestamp(`crash_hour`, 'HH:mm')"),
            F.expr("try_to_timestamp(`crash_hour`, 'h:mm a')"),
            F.expr("try_to_timestamp(`crash_hour`, 'hh:mm a')"),
            F.expr("try_to_timestamp(`crash_hour`, 'HH.mm')"),
            F.expr("try_to_timestamp(`crash_hour`, 'h.mm a')"),
            F.expr("try_to_timestamp(`crash_hour`, 'hh.mm a')"),
        )
    )
    hour_as_num = F.when(
        F.trim(raw_crash_hour).rlike(r"^\d{1,2}(\.\d+)?$"),
        F.trim(raw_crash_hour).cast(DecimalType(10, 2)).cast(IntegerType()),
    ).otherwise(None)
    candidate_hour = F.coalesce(
        hour_from_ref.cast(IntegerType()), hour_from_ts, hour_as_num, _digits_only(raw_crash_hour)
    )
    std_crash_hour = F.when(
        candidate_hour.between(0, 23),
        F.format_string("%02d.00", candidate_hour),
    ).otherwise(None)

    # crash_month: exact lookup (from ref or digits) -> fuzzy canonical fallback
    exact_crash_month = _build_map_expr("crash_month", mapping_dict, raw_crash_month)
    fuzzy_month_anchor = fuzzy_canonical(
        raw_crash_month, CANONICAL_ANCHORS["crash_month"]
    )
    effective_fuzzy_month = F.when(exact_crash_month.isNull(), fuzzy_month_anchor).otherwise(None)
    fuzzy_month_canonical = _build_map_expr(
        "crash_month", ANCHOR_TO_CANONICAL, effective_fuzzy_month
    )
    std_crash_month = F.coalesce(exact_crash_month, fuzzy_month_canonical)

    # crash_day_of_week: exact lookup (from ref or digits) -> fuzzy canonical fallback
    exact_crash_day = _build_map_expr("crash_day_of_week", mapping_dict, raw_crash_day)
    fuzzy_day_anchor = fuzzy_canonical(
        raw_crash_day, CANONICAL_ANCHORS["crash_day_of_week"]
    )
    effective_fuzzy_day = F.when(exact_crash_day.isNull(), fuzzy_day_anchor).otherwise(None)
    fuzzy_day_canonical = _build_map_expr(
        "crash_day_of_week", ANCHOR_TO_CANONICAL, effective_fuzzy_day
    )
    std_crash_day_of_week = F.coalesce(exact_crash_day, fuzzy_day_canonical)

    # num_units: ref lookup for words ('one' -> 1.0) or regex numeric extract
    units_from_ref = _build_map_expr("num_units", mapping_dict, raw_num_units)
    units_from_regex = F.regexp_extract(raw_num_units, r"(\d{1,8}(\.\d+)?)", 1).cast(DecimalType(10, 1))
    std_num_units = F.coalesce(units_from_ref.cast(DecimalType(10, 1)), units_from_regex)

    # posted_speed_limit: regex numeric extract
    std_posted_speed_limit = F.regexp_extract(raw_speed_limit, r"(\d{1,4})", 1).cast(IntegerType())

    # 5. Multi-field Failure Tracking into columns_to_remediate
    # Format: { field_name: (raw_col, std_col, is_mandatory) }
    tracked_fields = {
        "crash_date": (raw_crash_date, std_crash_date, True),
        "weather_condition": (raw_weather, std_weather, False),
        "lighting_condition": (raw_lighting, std_lighting, False),
        "hit_and_run_i": (raw_hit_and_run, std_hit_and_run, False),
        "damage": (raw_damage, std_damage, False),
        "street_direction": (raw_street_direction, std_street_direction, False),
        "num_units": (raw_num_units, std_num_units, False),
        "crash_month": (raw_crash_month, std_crash_month, False),
        "crash_hour": (raw_crash_hour, std_crash_hour, False),
        "crash_day_of_week": (raw_crash_day, std_crash_day_of_week, False),
        "date_police_notified": (raw_date_police_notified, std_date_police_notified, False),
        "posted_speed_limit": (raw_speed_limit, std_posted_speed_limit, False),
    }

    failure_structs = []
    for name, (raw_col, std_col, mandatory) in tracked_fields.items():
        if mandatory:
            # Fatal if standardized value is null
            cond = std_col.isNull()
            val = F.coalesce(raw_col, F.lit("MISSING_MANDATORY_VALUE"))
        else:
            # Failure if raw input existed but could not be parsed/mapped
            cond = raw_col.isNotNull() & (F.trim(raw_col) != "") & std_col.isNull()
            val = raw_col.cast(StringType())

        failure_structs.append(
            F.when(cond, F.struct(F.lit(name).alias("key"), val.alias("val"))).otherwise(None)
        )

    columns_to_remediate = F.map_from_entries(F.array_compact(F.array(*failure_structs)))

    # 6. Audit Trail for Fuzzy Canonical Corrections
    # Captures (field, record_id, raw, matched, distance) for every successful fuzzy
    # correction in the batch.
    fuzzy_audit_entries = F.array_compact(
        F.array(
            _fuzzy_audit_struct("weather_condition", record_id, raw_weather, effective_fuzzy_weather),
            _fuzzy_audit_struct("lighting_condition", record_id, raw_lighting, effective_fuzzy_lighting),
            _fuzzy_audit_struct("street_direction", record_id, raw_street_direction, effective_fuzzy_dir),
            _fuzzy_audit_struct("crash_month", record_id, raw_crash_month, effective_fuzzy_month),
            _fuzzy_audit_struct("crash_day_of_week", record_id, raw_crash_day, effective_fuzzy_day),
        )
    )

    # 7. Project standardized columns (exact 55 Silver columns + failure/audit/replay lineage)
    return df.select(
        F.col("record_id"),
        F.col("batch_id"),
        F.col("table_name"),
        F.col("ingestion_ts"),
        F.col("topic"),
        F.col("kafka_timestamp"),
        F.col("crash_record_id"),
        std_crash_date.alias("crash_date"),
        std_posted_speed_limit.alias("posted_speed_limit"),
        F.col("traffic_control_device"),
        F.col("device_condition"),
        std_weather.alias("weather_condition"),
        std_lighting.alias("lighting_condition"),
        F.col("first_crash_type"),
        F.col("trafficway_type"),
        F.col("alignment"),
        F.col("roadway_surface_cond"),
        F.col("road_defect"),
        F.col("report_type"),
        F.col("crash_type"),
        F.col("private_property_i"),
        std_hit_and_run.alias("hit_and_run_i"),
        std_damage.alias("damage"),
        std_date_police_notified.alias("date_police_notified"),
        F.col("prim_contributory_cause"),
        F.col("sec_contributory_cause"),
        F.col("street_no"),
        std_street_direction.alias("street_direction"),
        F.col("street_name"),
        F.col("beat_of_occurrence"),
        std_num_units.alias("num_units"),
        std_crash_month.alias("crash_month"),
        F.col("most_severe_injury"),
        F.col("injuries_total"),
        F.col("injuries_fatal"),
        F.col("injuries_incapacitating"),
        F.col("injuries_non_incapacitating"),
        F.col("injuries_reported_not_evident"),
        F.col("injuries_no_indication"),
        F.col("injuries_unknown"),
        std_crash_hour.alias("crash_hour"),
        std_crash_day_of_week.alias("crash_day_of_week"),
        F.col("idot_control_no"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("location"),
        F.col("intersection_related_i"),
        F.col("crash_date_est_i"),
        F.col("photos_taken_i"),
        F.col("statements_taken_i"),
        F.col("work_zone_i"),
        F.col("work_zone_type"),
        F.col("lane_cnt"),
        F.col("workers_present_i"),
        F.col("dooring_i"),
        columns_to_remediate.alias("columns_to_remediate"),
        fuzzy_audit_entries.alias("_fuzzy_audit_entries"),
        raw_record.alias("raw_record"),
    )


def _txn_write(df: DataFrame, path: str, table_tag: str, batch_id: int, merge_schema: bool = False):
    """Idempotent Delta append: a replayed (appId, batch_id) pair is skipped by Delta."""
    writer = (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", f"{TXN_APP_ID}:{table_tag}")
        .option("txnVersion", batch_id)
    )
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.save(path)


def _dead_letter_batch(batch_df: DataFrame, batch_id: int, exc: BaseException) -> None:
    """
    Park the raw batch plus the Spark error for the small-LLM triage node.
    The batch is stored as JSON so the dead-letter schema stays stable even when
    the failure is schema drift in Silver 1.
    """
    trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    cols = [batch_df[c] for c in batch_df.columns]
    out = (
        batch_df.select(F.to_json(F.struct(*cols)).alias("raw_json"))
        .withColumn("record_id", F.get_json_object("raw_json", "$.record_id"))
        .withColumn("stage", F.lit("transform"))
        .withColumn("error_type", F.lit(type(exc).__name__))
        .withColumn("error_message", F.lit(str(exc)[:MAX_ERROR_CHARS]))
        .withColumn("error_trace", F.lit(trace[-MAX_TRACE_CHARS:]))
        .withColumn("silver2_batch_id", F.lit(str(batch_id)))
        .withColumn("failed_at", F.current_timestamp())
        .withColumn("healing_status", F.lit("PENDING_LLM"))
    )
    try:
        _txn_write(out, DEAD_LETTER_PATH, "dead_letter", batch_id, merge_schema=True)
    except Exception as dl_exc:  # could not even park the batch: stop, let Spark replay it
        raise RuntimeError(
            f"[silver2] Batch {batch_id}: transform failed AND dead-letter write failed"
        ) from dl_exc
    print(
        f"[silver2] Batch {batch_id}: transform failed ({type(exc).__name__}); "
        f"batch parked at {DEAD_LETTER_PATH} for LLM triage"
    )


def make_batch_processor(spark: SparkSession):
    """Returns the foreachBatch function with access to spark session."""

    def process_micro_batch(batch_df: DataFrame, batch_id: int):
        # 1. Reload the latest committed canonical mappings per micro-batch
        mapping_dict = load_mappings_dict(spark, REF_MAPPINGS_PATH)

        # 2. TRANSFORM stage: build, persist and materialize in one go.
        #    Anything that blows up here is a data/schema problem -> dead letter.
        transformed = None
        try:
            transformed = standardize_silver2(batch_df, mapping_dict).persist()
            stats = transformed.agg(
                F.count(F.lit(1)).alias("total"),
                F.sum(F.when(F.size("columns_to_remediate") > 0, 1).otherwise(0)).alias("quarantined"),
                F.sum(F.when(F.size("_fuzzy_audit_entries") > 0, 1).otherwise(0)).alias("fuzzy"),
            ).first()
        except Exception as exc:
            if transformed is not None:
                transformed.unpersist()
            _dead_letter_batch(batch_df, batch_id, exc)
            # Classify only after the raw batch is durably preserved (IC2/IC4).
            from qwen_classifier.integration import capture_exception
            capture_exception(exc, job_name="Silver2", batch_id=batch_id,
                              dead_letter_path=DEAD_LETTER_PATH)
            return

        # 3. WRITE stage: idempotent writes; errors propagate so Spark replays the batch
        try:
            total = int(stats["total"] or 0)
            n_quarantined = int(stats["quarantined"] or 0)
            n_fuzzy = int(stats["fuzzy"] or 0)
            if total == 0:
                return

            failed = F.size(F.col("columns_to_remediate")) > 0

            # Clean rows: drop lineage/audit/replay columns before writing to Silver 2
            # Adds zero extra columns to Silver 2
            if total - n_quarantined > 0:
                valid_df = transformed.filter(~failed).drop(
                    "columns_to_remediate", "_fuzzy_audit_entries", "raw_record"
                )
                _txn_write(valid_df, SILVER2_PATH, "valid", batch_id)

            # Quarantined rows keep columns_to_remediate and the original raw_record
            if n_quarantined > 0:
                quarantine_out = (
                    transformed.filter(failed)
                    .drop("_fuzzy_audit_entries")
                    .withColumn("quarantined_at", F.current_timestamp())
                    .withColumn("silver2_batch_id", F.lit(str(batch_id)))
                    .withColumn("healing_status", F.lit("PENDING"))
                )
                _txn_write(quarantine_out, QUARANTINE_PATH, "quarantine", batch_id, merge_schema=True)
                print(
                    f"[silver2] Batch {batch_id}: quarantined {n_quarantined} of {total} records "
                    f"at {QUARANTINE_PATH}"
                )

            # Log fuzzy corrections: (field, record_id, raw, matched, distance)
            if n_fuzzy > 0:
                audit_df = (
                    transformed.filter(F.size(F.col("_fuzzy_audit_entries")) > 0)
                    .select(
                        F.explode("_fuzzy_audit_entries").alias("entry"),
                        F.lit(str(batch_id)).alias("batch_id"),
                        F.current_timestamp().alias("corrected_at"),
                    )
                    .select(
                        F.col("entry.field").alias("field"),
                        F.col("entry.record_id").alias("record_id"),
                        F.col("entry.raw").alias("raw"),
                        F.col("entry.matched").alias("matched"),
                        F.col("entry.distance").cast(IntegerType()).alias("distance"),
                        F.col("corrected_at"),
                        F.col("batch_id"),
                    )
                )
                _txn_write(audit_df, FUZZY_AUDIT_PATH, "fuzzy_audit", batch_id, merge_schema=True)
                print(
                    f"[silver2] Batch {batch_id}: logged fuzzy corrections to {FUZZY_AUDIT_PATH}"
                )
        finally:
            transformed.unpersist()

    return process_micro_batch


def _require_spark_version(spark: SparkSession) -> None:
    major_minor = tuple(int(p) for p in spark.version.split(".")[:2])
    if major_minor < MIN_SPARK_VERSION:
        raise RuntimeError(
            f"Silver 2 needs Spark >= {'.'.join(map(str, MIN_SPARK_VERSION))} "
            f"(try_to_timestamp, array_compact); running {spark.version}"
        )


def start(spark: SparkSession, upstream: Optional[StreamingQuery] = None) -> StreamingQuery:
    """Start the Silver 2 query on `spark`; `upstream` is the Silver 1 query if it shares this driver."""
    _require_spark_version(spark)

    # Initialize reference canonical mappings if table does not exist
    init_ref_mappings(spark, REF_MAPPINGS_PATH)

    # Initialize fuzzy audit Delta table if not already present
    init_fuzzy_audit(spark, FUZZY_AUDIT_PATH)

    # Wait for upstream Silver 1 Delta table
    wait_for_delta_table(SILVER1_PATH, upstream=upstream)

    reader = spark.readStream.format("delta")
    if SILVER1_SKIP_CHANGE_COMMITS:
        reader = reader.option("skipChangeCommits", "true")
    silver1_stream = reader.load(SILVER1_PATH)

    batch_processor = make_batch_processor(spark)

    # foreachBatch owns all writes, so no sink format is set on the writeStream
    query = (
        silver1_stream.writeStream.queryName("silver2")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(batch_processor)
        .start()
    )

    print(
        f"[silver2] Streaming {SILVER1_PATH} -> {SILVER2_PATH} "
        f"(Quarantine: {QUARANTINE_PATH}, Audit: {FUZZY_AUDIT_PATH}, "
        f"Dead letter: {DEAD_LETTER_PATH})"
    )
    return query


def main():
    start(build_spark("Silver2StandardizeTransform")).awaitTermination()


if __name__ == "__main__":
    main()