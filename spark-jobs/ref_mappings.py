"""
Reference Canonical Mappings Management.

Handles seed definitions, Delta table initialization, and dictionary lookups
for categorical standardization in Silver 2 and self-healing LLM loops.
"""

from datetime import datetime, timezone
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REF_MAPPINGS_SCHEMA = StructType(
    [
        StructField("field_name", StringType(), False),
        StructField("raw_synonym", StringType(), False),
        StructField("canonical_value", StringType(), False),
        StructField("added_by", StringType(), False),
        StructField("added_at", TimestampType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("source_batch_id", StringType(), True),
    ]
)

# record_id lets every fuzzy correction be traced back to its Silver row
# (and to its lineage node in Neo4j).
FUZZY_AUDIT_SCHEMA = StructType(
    [
        StructField("field", StringType(), False),
        StructField("record_id", StringType(), True),
        StructField("raw", StringType(), False),
        StructField("matched", StringType(), False),
        StructField("distance", IntegerType(), False),
        StructField("corrected_at", TimestampType(), False),
        StructField("batch_id", StringType(), True),
    ]
)

# Canonical seed anchors for closed-vocabulary fuzzy matching.
# Guardrail: Anchors are seed values only; promoted entries must NEVER become anchors
# to avoid attracting other typos.
# ==============================================================================
# SINGLE SOURCE OF TRUTH: CANONICAL REFERENCE TAXONOMY
# ==============================================================================
# Maps field_name -> {
#     CANONICAL_TARGET_VALUE: [list_of_synonyms_and_abbreviations]
# }
# Any edit here automatically updates CANONICAL_ANCHORS, ANCHOR_TO_CANONICAL,
# and SEED_ENTRIES consistently across the entire pipeline.
CANONICAL_TAXONOMY: Dict[str, Dict[str, List[str]]] = {
    "weather_condition": {
        "CLEAR": ["C", "CLEAR WEATHER"],
        "RAIN": ["R", "RAINY"],
        "SNOW": [],
        "FOG": [],
        "UNKNOWN": ["U", "NOT KNOWN"],
        "CLOUDY": ["CLOUDY/OVERCAST", "O", "OVERCAST"],
    },
    "lighting_condition": {
        "DAYLIGHT": ["D", "MORNING", "DAY"],
        "DARKNESS, LIGHTED ROAD": ["N, L", "NIGHT, LIGHTED", "NIGHT WITH LIGHT"],
        "DARKNESS": ["N", "NIGHT"],
        "UNKNOWN": ["U", "NOT KNOWN"],
    },
    "street_direction": {
        "NORTH": ["NOR", "N"],
        "SOUTH": ["SOU", "S"],
        "EAST": ["EAS", "E"],
        "WEST": ["WES", "W"],
    },
    "crash_month": {
        "JANUARY": ["JAN", "1", "01"],
        "FEBRUARY": ["FEB", "2", "02"],
        "MARCH": ["MAR", "3", "03"],
        "APRIL": ["APR", "4", "04"],
        "MAY": ["MAY", "5", "05"],
        "JUNE": ["JUN", "6", "06"],
        "JULY": ["JUL", "7", "07"],
        "AUGUST": ["AUG", "8", "08"],
        "SEPTEMBER": ["SEP", "9", "09"],
        "OCTOBER": ["OCT", "10"],
        "NOVEMBER": ["NOV", "11"],
        "DECEMBER": ["DEC", "12"],
    },
    "crash_day_of_week": {
        "SUNDAY": ["SUN", "SU", "1", "01"],
        "MONDAY": ["MON", "MO", "2", "02"],
        "TUESDAY": ["TUE", "TU", "3", "03"],
        "WEDNESDAY": ["WED", "WE", "4", "04"],
        "THURSDAY": ["THU", "TH", "5", "05"],
        "FRIDAY": ["FRI", "FR", "6", "06"],
        "SATURDAY": ["SAT", "SA", "7", "07"],
    },
    "hit_and_run_i": {
        "TRUE": ["Y", "YES", "RIGHT", "T", "1"],
        "FALSE": ["N", "NO", "WRONG", "F", "0"],
    },
    "damage": {
        "OVER $1,500": ["> $1500", "HIGH"],
        "$501 - $1,500": ["$501 - $1500", "MEDIUM"],
        "$500 OR LESS": ["<= $500", "≤ $500", "LOW"],
    },
    "num_units": {
        "1.0": ["ONE"],
        "2.0": ["TWO"],
        "3.0": ["THREE"],
        "4.0": ["FOUR"],
        "5.0": ["FIVE"],
        "6.0": ["SIX"],
    },
}

# Closed-vocabulary fields eligible for Levenshtein fuzzy distance matching
FUZZY_CANONICAL_FIELDS = [
    "weather_condition",
    "lighting_condition",
    "street_direction",
    "crash_month",
    "crash_day_of_week",
]

# Canonical seed anchors for fuzzy matching (auto-derived from taxonomy)
CANONICAL_ANCHORS: Dict[str, List[str]] = {
    field: list(CANONICAL_TAXONOMY[field].keys())
    for field in FUZZY_CANONICAL_FIELDS
}

# Anchor -> canonical stored value (shared by Silver 2 and promotion)
ANCHOR_TO_CANONICAL: Dict[str, Dict[str, str]] = {
    field: {anchor: anchor for anchor in CANONICAL_TAXONOMY[field].keys()}
    for field in FUZZY_CANONICAL_FIELDS
}


def fuzzy_canonical(col, anchors: List[str], max_frac: float = 0.25):
    """
    Deterministic Levenshtein matching against seed canonical anchors.

    Guardrails:
    1. Minimum length of 4: protects against single-character ambiguity like N/S/E/W.
    2. Distance scaled to length: limit = greatest(1, floor(length * max_frac)).
    3. Unique winner with a margin: second["d"] - best["d"] >= 2 (e.g. JUNY is 1 edit
       from both JUNE and JULY, so it stays in quarantine instead of guessing).
    4. Anchors are seed values only: dynamic or promoted entries must never be anchors.
    """
    c = F.col(col) if isinstance(col, str) else col
    key = F.upper(F.trim(c))
    scored = F.array_sort(
        F.transform(
            F.array(*[F.lit(a) for a in anchors]),
            lambda a: F.struct(F.levenshtein(key, a).alias("d"), a.alias("v")),
        )
    )
    best, second = scored[0], scored[1]
    # A single-anchor list has no runner-up: treat it as infinitely far away
    second_d = F.coalesce(second["d"], F.lit(1000))
    limit = F.greatest(F.lit(1), F.floor(F.length(key) * max_frac))
    return F.when(
        (F.length(key) >= 4) & (best["d"] <= limit) & (second_d - best["d"] >= 2),
        best["v"],
    )


# Initial seed vocabulary extracted from original Silver 2 SQL rules.
# Deliberately NOT seeded: two-letter month abbreviations that are ambiguous
# ("MA" = March/May, "JU" = June/July). Those go to quarantine instead of guessing.
# Initial seed vocabulary extracted dynamically from CANONICAL_TAXONOMY.
# Deliberately NOT seeded: two-letter month abbreviations that are ambiguous
# ("MA" = March/May, "JU" = June/July). Those go to quarantine instead of guessing.
SEED_ENTRIES: List[Tuple[str, str, str]] = []
for _field, _groups in CANONICAL_TAXONOMY.items():
    for _canonical, _synonyms in _groups.items():
        # Canonical target maps to itself
        SEED_ENTRIES.append((_field, _canonical, _canonical))
        # Known abbreviations and synonyms map to canonical target
        for _syn in _synonyms:
            SEED_ENTRIES.append((_field, _syn, _canonical))


def _is_delta_table(spark: SparkSession, path: str) -> bool:
    """True if `path` holds a Delta table (works for local, HDFS and object stores)."""
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def init_ref_mappings(spark: SparkSession, ref_path: str) -> None:
    """Initialize the ref_canonical_mappings Delta table if not already present."""
    if not _is_delta_table(spark, ref_path):
        print(f"[ref_mappings] Initializing canonical reference table at {ref_path}...")
        now = datetime.now(timezone.utc)
        rows = [
            (
                field_name,
                raw_synonym.strip().upper(),
                canonical_val,
                "seed",
                now,
                1.0,
                None,
            )
            for field_name, raw_synonym, canonical_val in SEED_ENTRIES
        ]
        df = spark.createDataFrame(rows, schema=REF_MAPPINGS_SCHEMA)
        df.write.format("delta").mode("overwrite").save(ref_path)
        print(f"[ref_mappings] Initialized with {len(rows)} seed rules.")


def load_mappings_dict(spark: SparkSession, ref_path: str) -> Dict[str, Dict[str, str]]:
    """
    Load the latest committed canonical mappings from Delta into a nested dictionary:
    { field_name: { UPPER_RAW_SYNONYM: CANONICAL_VALUE } }

    Rows are applied oldest-first so that if the same (field, synonym) was ever
    written twice with different canonical values, the most recent one wins
    deterministically.
    """
    ref_df = spark.read.format("delta").load(ref_path)
    collected = (
        ref_df.select("field_name", "raw_synonym", "canonical_value", "added_at")
        .orderBy("added_at")
        .collect()
    )
    mapping_dict: Dict[str, Dict[str, str]] = {}
    for row in collected:
        field = row.field_name
        synonym = (row.raw_synonym or "").strip().upper()
        if not field or not synonym:
            continue
        mapping_dict.setdefault(field, {})[synonym] = row.canonical_value
    return mapping_dict


def overlay_mappings(
    base: Dict[str, Dict[str, str]],
    overrides: Dict[str, Dict[str, str]],
) -> Dict[str, Dict[str, str]]:
    """
    Return a copy of `base` with `overrides` applied (in memory only, nothing is
    written to Delta).

    Used by the Validate step of the healing loop: an LLM-proposed fix such as
    {"weather_condition": {"MISTY DARK": "CLOUDY"}} can be tested by replaying the
    quarantined raw rows through standardize_silver2 with the overlay, BEFORE the
    value has reached the promotion threshold in ref_canonical_mappings.
    """
    merged = {field: dict(synonyms) for field, synonyms in base.items()}
    for field, synonyms in overrides.items():
        target = merged.setdefault(field, {})
        for raw, canonical in synonyms.items():
            key = (raw or "").strip().upper()
            if key:
                target[key] = canonical
    return merged


def init_fuzzy_audit(spark: SparkSession, audit_path: str) -> None:
    """Initialize the silver2_fuzzy_audit Delta table if not already present."""
    if not _is_delta_table(spark, audit_path):
        print(f"[ref_mappings] Initializing fuzzy audit table at {audit_path}...")
        df = spark.createDataFrame([], schema=FUZZY_AUDIT_SCHEMA)
        df.write.format("delta").mode("overwrite").save(audit_path)
        print(f"[ref_mappings] Initialized fuzzy audit table at {audit_path}.")


def promote_fuzzy_variants(
    spark: SparkSession,
    audit_path: str,
    ref_path: str,
    threshold: int = 5,
) -> int:
    """
    Scans the fuzzy audit Delta table for recurring typos/variants.
    If a (field, raw, matched) pattern appears >= threshold times, promote it
    into ref_canonical_mappings so future lookups succeed via exact match O(1).

    The insert is a Delta MERGE keyed on (field_name, raw_synonym), so running
    promotion twice (or concurrently) can never create duplicate rules.

    IMPORTANT: Promoted entries are NEVER added to CANONICAL_ANCHORS to prevent
    bad promotions from attracting other typos.
    """
    from delta.tables import DeltaTable

    if not _is_delta_table(spark, audit_path):
        return 0

    audit_df = spark.read.format("delta").load(audit_path)
    if audit_df.isEmpty():
        return 0

    # Aggregate by field, normalized raw synonym, and matched anchor
    grouped = (
        audit_df.withColumn("raw_clean", F.upper(F.trim(F.col("raw"))))
        .groupBy("field", "raw_clean", "matched")
        .agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") >= threshold)
    )

    candidates = grouped.collect()
    if not candidates:
        return 0

    # Skip rules that already exist (also keeps the log output accurate)
    ref_df = spark.read.format("delta").load(ref_path)
    existing_synonyms = set(
        (r.field_name, r.raw_synonym)
        for r in ref_df.select("field_name", "raw_synonym").collect()
    )

    now = datetime.now(timezone.utc)
    new_rows = []
    for row in candidates:
        f_name = row.field
        raw_syn = row.raw_clean
        matched = row.matched
        cnt = row.cnt

        if not raw_syn or (f_name, raw_syn) in existing_synonyms:
            continue

        # Map anchor to canonical target value (identity for weather/lighting)
        canonical_val = ANCHOR_TO_CANONICAL.get(f_name, {}).get(matched, matched)

        new_rows.append(
            (
                f_name,
                raw_syn,
                canonical_val,
                "fuzzy_promotion",
                now,
                1.0,
                f"threshold_{threshold}_count_{cnt}",
            )
        )

    if not new_rows:
        return 0

    new_df = spark.createDataFrame(new_rows, schema=REF_MAPPINGS_SCHEMA)
    (
        DeltaTable.forPath(spark, ref_path)
        .alias("t")
        .merge(
            new_df.alias("s"),
            "t.field_name = s.field_name AND t.raw_synonym = s.raw_synonym",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(
        f"[ref_mappings] Promoted {len(new_rows)} recurring fuzzy variants into {ref_path}."
    )
    for r in new_rows:
        print(f"  -> Promoted '{r[1]}' for field '{r[0]}' -> '{r[2]}' ({r[6]})")
    return len(new_rows)