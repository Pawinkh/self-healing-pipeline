"""
Delta Lakehouse Parquet Inspector & Lineage Visualizer (v2)
===========================================================

FUNCTION:
  Reads and inspects Delta Lake Parquet tables directly on the host using DuckDB
  without needing a running Spark JVM. Supports rich terminal card views, tabular
  tables, JSON dumps, schema analysis, cross-layer record lineage tracing (Bronze ->
  Silver 1 -> Silver 2 -> Gold / Quarantine), and interactive HTML web views.

BASIC USAGE:
  python "Read Data/read_parquet_v2.py" [LAYER | trace] [OPTIONS]
  (or from within 'Read Data/': python read_parquet_v2.py [LAYER] [OPTIONS])

TARGET LAYERS:
  bronze      - Inspect raw Kafka ingest payload (./delta/bronze)
  silver1     - Inspect cleaned & typed layer (./delta/silver1)
  silver2     - Inspect standardized records (./delta/silver2)
  gold        - Inspect business-ready table (./delta/gold)
  quarantine  - Inspect quarantined/failed records (./delta/silver2_quarantine)
  audit       - Inspect fuzzy matching audit trail (./delta/silver2_fuzzy_audit)
  ref         - Inspect canonical dictionary rules (./delta/ref_canonical_mappings)
  trace       - Trace a single record_id across ALL layers simultaneously

COMMON COMMANDS & EXAMPLES:
  1. Quick Layer Preview (Card View of active synced record):
       python "Read Data/read_parquet_v2.py" silver2
       python "Read Data/read_parquet_v2.py" gold
       python "Read Data/read_parquet_v2.py" quarantine

  2. Cross-Layer Lineage / Journey Trace:
       python "Read Data/read_parquet_v2.py" trace
       (shows Bronze -> Silver 1 -> Silver 2 -> Gold transformations side-by-side)

  3. Trace or Inspect a Specific Record ID:
       python "Read Data/read_parquet_v2.py" trace --id <record_id>
       python "Read Data/read_parquet_v2.py" silver2 <record_id>

  4. Step / Navigate Records (maintains sync across layers):
       python "Read Data/read_parquet_v2.py" --next      # Go to next record
       python "Read Data/read_parquet_v2.py" --prev      # Go to previous record
       python "Read Data/read_parquet_v2.py" --random    # Pick a random record
       python "Read Data/read_parquet_v2.py" --reset     # Reset to default anchor

  5. Tabular Multi-Row View:
       python "Read Data/read_parquet_v2.py" silver2 --table 10
       python "Read Data/read_parquet_v2.py" ref --table 20

  6. Schema & Column Catalog:
       python "Read Data/read_parquet_v2.py" silver2 --schema

  7. Interactive Web Browser Viewer:
       python "Read Data/read_parquet_v2.py" gold --html
       python "Read Data/read_parquet_v2.py" trace --html

  8. Column Filtering:
       python "Read Data/read_parquet_v2.py" silver2 --col weather,lighting,crash_month

  9. Raw vs. Unpacked Bronze:
       python "Read Data/read_parquet_v2.py" bronze        # Unpacked JSON payload
       python "Read Data/read_parquet_v2.py" bronze --raw  # Raw Kafka fields
"""

import sys
import os
import glob
import json
import re
import webbrowser
from functools import lru_cache
from typing import Optional, Dict, Any, Tuple, List

# Fix Windows encoding for unicode box drawing & symbols in terminal
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# pyrefly: ignore [missing-import]
import duckdb
import pandas as pd
from deltalake import DeltaTable

# Check Rich library for beautiful terminal formatting
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.syntax import Syntax
    from rich import box
    from rich.text import Text
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None

# Determine project root path dynamically
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
ACTIVE_ID_FILE = os.path.join(SCRIPT_DIR, ".active_id")
ACTIVE_LAYER_FILE = os.path.join(SCRIPT_DIR, ".active_layer")

def resolve_layer_path(relative_path: str) -> str:
    """Resolves a table folder whether script is executed from project root or Read Data/ folder."""
    clean_rel = relative_path.lstrip("./")
    from_root = os.path.join(PROJECT_ROOT, clean_rel)
    if os.path.isdir(from_root):
        return from_root
    if os.path.isdir(relative_path):
        return relative_path
    return from_root

# Mapping of Delta table folders for your pipeline layers
LAYERS = {
    "bronze": resolve_layer_path("./delta/bronze"),
    "silver1": resolve_layer_path("./delta/silver1"),
    "silver2": resolve_layer_path("./delta/silver2"),
    "gold": resolve_layer_path("./delta/gold"),
    "audit": resolve_layer_path("./delta/silver2_fuzzy_audit"),
    "quarantine": resolve_layer_path("./delta/silver2_quarantine"),
    "ref": resolve_layer_path("./delta/ref_canonical_mappings"),
    # Aliases
    "silver": resolve_layer_path("./delta/silver1"),
    "silver_cars": resolve_layer_path("./delta/silver_cars"),
    "gold_car_overview": resolve_layer_path("./delta/gold_car_overview"),
}

def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"

@lru_cache(maxsize=None)
def layer_source(path: str) -> str:
    """
    DuckDB FROM-clause source for a Delta table folder (or a plain Parquet path/glob).

    A Delta folder also keeps files that compaction, MERGE or overwrite have
    logically removed, so globbing part-*.parquet would double-count rows. Only
    the files in the table's current snapshot are read. union_by_name lines up
    columns for tables written with mergeSchema (quarantine, audit).
    Raises FileNotFoundError if there is nothing to read yet.
    """
    if os.path.isdir(os.path.join(path, "_delta_log")):
        files = DeltaTable(path).file_uris()
        if not files:
            raise FileNotFoundError(f"Delta table at {path} has no data files yet")
        file_list = "[" + ", ".join(_sql_str(f) for f in files) + "]"
    elif glob.glob(path):
        file_list = _sql_str(path)
    else:
        raise FileNotFoundError(f"No Delta table or Parquet files at {path}")
    return f"read_parquet({file_list}, union_by_name = true)"

def find_bronze_row(src: str, rec_id: str) -> pd.DataFrame:
    """
    Bronze row for rec_id. The producer uses record_id as the Kafka key, so try an
    exact kafka_key match first. The broad match (key prefix, or the id anywhere in
    the raw JSON, e.g. a crash_record_id) scans every raw_value string, so it only
    runs when the exact match finds nothing.
    """
    df = duckdb.sql(f"SELECT * FROM {src} WHERE kafka_key = '{rec_id}' LIMIT 1").df()
    if len(df) == 0:
        df = duckdb.sql(
            f"SELECT * FROM {src} "
            f"WHERE kafka_key LIKE '{rec_id}%' OR raw_value LIKE '%{rec_id}%' LIMIT 1"
        ).df()
    return df

TRACE_ALIASES = {"trace", "pipeline", "all", "compare", "lineage", "journey"}

def get_active_layer() -> str:
    """Gets the most recently viewed pipeline layer, or defaults to silver1."""
    if os.path.exists(ACTIVE_LAYER_FILE):
        try:
            with open(ACTIVE_LAYER_FILE, "r", encoding="utf-8") as f:
                l = f.read().strip()
                if l in LAYERS or l in TRACE_ALIASES:
                    return l
        except Exception:
            pass
    return "silver1"

def set_active_layer(layer_name: str) -> None:
    """Persists the active pipeline layer."""
    try:
        os.makedirs(os.path.dirname(ACTIVE_LAYER_FILE), exist_ok=True)
        with open(ACTIVE_LAYER_FILE, "w", encoding="utf-8") as f:
            f.write(layer_name.strip())
    except Exception:
        pass

def get_active_id() -> str:
    """Gets the currently synced record ID from cache, or initializes with a default anchor."""
    if os.path.exists(ACTIVE_ID_FILE):
        try:
            with open(ACTIVE_ID_FILE, "r", encoding="utf-8") as f:
                rec_id = f.read().strip()
                if rec_id:
                    return rec_id
        except Exception:
            pass

    # Find a default anchor ID present in silver1 / gold
    try:
        s1_src = layer_source(LAYERS["silver1"])
        anchor = duckdb.sql(f"SELECT record_id FROM {s1_src} WHERE record_id IS NOT NULL LIMIT 1").fetchone()
        if anchor and anchor[0]:
            set_active_id(anchor[0])
            return anchor[0]
    except Exception:
        pass

    # Fallback known anchor ID
    fallback_id = "e8e76ac3-0376-4540-a532-acf3d735216b"
    set_active_id(fallback_id)
    return fallback_id

def set_active_id(rec_id: str) -> None:
    """Saves the active record ID so all layers track the same data."""
    try:
        os.makedirs(os.path.dirname(ACTIVE_ID_FILE), exist_ok=True)
        with open(ACTIVE_ID_FILE, "w", encoding="utf-8") as f:
            f.write(rec_id.strip())
    except Exception:
        pass

def navigate_active_id(direction: str, current_id: str, layer: str = "silver1") -> str:
    """Navigates to next, previous, or random record in the dataset and updates active ID."""
    nav_layer = layer if layer in LAYERS else "silver1"
    if nav_layer == "ref":
        nav_layer = "silver1"

    pat = LAYERS.get(nav_layer, LAYERS["silver1"])
    id_col = "kafka_key" if nav_layer == "bronze" else "record_id"
    new_id = None
    try:
        src = layer_source(pat)
        if direction == "next":
            res = duckdb.sql(f"SELECT {id_col} FROM {src} WHERE {id_col} > '{current_id}' ORDER BY {id_col} ASC LIMIT 1").fetchone()
            if not res or not res[0]:
                res = duckdb.sql(f"SELECT {id_col} FROM {src} ORDER BY {id_col} ASC LIMIT 1").fetchone()
            if res:
                new_id = str(res[0])
        elif direction == "prev":
            res = duckdb.sql(f"SELECT {id_col} FROM {src} WHERE {id_col} < '{current_id}' ORDER BY {id_col} DESC LIMIT 1").fetchone()
            if not res or not res[0]:
                res = duckdb.sql(f"SELECT {id_col} FROM {src} ORDER BY {id_col} DESC LIMIT 1").fetchone()
            if res:
                new_id = str(res[0])
        elif direction == "random":
            res = duckdb.sql(f"SELECT {id_col} FROM {src} ORDER BY random() LIMIT 1").fetchone()
            if res:
                new_id = str(res[0])
    except Exception:
        pass

    if new_id:
        set_active_id(new_id)
        return new_id
    return current_id

def unpack_bronze_record(b_df: pd.DataFrame) -> pd.DataFrame:
    """Unpacks Bronze raw_value JSON payload into clean tabular columns matching Silver/Gold."""
    if len(b_df) == 0:
        return b_df
    rows = []
    for _, row in b_df.iterrows():
        raw_val = row.get("raw_value")
        if isinstance(raw_val, str) and raw_val.strip().startswith("{"):
            try:
                parsed = json.loads(raw_val)
                payload = parsed.get("payload", {})
                flattened = {
                    "record_id": parsed.get("record_id", row.get("kafka_key")),
                    "batch_id": parsed.get("batch_id"),
                    "table_name": parsed.get("table_name"),
                    "ingestion_ts": parsed.get("ingestion_ts"),
                    "topic": row.get("topic"),
                    "partition": row.get("partition"),
                    "offset": row.get("offset"),
                    "kafka_timestamp": str(row.get("kafka_timestamp")),
                    "ingested_at": str(row.get("ingested_at")),
                    **payload
                }
                rows.append(flattened)
                continue
            except Exception:
                pass
        rows.append(row.to_dict())
    return pd.DataFrame(rows)

def format_val(val, max_len: Optional[int] = None) -> str:
    """Format cell value with clean styles for NULLs, numbers, and booleans."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or str(val) == "nan":
        return "[dim italic]null[/dim italic]" if RICH_AVAILABLE else "null"
    s = str(val).strip()
    if s == "":
        return "[dim]—[/dim]" if RICH_AVAILABLE else "—"
    if s.lower() in ("true", "false") and RICH_AVAILABLE:
        return f"[bold yellow]{s}[/bold yellow]"
    try:
        float(s)
        return f"[bright_cyan]{s}[/bright_cyan]" if RICH_AVAILABLE else s
    except ValueError:
        pass
    if max_len and len(s) > max_len:
        s = s[:max_len - 3] + "..."
    return s

def print_banner(target_name: str, total_count: int, total_cols: int, preview_count: int, mode: str, active_id: Optional[str] = None):
    """Prints a styled header with dataset metadata and synced ID indicator."""
    if RICH_AVAILABLE:
        sync_str = f"   │   [bold cyan]Active ID (Synced):[/bold cyan] [bold green]{active_id}[/bold green]" if active_id else ""
        summary_text = (
            f"[bold cyan]Layer / Target:[/bold cyan] [white]{target_name.upper()}[/white]   │   "
            f"[bold cyan]Total Rows:[/bold cyan] [green]{total_count:,}[/green]   │   "
            f"[bold cyan]Columns:[/bold cyan] [yellow]{total_cols}[/yellow]   │   "
            f"[bold cyan]Mode:[/bold cyan] [magenta]{mode.upper()}[/magenta]"
            f"{sync_str}"
        )
        console.print(Panel(summary_text, title="[bold white on blue] 📊 Medallion Parquet Inspector [/bold white on blue]", border_style="blue"))
    else:
        sync_str = f" | Active ID (Synced): {active_id}" if active_id else ""
        print(f"\n=== Parquet Inspector: {target_name.upper()} ({total_count:,} rows, {total_cols} columns) [Mode: {mode}]{sync_str} ===")

def render_schema_view(df: pd.DataFrame):
    """Displays all columns, data types, null counts, and a clean sample value in a compact table."""
    if RICH_AVAILABLE:
        table = Table(
            title=f"Dataset Schema & Column Catalog ({len(df.columns)} Columns)",
            header_style="bold magenta",
            box=box.ROUNDED,
            border_style="blue",
            show_lines=False
        )
        table.add_column("#", style="dim", justify="right", width=3)
        table.add_column("Column Name", style="bold cyan", width=25)
        table.add_column("Type", style="green", width=10)
        table.add_column("Populated", justify="right", style="yellow", width=10)
        table.add_column("Sample Value", style="white")

        c_width = console.width if console else 80
        for idx, col in enumerate(df.columns, start=1):
            dtype = str(df[col].dtype)
            non_null = df[col].notna().sum()
            pct = (non_null / len(df)) * 100 if len(df) > 0 else 0
            first_valid = df[col].dropna()
            sample = str(first_valid.iloc[0]) if len(first_valid) > 0 else "[dim]null[/dim]"
            pop_str = f"{pct:.0f}%" if c_width < 100 else f"{non_null}/{len(df)} ({pct:.0f}%)"
            table.add_row(str(idx), col, dtype, pop_str, sample)

        console.print(table)
    else:
        print(f"{'#':<4} {'Column Name':<30} {'Data Type':<15} {'Sample Value'}")
        print("-" * 75)
        for idx, col in enumerate(df.columns, start=1):
            first_valid = df[col].dropna()
            sample = str(first_valid.iloc[0]) if len(first_valid) > 0 else "null"
            print(f"{idx:<4} {col:<30} {str(df[col].dtype):<15} {sample[:30]}")

def render_card_view(df: pd.DataFrame, force_vertical: bool = False, force_grid: bool = False, custom_title: Optional[str] = None):
    """Renders rows as individual cards with all columns displayed cleanly without messy horizontal overflow."""
    cols = list(df.columns)
    c_width = console.width if RICH_AVAILABLE else 80
    use_split = (not force_vertical) and (force_grid or (c_width >= 100 and len(cols) > 10))

    for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
        card_title = custom_title or f"[bold green]Record #{row_idx}[/bold green] [dim](Row {row_idx} of {len(df)} previewed - {len(cols)} columns)[/dim]"
        if RICH_AVAILABLE:
            if use_split:
                mid = (len(cols) + 1) // 2
                col_l = [(i + 1, cols[i]) for i in range(mid)]
                col_r = [(i + 1, cols[i]) for i in range(mid, len(cols))]

                t = Table(box=None, padding=(0, 1), show_header=True, header_style="bold magenta")
                t.add_column("#", style="dim", justify="right", width=3)
                t.add_column("Field", style="bold cyan", width=26, no_wrap=True)
                t.add_column("Value", style="white", width=22, no_wrap=True)
                t.add_column("│", style="dim", width=1)
                t.add_column("#", style="dim", justify="right", width=3)
                t.add_column("Field", style="bold cyan", width=26, no_wrap=True)
                t.add_column("Value", style="white", width=22, no_wrap=True)

                for i in range(mid):
                    idx_1, name_1 = col_l[i]
                    val_1 = format_val(row[name_1], max_len=22)

                    if i < len(col_r):
                        idx_2, name_2 = col_r[i]
                        val_2 = format_val(row[name_2], max_len=22)
                        t.add_row(str(idx_1), name_1, val_1, "│", str(idx_2), name_2, val_2)
                    else:
                        t.add_row(str(idx_1), name_1, val_1, "│", "", "", "")
            else:
                t = Table(box=box.SIMPLE_HEAD, padding=(0, 1), show_header=True, header_style="bold magenta")
                t.add_column("#", style="dim", justify="right", width=4)
                t.add_column("Field Name", style="bold cyan", width=30, no_wrap=True)
                t.add_column("Value", style="white")
                for idx, col_name in enumerate(cols, start=1):
                    t.add_row(str(idx), col_name, format_val(row[col_name], max_len=80))

            console.print(Panel(
                t,
                title=card_title,
                border_style="bright_blue",
                expand=False
            ))
        else:
            print(f"\n--- {custom_title or f'Record #{row_idx}'} ({len(cols)} columns) ---")
            for idx, col_name in enumerate(cols, start=1):
                print(f"  [{idx:2d}] {col_name:<30} : {row[col_name]}")

def render_table_view(df: pd.DataFrame):
    """Renders rows as a horizontal table."""
    if RICH_AVAILABLE:
        table = Table(title="Tabular View", box=box.ROUNDED, header_style="bold cyan")
        table.add_column("#", style="dim", width=4)
        for col in df.columns:
            table.add_column(col, overflow="ellipsis", max_width=18)
        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            row_vals = [str(idx)] + [format_val(row[c]) for c in df.columns]
            table.add_row(*row_vals)
        console.print(table)
    else:
        print(df.to_string())

def render_json_view(df: pd.DataFrame):
    """Outputs pretty-printed JSON records with syntax highlighting."""
    records = df.to_dict(orient="records")
    json_str = json.dumps(records, indent=2, default=str)
    if RICH_AVAILABLE:
        console.print(Syntax(json_str, "json", theme="monokai", line_numbers=True))
    else:
        print(json_str)

def fetch_record_across_pipeline(rec_id: str, unpack_bronze: bool = True) -> Dict[str, Optional[pd.DataFrame]]:
    """Fetches the exact record by ID across all pipeline layers."""
    results = {}

    # Bronze
    try:
        b_df = find_bronze_row(layer_source(LAYERS['bronze']), rec_id)
        if len(b_df) > 0:
            results['bronze'] = unpack_bronze_record(b_df) if unpack_bronze else b_df
        else:
            results['bronze'] = None
    except Exception:
        results['bronze'] = None

    # Silver 1
    try:
        s1_df = duckdb.sql(
            f"SELECT * FROM {layer_source(LAYERS['silver1'])} "
            f"WHERE record_id = '{rec_id}' OR record_id LIKE '{rec_id}%' OR crash_record_id = '{rec_id}' OR crash_record_id LIKE '{rec_id}%' LIMIT 1"
        ).df()
        results['silver1'] = s1_df if len(s1_df) > 0 else None
    except Exception:
        results['silver1'] = None

    # Silver 2
    try:
        s2_df = duckdb.sql(
            f"SELECT * FROM {layer_source(LAYERS['silver2'])} "
            f"WHERE record_id = '{rec_id}' OR record_id LIKE '{rec_id}%' OR crash_record_id = '{rec_id}' OR crash_record_id LIKE '{rec_id}%' LIMIT 1"
        ).df()
        results['silver2'] = s2_df if len(s2_df) > 0 else None
    except Exception:
        results['silver2'] = None

    # Silver 2 Quarantine
    try:
        q_df = duckdb.sql(
            f"SELECT * FROM {layer_source(LAYERS['quarantine'])} "
            f"WHERE record_id = '{rec_id}' OR record_id LIKE '{rec_id}%' OR crash_record_id = '{rec_id}' OR crash_record_id LIKE '{rec_id}%' LIMIT 1"
        ).df()
        results['quarantine'] = q_df if len(q_df) > 0 else None
    except Exception:
        results['quarantine'] = None

    # Gold
    try:
        g_df = duckdb.sql(
            f"SELECT * FROM {layer_source(LAYERS['gold'])} "
            f"WHERE record_id = '{rec_id}' OR record_id LIKE '{rec_id}%' OR crash_record_id = '{rec_id}' OR crash_record_id LIKE '{rec_id}%' LIMIT 1"
        ).df()
        results['gold'] = g_df if len(g_df) > 0 else None
    except Exception:
        results['gold'] = None

    # Fuzzy Audit
    try:
        a_df = duckdb.sql(
            f"SELECT * FROM {layer_source(LAYERS['audit'])} "
            f"WHERE record_id = '{rec_id}' OR record_id LIKE '{rec_id}%' LIMIT 10"
        ).df()
        results['audit'] = a_df if len(a_df) > 0 else None
    except Exception:
        results['audit'] = None

    return results

def compute_layer_diffs(records: Dict[str, Optional[pd.DataFrame]]) -> List[Tuple[str, str, str, str, str]]:
    """Calculates fields that evolved or changed values across Bronze -> Silver1 -> Silver2 -> Gold."""
    b_row = records['bronze'].iloc[0].to_dict() if records['bronze'] is not None and len(records['bronze']) > 0 else {}
    s1_row = records['silver1'].iloc[0].to_dict() if records['silver1'] is not None and len(records['silver1']) > 0 else {}
    s2_row = records['silver2'].iloc[0].to_dict() if records['silver2'] is not None and len(records['silver2']) > 0 else {}
    g_row = records['gold'].iloc[0].to_dict() if records['gold'] is not None and len(records['gold']) > 0 else {}

    all_keys = list(s1_row.keys()) if s1_row else list(b_row.keys())
    ignored = {"topic", "kafka_timestamp", "ingestion_ts", "record_id", "batch_id", "table_name", "partition", "offset", "ingested_at"}

    diffs = []
    for k in all_keys:
        if k in ignored:
            continue
        bv = str(b_row.get(k, "")).strip()
        s1v = str(s1_row.get(k, "")).strip()
        s2v = str(s2_row.get(k, "")).strip()
        gv = str(g_row.get(k, "")).strip()

        # Compare valid distinct values
        vals = [v for v in [bv, s1v, s2v, gv] if v not in ("", "None", "nan", "<NA>")]
        if len(set(vals)) > 1:
            diffs.append((k, bv, s1v, s2v, gv))
    return diffs

def render_trace_view(rec_id: str, records: Dict[str, Optional[pd.DataFrame]], force_vertical: bool = False, force_grid: bool = False):
    """Renders the comprehensive cross-layer pipeline journey of a single record."""
    has_b = records['bronze'] is not None and len(records['bronze']) > 0
    has_s1 = records['silver1'] is not None and len(records['silver1']) > 0
    has_s2 = records['silver2'] is not None and len(records['silver2']) > 0
    has_q = records['quarantine'] is not None and len(records['quarantine']) > 0
    has_g = records['gold'] is not None and len(records['gold']) > 0

    # Pipeline Stepper Summary
    b_badge = "[bold green]✔ Ingested[/bold green]" if has_b else "[bold red]✘ Missing[/bold red]"
    s1_badge = "[bold green]✔ Cleaned[/bold green]" if has_s1 else "[bold red]✘ Missing[/bold red]"
    if has_s2:
        s2_badge = "[bold green]✔ Standardized (PASS)[/bold green]"
    elif has_q:
        s2_badge = "[bold red]⚠️ QUARANTINED (FAIL)[/bold red]"
    else:
        s2_badge = "[dim]— Not Reached[/dim]"
    g_badge = "[bold green]✔ Business Ready[/bold green]" if has_g else ("[dim]⊘ Skipped (Quarantine)[/dim]" if has_q else "[dim]— Not Reached[/dim]")

    if RICH_AVAILABLE:
        pipeline_diagram = (
            f"  [bold cyan]1. BRONZE[/bold cyan]           ➜   "
            f"[bold cyan]2. SILVER 1[/bold cyan]         ➜   "
            f"[bold cyan]3. SILVER 2[/bold cyan]                 ➜   "
            f"[bold cyan]4. GOLD[/bold cyan]\n"
            f"  {b_badge}        {s1_badge}          {s2_badge}      {g_badge}"
        )
        console.print(Panel(
            pipeline_diagram,
            title=f"[bold white on blue] 🔄 Medallion Pipeline Journey: {rec_id} [/bold white on blue]",
            border_style="bright_blue"
        ))
    else:
        print(f"\n=== Medallion Pipeline Journey: {rec_id} ===")
        print(f"Bronze: {'PASS' if has_b else 'FAIL'} -> Silver 1: {'PASS' if has_s1 else 'FAIL'} -> Silver 2: {'PASS' if has_s2 else ('QUARANTINE' if has_q else 'MISSING')} -> Gold: {'PASS' if has_g else 'SKIPPED'}")

    # If quarantined, highlight the failure details prominently!
    if has_q:
        q_row = records['quarantine'].iloc[0]
        remed = q_row.get("columns_to_remediate")
        if RICH_AVAILABLE:
            console.print(Panel(
                f"[bold red]Validation Failure Reason:[/bold red] [yellow]{remed}[/yellow]\n"
                f"[dim]This record was filtered out of Silver 2 & Gold Lakehouse tables and parked in delta/silver2_quarantine.[/dim]",
                title="[bold white on red] ⚠️ Quarantine Isolation Alert [/bold white on red]",
                border_style="red"
            ))
        else:
            print(f"\n[!] QUARANTINE REASON: {remed}")

    # Field Transformation History Table
    diffs = compute_layer_diffs(records)
    if diffs:
        if RICH_AVAILABLE:
            t_diff = Table(
                title=f"Field Transformation History across Pipeline ({len(diffs)} transformed attributes)",
                box=box.ROUNDED,
                header_style="bold magenta",
                border_style="blue"
            )
            t_diff.add_column("Field Name", style="bold cyan", width=25)
            t_diff.add_column("Bronze (Raw)", style="dim white", width=22)
            t_diff.add_column("Silver 1 (Clean)", style="yellow", width=22)
            t_diff.add_column("Silver 2 (Standard)", style="green", width=22)
            t_diff.add_column("Gold (Analytics)", style="bright_green", width=22)

            for k, bv, s1v, s2v, gv in diffs:
                t_diff.add_row(k, bv[:21], s1v[:21], s2v[:21], gv[:21])
            console.print(t_diff)
        else:
            print(f"\n--- Field Transformation History ({len(diffs)} fields) ---")
            print(f"{'Field':<25} {'Bronze (Raw)':<20} {'Silver 1 (Clean)':<20} {'Silver 2 (Std)':<20} {'Gold'}")
            print("-" * 105)
            for k, bv, s1v, s2v, gv in diffs:
                print(f"{k:<25} {bv[:18]:<20} {s1v[:18]:<20} {s2v[:18]:<20} {gv[:18]}")

    # Layer Cards
    if has_b:
        render_card_view(records['bronze'], force_vertical, force_grid, custom_title="[bold cyan]Layer 1: BRONZE (Raw Kafka Ingestion)[/bold cyan]")
    if has_s1:
        render_card_view(records['silver1'], force_vertical, force_grid, custom_title="[bold cyan]Layer 2: SILVER 1 (Cleaned & Flattened SQL)[/bold cyan]")
    if has_s2:
        render_card_view(records['silver2'], force_vertical, force_grid, custom_title="[bold green]Layer 3: SILVER 2 (Standardized & Canonicalized)[/bold green]")
    elif has_q:
        render_card_view(records['quarantine'], force_vertical, force_grid, custom_title="[bold red]Layer 3: QUARANTINE (Failed Validation)[/bold red]")
    if has_g:
        render_card_view(records['gold'], force_vertical, force_grid, custom_title="[bold yellow]Layer 4: GOLD (Business Analytics Ready)[/bold yellow]")

def generate_html_viewer(df: pd.DataFrame, target_name: str, total_count: int, active_id: Optional[str] = None) -> str:
    """Generates an interactive standalone HTML table with DataTables and automatically opens it."""
    out_dir = SCRIPT_DIR
    out_file = os.path.join(out_dir, "read_parquet_preview.html")

    columns = list(df.columns)
    records = df.to_dict(orient="records")

    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    sync_badge = f'<span class="badge bg-info fs-6">Active ID: {esc(active_id)}</span>' if active_id else ""

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Parquet Inspector: {target_name}</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
    <style>
        body {{
            background-color: #0b0f19;
            color: #e2e8f0;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            padding: 24px;
        }}
        .card-custom {{
            background-color: #111827;
            border: 1px solid #1f2937;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5);
        }}
        .table-responsive {{
            margin-top: 16px;
        }}
        table.dataTable {{
            color: #cbd5e1 !important;
            font-size: 0.85rem;
            white-space: nowrap;
        }}
        table.dataTable thead th {{
            background-color: #1f2937 !important;
            color: #38bdf8 !important;
            font-weight: 600;
            border-bottom: 2px solid #0284c7 !important;
        }}
        table.dataTable tbody tr {{
            background-color: #111827 !important;
        }}
        table.dataTable tbody tr:hover {{
            background-color: #1e293b !important;
        }}
        table.dataTable tbody td {{
            border-color: #1f2937 !important;
        }}
        .null-val {{
            color: #64748b;
            font-style: italic;
        }}
        .dataTables_info, .dataTables_paginate, .dataTables_length, .dataTables_filter {{
            color: #94a3b8 !important;
        }}
        .dataTables_filter input, .dataTables_length select {{
            background-color: #0b0f19 !important;
            border: 1px solid #374151 !important;
            color: #f3f4f6 !important;
            border-radius: 6px;
            padding: 4px 10px;
        }}
        .page-link {{
            background-color: #1f2937;
            border-color: #374151;
            color: #94a3b8;
        }}
        .page-item.active .page-link {{
            background-color: #0284c7;
            border-color: #0284c7;
            color: white;
        }}
    </style>
</head>
<body>
    <div class="container-fluid">
        <div class="card-custom">
            <div class="d-flex justify-content-between align-items-center mb-3">
                <div>
                    <h2 class="h4 mb-0 text-info">Parquet Dataset: {target_name.upper()}</h2>
                    <small class="text-secondary">Viewing {len(columns)} columns with full search & sorting</small>
                </div>
                <div class="d-flex gap-2">
                    {sync_badge}
                    <span class="badge bg-primary fs-6">Total Rows: {total_count:,}</span>
                    <span class="badge bg-success fs-6">Loaded: {len(df):,} rows</span>
                    <span class="badge bg-secondary fs-6">Columns: {len(columns)}</span>
                </div>
            </div>
            <div class="table-responsive">
                <table id="parquetTable" class="table table-bordered table-hover w-100">
                    <thead>
                        <tr>
                            <th>#</th>
                            {''.join(f'<th>{esc(c)}</th>' for c in columns)}
                        </tr>
                    </thead>
                    <tbody>
"""
    for idx, row in enumerate(records, start=1):
        tds = [f"<td>{idx}</td>"]
        for c in columns:
            v = row[c]
            if v is None or (isinstance(v, float) and pd.isna(v)) or str(v) == "nan":
                tds.append('<td class="null-val">null</td>')
            else:
                tds.append(f"<td>{esc(str(v))}</td>")
        html_content += f"<tr>{''.join(tds)}</tr>\n"

    html_content += """
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
    <script>
        $(document).ready(function() {
            $('#parquetTable').DataTable({
                scrollX: true,
                pageLength: 25,
                lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
                order: []
            });
        });
    </script>
</body>
</html>
"""
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    try:
        webbrowser.open("file://" + os.path.abspath(out_file))
    except Exception:
        pass
    return out_file

def generate_trace_html_viewer(rec_id: str, records: Dict[str, Optional[pd.DataFrame]], diffs: List[Tuple[str, str, str, str, str]]) -> str:
    """Generates an interactive cross-layer lineage comparison HTML dashboard."""
    out_dir = SCRIPT_DIR
    out_file = os.path.join(out_dir, "read_parquet_trace_preview.html")

    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    has_b = records['bronze'] is not None and len(records['bronze']) > 0
    has_s1 = records['silver1'] is not None and len(records['silver1']) > 0
    has_s2 = records['silver2'] is not None and len(records['silver2']) > 0
    has_q = records['quarantine'] is not None and len(records['quarantine']) > 0
    has_g = records['gold'] is not None and len(records['gold']) > 0

    status_b = '<span class="badge bg-success">✔ Ingested</span>' if has_b else '<span class="badge bg-danger">✘ Missing</span>'
    status_s1 = '<span class="badge bg-success">✔ Cleaned</span>' if has_s1 else '<span class="badge bg-danger">✘ Missing</span>'
    status_s2 = '<span class="badge bg-success">✔ Standardized</span>' if has_s2 else ('<span class="badge bg-danger">⚠️ Quarantined</span>' if has_q else '<span class="badge bg-secondary">⊘ Missing</span>')
    status_g = '<span class="badge bg-success">✔ Business Ready</span>' if has_g else ('<span class="badge bg-warning text-dark">⊘ Quarantined</span>' if has_q else '<span class="badge bg-secondary">⊘ Missing</span>')

    diff_rows = ""
    for k, bv, s1v, s2v, gv in diffs:
        diff_rows += f"""
        <tr>
            <td class="fw-bold text-info">{esc(k)}</td>
            <td class="text-secondary">{esc(bv)}</td>
            <td class="text-warning">{esc(s1v)}</td>
            <td class="text-success">{esc(s2v)}</td>
            <td class="text-white fw-bold">{esc(gv)}</td>
        </tr>
        """

    def render_layer_tab(df: Optional[pd.DataFrame], label: str) -> str:
        if df is None or len(df) == 0:
            return f'<div class="p-4 text-secondary">Record not present in {esc(label)}.</div>'
        row = df.iloc[0].to_dict()
        rows_html = ""
        for idx, (k, v) in enumerate(row.items(), start=1):
            val_str = "null" if (v is None or str(v) in ("None", "nan")) else esc(str(v))
            val_cls = "null-val" if val_str == "null" else ""
            rows_html += f"<tr><td class='text-muted' style='width: 40px;'>{idx}</td><td class='text-info fw-bold' style='width: 280px;'>{esc(k)}</td><td class='{val_cls}'>{val_str}</td></tr>"
        return f'<table class="table table-dark table-hover table-striped mb-0"><tbody>{rows_html}</tbody></table>'

    tab_bronze = render_layer_tab(records['bronze'], "Bronze")
    tab_silver1 = render_layer_tab(records['silver1'], "Silver 1")
    tab_silver2 = render_layer_tab(records['silver2'], "Silver 2")
    tab_quarantine = render_layer_tab(records['quarantine'], "Quarantine")
    tab_gold = render_layer_tab(records['gold'], "Gold")

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pipeline Trace: {esc(rec_id)}</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
    <style>
        body {{
            background-color: #0b0f19;
            color: #e2e8f0;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            padding: 24px;
        }}
        .card-custom {{
            background-color: #111827;
            border: 1px solid #1f2937;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5);
            margin-bottom: 24px;
        }}
        .stepper {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 16px 24px;
            background: #1e293b;
            border-radius: 8px;
            margin-bottom: 24px;
        }}
        .step-item {{
            text-align: center;
            flex: 1;
        }}
        .step-arrow {{
            font-size: 1.5rem;
            color: #64748b;
        }}
        .table-dark {{
            background-color: #111827 !important;
            border-color: #1f2937 !important;
        }}
        .null-val {{
            color: #64748b;
            font-style: italic;
        }}
        .nav-tabs .nav-link {{
            color: #94a3b8;
            border-color: #1f2937;
        }}
        .nav-tabs .nav-link.active {{
            background-color: #111827;
            color: #38bdf8;
            border-color: #1f2937 #1f2937 #111827;
        }}
    </style>
</head>
<body>
    <div class="container-fluid">
        <div class="card-custom">
            <div class="d-flex justify-content-between align-items-center mb-3">
                <div>
                    <h2 class="h4 mb-0 text-info">🔄 Medallion Pipeline Journey</h2>
                    <small class="text-secondary">Tracking ID: <code>{esc(rec_id)}</code> across Bronze, Silver, and Gold</small>
                </div>
            </div>

            <!-- Stepper -->
            <div class="stepper">
                <div class="step-item">
                    <div class="fw-bold text-cyan">1. BRONZE</div>
                    <div>{status_b}</div>
                    <small class="text-muted">Kafka Landing</small>
                </div>
                <div class="step-arrow">➔</div>
                <div class="step-item">
                    <div class="fw-bold text-cyan">2. SILVER 1</div>
                    <div>{status_s1}</div>
                    <small class="text-muted">Cleaning & Typing</small>
                </div>
                <div class="step-arrow">➔</div>
                <div class="step-item">
                    <div class="fw-bold text-cyan">3. SILVER 2</div>
                    <div>{status_s2}</div>
                    <small class="text-muted">Standardization</small>
                </div>
                <div class="step-arrow">➔</div>
                <div class="step-item">
                    <div class="fw-bold text-cyan">4. GOLD</div>
                    <div>{status_g}</div>
                    <small class="text-muted">Analytics Lakehouse</small>
                </div>
            </div>

            <!-- Field Evolution Table -->
            <div class="mb-4">
                <h5 class="text-warning mb-2">⚡ Field Transformation Evolution ({len(diffs)} transformed attributes)</h5>
                <div class="table-responsive">
                    <table class="table table-dark table-hover table-bordered">
                        <thead>
                            <tr class="table-secondary text-dark">
                                <th>Field Name</th>
                                <th>Bronze (Raw)</th>
                                <th>Silver 1 (Cleaned)</th>
                                <th>Silver 2 (Standardized)</th>
                                <th>Gold (Analytics)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {diff_rows if diffs else '<tr><td colspan="5" class="text-center text-muted">No values transformed across layers.</td></tr>'}
                        </tbody>
                    </table>
                </div>
            </div>

            <!-- Tabs for individual layers -->
            <h5 class="text-info mb-3">📋 Detailed Layer Inspection</h5>
            <ul class="nav nav-tabs" id="layerTabs" role="tablist">
                <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#tab-b">1. Bronze</button></li>
                <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-s1">2. Silver 1</button></li>
                <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-s2">3. Silver 2</button></li>
                <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-q">3. Quarantine</button></li>
                <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#tab-g">4. Gold</button></li>
            </ul>
            <div class="tab-content border border-top-0 border-secondary rounded-bottom p-3">
                <div class="tab-pane fade show active" id="tab-b">{tab_bronze}</div>
                <div class="tab-pane fade" id="tab-s1">{tab_silver1}</div>
                <div class="tab-pane fade" id="tab-s2">{tab_silver2}</div>
                <div class="tab-pane fade" id="tab-q">{tab_quarantine}</div>
                <div class="tab-pane fade" id="tab-g">{tab_gold}</div>
            </div>
        </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    try:
        webbrowser.open("file://" + os.path.abspath(out_file))
    except Exception:
        pass
    return out_file

def print_help_tips(target_name: str, active_id: str):
    """Prints actionable usage commands and tips."""
    if os.path.exists("read_parquet_v2.py"):
        script_call = "python read_parquet_v2.py"
    else:
        script_call = 'python "Read Data/read_parquet_v2.py"'

    nav_target = f" {target_name}" if target_name not in ("silver1", "trace") else ""

    if RICH_AVAILABLE:
        tips = (
            f"[dim]💡 [bold white]Display Tips & Cross-Layer Sync Commands:[/bold white]\n"
            f"  • Trace record across all layers: [bold cyan]{script_call} trace[/bold cyan] (or [cyan]--trace[/cyan])\n"
            f"  • View specific ID in this layer: [bold cyan]{script_call} {target_name} --id {active_id[:8]}[/bold cyan]\n"
            f"  • Move to next/previous record:   [bold cyan]{script_call}{nav_target} --next[/bold cyan]  /  [bold cyan]{script_call}{nav_target} --prev[/bold cyan]\n"
            f"  • Pick random record:             [bold cyan]{script_call}{nav_target} --random[/bold cyan]\n"
            f"  • View more rows (table view):    [cyan]{script_call} {target_name} 10 --table[/cyan]\n"
            f"  • Interactive Browser / HTML:     [cyan]{script_call} {target_name} --html[/cyan]  (or [cyan]{script_call} trace --html[/cyan])\n"
            f"  • Column catalog / schema:        [cyan]{script_call} {target_name} --schema[/cyan][/dim]"
        )
        console.print(tips)
    else:
        print(f"\nTips: Run '{script_call} trace' to view this ID across all layers, or '{script_call}{nav_target} --next' for next record.")

def main():
    raw_args = sys.argv[1:]

    # Flags
    mode = "card"
    if "--schema" in raw_args or "-s" in raw_args:
        mode = "schema"
    elif "--html" in raw_args or "--web" in raw_args or "-w" in raw_args:
        mode = "html"
    elif "--table" in raw_args or "-t" in raw_args:
        mode = "table"
    elif "--json" in raw_args or "-j" in raw_args:
        mode = "json"
    elif "--trace" in raw_args:
        mode = "trace"

    force_vertical = "--vertical" in raw_args or "-v" in raw_args
    force_grid = "--grid" in raw_args or "-g" in raw_args
    show_all = "--all" in raw_args or "-a" in raw_args
    raw_bronze = "--raw" in raw_args
    no_sync = "--no-sync" in raw_args or "--unsynced" in raw_args

    # Check for column filter flag
    col_filter = None
    for idx, a in enumerate(raw_args):
        if a in ("--col", "--columns", "-c", "--filter", "-f") and idx + 1 < len(raw_args):
            col_filter = raw_args[idx + 1]

    # Check for explicit ID flag: --id or -i
    target_id = None
    for idx, a in enumerate(raw_args):
        if a in ("--id", "-i", "--record-id", "--rec-id") and idx + 1 < len(raw_args):
            target_id = raw_args[idx + 1]

    # Check for navigation flags
    nav_dir = None
    if "--next" in raw_args or "-n" in raw_args:
        nav_dir = "next"
    elif "--prev" in raw_args or "-p" in raw_args:
        nav_dir = "prev"
    elif "--random" in raw_args or "-r" in raw_args:
        nav_dir = "random"
    elif "--reset" in raw_args:
        nav_dir = "reset"

    # Extract non-flag arguments
    flag_values = set()
    for idx, a in enumerate(raw_args):
        if a in ("--col", "--columns", "-c", "--filter", "-f", "--id", "-i", "--record-id", "--rec-id") and idx + 1 < len(raw_args):
            flag_values.add(raw_args[idx + 1])

    non_flags = [a for a in raw_args if not a.startswith("-") and a not in flag_values]

    target_layer = "silver1"
    limit = None
    explicit_limit = False
    user_specified_layer = False

    for a in non_flags:
        lower_a = a.lower()
        if lower_a in TRACE_ALIASES:
            if mode == "card":
                mode = "trace"
            target_layer = "trace"
            user_specified_layer = True
        elif lower_a in LAYERS:
            target_layer = lower_a
            user_specified_layer = True
        elif a.isdigit():
            limit = int(a)
            explicit_limit = True
        elif len(a) >= 6 and (re.match(r'^[0-9a-fA-F-]+$', a) or "-" in a):
            target_id = a
        elif not os.path.exists(a) and not a.endswith(".parquet") and not a.startswith("-"):
            target_id = a
        else:
            target_layer = a
            user_specified_layer = True

    if user_specified_layer:
        set_active_layer(target_layer)
    else:
        last_layer = get_active_layer()
        if last_layer in LAYERS or last_layer in TRACE_ALIASES:
            target_layer = last_layer
            if target_layer in TRACE_ALIASES and mode == "card":
                mode = "trace"

    active_id = get_active_id()

    # Apply navigation if requested
    if nav_dir == "reset":
        set_active_id("e8e76ac3-0376-4540-a532-acf3d735216b")
        active_id = "e8e76ac3-0376-4540-a532-acf3d735216b"
    elif nav_dir:
        active_id = navigate_active_id(nav_dir, active_id, layer=target_layer)

    # Apply explicit target ID
    if target_id:
        active_id = target_id.strip()
        set_active_id(active_id)

    # 1. TRACE MODE (Cross-Layer Journey)
    if mode == "trace" or target_layer in TRACE_ALIASES:
        records = fetch_record_across_pipeline(active_id, unpack_bronze=not raw_bronze)
        found_any = any(v is not None and len(v) > 0 for v in records.values())
        if not found_any:
            print(f"\n[!] Record ID '{active_id}' not found in any pipeline layer.")
            print("Tip: Run with '--random' to pick a valid record, or '--reset' for the default anchor.")
            return None

        # Update active_id to the exact canonical full record_id
        for k in ['silver1', 'silver2', 'gold', 'bronze', 'quarantine']:
            if records[k] is not None and len(records[k]) > 0:
                full_rec = str(records[k].iloc[0].get('record_id', active_id))
                if full_rec and full_rec != 'None':
                    active_id = full_rec
                    set_active_id(active_id)
                    break

        if mode == "html":
            diffs = compute_layer_diffs(records)
            html_path = generate_trace_html_viewer(active_id, records, diffs)
            if RICH_AVAILABLE:
                console.print(f"[bold green]✔ Interactive Pipeline Trace opened in your browser![/bold green] ([dim]{html_path}[/dim])")
            else:
                print(f"Interactive Pipeline Trace opened at: {html_path}")
        else:
            render_trace_view(active_id, records, force_vertical=force_vertical, force_grid=force_grid)

        print_help_tips("trace", active_id)
        return records

    # 2. SINGLE LAYER MODE
    if os.path.exists(target_layer) or target_layer.endswith(".parquet"):
        parquet_pattern = target_layer
        target_name = target_layer
    elif target_layer in LAYERS:
        parquet_pattern = LAYERS[target_layer]
        target_name = target_layer
    else:
        print(f"Unknown layer '{target_layer}'. Available layers: {list(LAYERS.keys())} or 'trace'")
        sys.exit(1)

    try:
        # Check files exist
        try:
            src = layer_source(parquet_pattern)
        except FileNotFoundError as e:
            print(f"\n[!] Nothing to read: {e}")
            print("Note: If the pipeline hasn't run yet, start it with `docker compose up` first to generate data.")
            return None

        # Row count
        count = duckdb.sql(f"SELECT COUNT(*) FROM {src}").fetchone()[0]

        is_bronze = (target_name == "bronze")
        use_active_id = (not no_sync) and (not show_all) and (not explicit_limit) and (mode != "schema")

        df = None
        quarantine_alert = False

        if use_active_id:
            # Query the synced active record ID
            if is_bronze:
                b_df = find_bronze_row(src, active_id)
                if len(b_df) > 0:
                    df = unpack_bronze_record(b_df) if not raw_bronze else b_df
            else:
                q_single = (
                    f"SELECT * FROM {src} "
                    f"WHERE record_id = '{active_id}' OR record_id LIKE '{active_id}%' "
                    f"OR crash_record_id = '{active_id}' OR crash_record_id LIKE '{active_id}%' LIMIT 1"
                )
                res_df = duckdb.sql(q_single).df()
                if len(res_df) > 0:
                    df = res_df
                elif target_name in ("silver2", "gold"):
                    # Check if the record was quarantined during Silver 2 validation!
                    quar_src = layer_source(LAYERS["quarantine"])
                    q_check = (
                        f"SELECT * FROM {quar_src} "
                        f"WHERE record_id = '{active_id}' OR record_id LIKE '{active_id}%' "
                        f"OR crash_record_id = '{active_id}' OR crash_record_id LIKE '{active_id}%' LIMIT 1"
                    )
                    quar_df = duckdb.sql(q_check).df()
                    if len(quar_df) > 0:
                        df = quar_df
                        quarantine_alert = True

        # Fallback to multi-record or un-synced query
        if df is None or len(df) == 0:
            if use_active_id and target_id:
                print(f"[!] Record ID '{active_id}' was not found in layer '{target_name}'.")
                return None

            # Multi-record / schema / fallback query with deterministic ordering
            # Only sort by columns this table has (ref/audit have no kafka_timestamp);
            # quoted because "offset" is a reserved word in DuckDB
            table_cols = {r[0] for r in duckdb.sql(f"DESCRIBE SELECT * FROM {src}").fetchall()}
            sort_cols = ["kafka_timestamp", "offset"] if is_bronze else ["kafka_timestamp", "record_id"]
            sort_cols = [c for c in sort_cols if c in table_cols]
            order_clause = ("ORDER BY " + ", ".join(f'"{c}" ASC' for c in sort_cols)) if sort_cols else ""
            base_query = f"SELECT * FROM {src} {order_clause}"

            if mode == "schema":
                df = duckdb.sql(f"SELECT * FROM {src} LIMIT 500").df()
            elif show_all:
                df = duckdb.sql(base_query).df()
            elif explicit_limit:
                df = duckdb.sql(f"{base_query} LIMIT {limit}").df()
            elif mode == "table":
                df = duckdb.sql(f"{base_query} LIMIT 10").df()
            else:
                # Default preview 1 row
                df = duckdb.sql(f"{base_query} LIMIT 1").df()
                if is_bronze and not raw_bronze:
                    df = unpack_bronze_record(df)

        # Update active_id if found
        if 'record_id' in df.columns and len(df) > 0:
            rec_val = str(df['record_id'].iloc[0])
            if rec_val and rec_val != "None":
                active_id = rec_val
                set_active_id(active_id)

        # Apply column filtering if requested (e.g. --col injury,crash)
        if col_filter:
            keywords = [k.strip().lower() for k in col_filter.split(",")]
            expanded_kw = set(keywords)
            for k in keywords:
                if k.endswith("y"):
                    expanded_kw.add(k[:-1] + "ies")
                    expanded_kw.add(k[:-1])
                elif k.endswith("ies"):
                    expanded_kw.add(k[:-3] + "y")
            matched_cols = [c for c in df.columns if any(k in c.lower() for k in expanded_kw)]
            if matched_cols:
                df = df[matched_cols]
            else:
                if RICH_AVAILABLE:
                    console.print(f"[yellow]Warning: No columns matched filter '{col_filter}'. Displaying all columns.[/yellow]")

        total_cols = len(df.columns)

        # Print banner
        print_banner(target_name, count, total_cols, len(df), mode, active_id if use_active_id else None)

        is_quarantine_record = quarantine_alert or (target_name == "quarantine" and len(df) > 0 and "columns_to_remediate" in df.columns)
        if is_quarantine_record:
            remed = df.iloc[0].get("columns_to_remediate")
            healing_status = df.iloc[0].get("healing_status", "PENDING")
            if RICH_AVAILABLE:
                console.print(Panel(
                    f"[bold red]⚠️ Quarantined Record (Failed Silver 2 Validation)[/bold red]\n"
                    f"[yellow]Columns To Remediate:[/yellow] [bold white]{remed}[/bold white]   │   [cyan]Healing Status:[/cyan] [bold green]{healing_status}[/bold green]\n"
                    f"[dim]Stored in delta/silver2_quarantine[/dim]",
                    title="[bold white on red] 🚨 Quarantine Alert [/bold white on red]",
                    border_style="red"
                ))
            else:
                print(f"\n[!] QUARANTINE: Remediation Needed: {remed} | Status: {healing_status}")

        # Render chosen mode
        if mode == "schema":
            render_schema_view(df)
        elif mode == "html":
            if explicit_limit:
                df_html = df
            elif not show_all:
                df_html = duckdb.sql(f"SELECT * FROM {src} LIMIT 100").df()
            else:
                df_html = df
            html_path = generate_html_viewer(df_html, target_name, count, active_id=active_id if use_active_id else None)
            if RICH_AVAILABLE:
                console.print(f"[bold green]✔ Interactive HTML viewer opened in your browser![/bold green] ([dim]{html_path}[/dim])")
            else:
                print(f"Interactive HTML viewer opened at: {html_path}")
        elif mode == "table":
            render_table_view(df)
        elif mode == "json":
            render_json_view(df)
        else:
            # Default: Card View
            render_card_view(df, force_vertical=force_vertical, force_grid=force_grid)

        # Print tips
        print_help_tips(target_name, active_id)

        return df

    except duckdb.IOException as e:
        print(f"\n[!] Parquet I/O error at {parquet_pattern}: {e}")
    except Exception as e:
        print(f"\nError reading parquet files: {e}")

if __name__ == "__main__":
    main()
