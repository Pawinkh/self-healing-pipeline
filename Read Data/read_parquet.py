import sys
import os
# pyrefly: ignore [missing-import]
import duckdb

# Mapping of folder names for your pipeline layers
LAYERS = {
    "bronze": "./delta/bronze/part-*.parquet",
    "silver": "./delta/silver_cars/part-*.parquet",
    "silver_cars": "./delta/silver_cars/part-*.parquet",
    "gold": "./delta/gold_car_overview/part-*.parquet",
    "gold_car_overview": "./delta/gold_car_overview/part-*.parquet",
}

def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "gold_car_overview"

    # Check if the user passed a direct path to a file (e.g. delta/gold_car_overview/part-....parquet)
    if os.path.exists(arg) or arg.endswith(".parquet"):
        parquet_pattern = arg
        target_name = arg
    elif arg in LAYERS:
        parquet_pattern = LAYERS[arg]
        target_name = arg
    else:
        print(f"Unknown argument '{arg}'. Provide a layer (bronze, silver, gold) or a direct path to a .parquet file.")
        sys.exit(1)

    print(f"\n--- Reading: {target_name} ---")

    try:
        query = f"SELECT * FROM '{parquet_pattern}'"

        # Check if user wants all rows or a custom limit
        show_all = "--all" in sys.argv or "-a" in sys.argv or "all" in sys.argv
        
        # Determine row limit
        limit = None
        for a in sys.argv[2:]:
            if a.isdigit():
                limit = int(a)
                break

        # 1. Preview data
        if show_all:
            print("\n=== Full Data (All Rows & Columns) ===")
            duckdb.sql(query).show(max_rows=100000, max_width=10000)
        elif limit:
            print(f"\n=== Data Preview (Top {limit} rows) ===")
            duckdb.sql(query).limit(limit).show(max_rows=limit, max_width=10000)
        else:
            print("\n=== Data Preview (Top 10 rows) ===")
            duckdb.sql(query).limit(10).show(max_rows=10, max_width=10000)
            print("(Tip: Run with '--all' to view all rows, or specify a number: python3 read_parquet.py gold 20)")

        # 2. Get total row count
        count = duckdb.sql(f"SELECT COUNT(*) FROM '{parquet_pattern}'").fetchone()[0]
        print(f"Total rows in {target_name}: {count}")

        # 3. Convert to Pandas DataFrame for further Python processing
        df = duckdb.sql(query).df()
        return df

    except duckdb.IOException:
        print(f"\n[!] No Parquet files found at: {parquet_pattern}")
        print("Note: If the pipeline hasn't run yet, start it with `docker compose up` first to generate the data.")
    except Exception as e:
        print(f"\nError reading parquet files: {e}")

if __name__ == "__main__":
    main()
