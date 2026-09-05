"""
Quick sanity check, run from the host (not inside a container).

Reads any of the three Delta tables directly using the lightweight
`deltalake` package -- no Spark/JVM required. Handy for confirming data is
flowing through all three layers while the docker-compose stack runs.

Usage:
    pip install deltalake pandas
    python verify_delta.py bronze
    python verify_delta.py silver_cars
    python verify_delta.py gold_car_overview
"""

import sys

from deltalake import DeltaTable

TABLES = {
    "bronze": "./delta/bronze",
    "silver_cars": "./delta/silver_cars",
    "gold_car_overview": "./delta/gold_car_overview",
}

if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "gold_car_overview"
    if name not in TABLES:
        print(f"Unknown table '{name}'. Choose from: {list(TABLES)}")
        sys.exit(1)

    dt = DeltaTable(TABLES[name])
    df = dt.to_pandas()
    print(f"Table: {name}  (version {dt.version()}, {len(df)} rows)")
    print(df.tail(10).to_string())
