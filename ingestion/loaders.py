"""
Data Loaders
=============
Load raw Parquet/CSV data into DuckDB warehouse for dbt consumption.
"""

import duckdb
from pathlib import Path


RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
DB_PATH = Path(__file__).parent.parent / "data" / "warehouse.duckdb"


def load_parquet_to_duckdb(
    parquet_path: Path,
    table_name: str,
    schema: str = "raw",
    con: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Load a Parquet file into a DuckDB table, replacing if exists."""
    should_close = con is None
    if con is None:
        con = duckdb.connect(str(DB_PATH))

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(
        f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
    )
    row_count = con.execute(f"SELECT count(*) FROM {schema}.{table_name}").fetchone()[0]

    if should_close:
        con.close()
    return row_count


def load_csv_to_duckdb(
    csv_path: Path,
    table_name: str,
    schema: str = "raw",
    con: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Load a CSV file into a DuckDB table, replacing if exists."""
    should_close = con is None
    if con is None:
        con = duckdb.connect(str(DB_PATH))

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(
        f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
    )
    row_count = con.execute(f"SELECT count(*) FROM {schema}.{table_name}").fetchone()[0]

    if should_close:
        con.close()
    return row_count


def load_all_raw_data() -> dict[str, int]:
    """Load all raw Parquet files into DuckDB."""
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    results = {}
    for name in ["policies", "claims", "contracts"]:
        parquet_path = RAW_DIR / f"{name}.parquet"
        if parquet_path.exists():
            count = load_parquet_to_duckdb(parquet_path, name, "raw", con)
            results[name] = count
            print(f"  Loaded {name}: {count:,} rows")
        else:
            print(f"  Skipped {name}: file not found")

    con.close()
    return results


if __name__ == "__main__":
    print("Loading raw data into DuckDB...")
    load_all_raw_data()
    print("Done.")
