"""Data Loaders.

Takes raw data files and loads them into our local database so the
rest of the pipeline can work with them.

Example:
    Load all raw datasets into the database::

        $ python -m ingestion.loaders
"""

# ───────────────────────────────────────────────────────
# WHAT THIS FILE DOES (in plain English):
#
# This file moves data from files on disk into our database.
# Think of it as an "importer" — it reads data files (Parquet
# or CSV format) and puts them into DuckDB (our local database)
# so that later steps in the pipeline can query and transform them.
#
# It handles:
#   - Loading Parquet files (a compressed data format)
#   - Loading CSV files (plain-text spreadsheet files)
#   - Loading all the expected raw datasets at once
# ───────────────────────────────────────────────────────

import duckdb
from pathlib import Path


# Where the raw data files live on disk
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
# Where our local database file is stored
DB_PATH = Path(__file__).parent.parent / "data" / "warehouse.duckdb"


def load_parquet_to_duckdb(
    parquet_path: Path,
    table_name: str,
    schema: str = "raw",
    con: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Read a Parquet file and put its contents into a database table.

    If the table already exists, it gets replaced with the new data.
    This ensures we always have a fresh copy of the data.

    Args:
        parquet_path: The location of the Parquet file on disk.
        table_name: What to name the table in the database.
        schema: Which section of the database to put the table in (default: "raw").
        con: An existing database connection to reuse. If not provided,
            a new connection is opened and closed automatically.

    Returns:
        The number of rows that were loaded into the table.
    """
    # Keep track of whether we created the connection ourselves
    # so we know if we should close it when we're done
    should_close = con is None
    if con is None:
        con = duckdb.connect(str(DB_PATH))

    # Make sure the schema exists, then replace the table with fresh data
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(
        f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
    )
    # Count how many rows were loaded so we can report it
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
    """Read a CSV file and put its contents into a database table.

    Works the same as the Parquet loader above, but for CSV files
    (plain-text spreadsheet files). If the table already exists,
    it gets replaced with the new data.

    Args:
        csv_path: The location of the CSV file on disk.
        table_name: What to name the table in the database.
        schema: Which section of the database to put the table in (default: "raw").
        con: An existing database connection to reuse. If not provided,
            a new connection is opened and closed automatically.

    Returns:
        The number of rows that were loaded into the table.
    """
    # Same pattern as the Parquet loader: track if we own the connection
    should_close = con is None
    if con is None:
        con = duckdb.connect(str(DB_PATH))

    # Make sure the schema exists, then replace the table with fresh data
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(
        f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
    )
    # Count how many rows were loaded so we can report it
    row_count = con.execute(f"SELECT count(*) FROM {schema}.{table_name}").fetchone()[0]

    if should_close:
        con.close()
    return row_count


def load_all_raw_data() -> dict[str, int]:
    """Load all the expected raw data files into the database at once.

    Goes through each expected dataset (policies, claims, contracts)
    and loads it into the "raw" section of the database. If a file
    is missing, it just skips it and moves on.

    Returns:
        A dictionary showing how many rows were loaded for each table.
        For example: {"policies": 50000, "claims": 3200, "contracts": 3}
    """
    # Open a single connection and reuse it for all the loads
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    results = {}
    # Try to load each of the three expected data files
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
