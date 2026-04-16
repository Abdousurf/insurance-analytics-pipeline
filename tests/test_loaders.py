"""Tests pour les chargeurs de données."""

import duckdb
import pandas as pd
from pathlib import Path

from ingestion.loaders import load_parquet_to_duckdb, load_csv_to_duckdb


class TestLoadParquet:
    """Tests sur le chargement Parquet → DuckDB."""

    def test_load_creates_table(self, tmp_path, sample_policies):
        parquet_path = tmp_path / "policies.parquet"
        sample_policies.to_parquet(parquet_path, index=False)

        db_path = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        count = load_parquet_to_duckdb(parquet_path, "policies", "raw", con)
        assert count == len(sample_policies)

        result = con.execute("SELECT count(*) FROM raw.policies").fetchone()[0]
        assert result == len(sample_policies)
        con.close()

    def test_load_replaces_existing(self, tmp_path, sample_policies):
        parquet_path = tmp_path / "policies.parquet"
        sample_policies.to_parquet(parquet_path, index=False)

        db_path = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        load_parquet_to_duckdb(parquet_path, "policies", "raw", con)
        # Charger une deuxième fois — doit remplacer, pas dupliquer
        count = load_parquet_to_duckdb(parquet_path, "policies", "raw", con)
        assert count == len(sample_policies)
        con.close()


class TestLoadCsv:
    """Tests sur le chargement CSV → DuckDB."""

    def test_load_csv(self, tmp_path):
        csv_path = tmp_path / "test.csv"
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        df.to_csv(csv_path, index=False)

        db_path = tmp_path / "test.duckdb"
        con = duckdb.connect(str(db_path))
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")

        count = load_csv_to_duckdb(csv_path, "test_table", "raw", con)
        assert count == 3

        result = con.execute("SELECT * FROM raw.test_table ORDER BY a").fetchdf()
        assert list(result["a"]) == [1, 2, 3]
        con.close()
