"""Chargeurs de données.

Prend les fichiers de données brutes et les charge dans notre base de données
locale pour que le reste du pipeline puisse les exploiter.

Exemple :
    Charger tous les jeux de données brutes dans la base ::

        $ python -m ingestion.loaders
"""

# ───────────────────────────────────────────────────────
# CE QUE FAIT CE FICHIER :
#
# Ce fichier transfère les données depuis les fichiers sur disque
# vers notre base de données. Considérez-le comme un « importateur » —
# il lit les fichiers de données (format Parquet ou CSV) et les
# insère dans DuckDB (notre base de données locale) pour que les
# étapes suivantes du pipeline puissent les interroger et les transformer.
#
# Il gère :
#   - Le chargement de fichiers Parquet (format de données compressé)
#   - Le chargement de fichiers CSV (fichiers texte tabulaires)
#   - Le chargement de tous les jeux de données brutes attendus en une fois
# ───────────────────────────────────────────────────────

import duckdb
from pathlib import Path

# Emplacement des fichiers de données brutes sur le disque
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
# Emplacement du fichier de base de données locale
DB_PATH = Path(__file__).parent.parent / "data" / "warehouse.duckdb"


def load_parquet_to_duckdb(
    parquet_path: Path,
    table_name: str,
    schema: str = "raw",
    con: duckdb.DuckDBPyConnection | None = None,
) -> int:
    """Lire un fichier Parquet et insérer son contenu dans une table de la base.

    Si la table existe déjà, elle est remplacée par les nouvelles données.
    Cela garantit que nous disposons toujours d'une copie fraîche des données.

    Args:
        parquet_path: Emplacement du fichier Parquet sur le disque.
        table_name: Nom à donner à la table dans la base de données.
        schema: Section de la base dans laquelle placer la table (par défaut : "raw").
        con: Connexion existante à réutiliser. Si non fournie,
            une nouvelle connexion est ouverte puis fermée automatiquement.

    Returns:
        Le nombre de lignes chargées dans la table.
    """
    # Mémoriser si nous avons créé la connexion nous-mêmes
    # pour savoir si nous devons la fermer à la fin
    should_close = con is None
    if con is None:
        con = duckdb.connect(str(DB_PATH))

    # S'assurer que le schéma existe, puis remplacer la table avec les données fraîches
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(
        f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM read_parquet('{parquet_path}')"
    )
    # Compter le nombre de lignes chargées pour pouvoir le rapporter
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
    """Lire un fichier CSV et insérer son contenu dans une table de la base.

    Fonctionne de la même manière que le chargeur Parquet ci-dessus, mais
    pour les fichiers CSV (fichiers texte tabulaires). Si la table existe
    déjà, elle est remplacée par les nouvelles données.

    Args:
        csv_path: Emplacement du fichier CSV sur le disque.
        table_name: Nom à donner à la table dans la base de données.
        schema: Section de la base dans laquelle placer la table (par défaut : "raw").
        con: Connexion existante à réutiliser. Si non fournie,
            une nouvelle connexion est ouverte puis fermée automatiquement.

    Returns:
        Le nombre de lignes chargées dans la table.
    """
    # Même logique que le chargeur Parquet : mémoriser si nous possédons la connexion
    should_close = con is None
    if con is None:
        con = duckdb.connect(str(DB_PATH))

    # S'assurer que le schéma existe, puis remplacer la table avec les données fraîches
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"DROP TABLE IF EXISTS {schema}.{table_name}")
    con.execute(
        f"CREATE TABLE {schema}.{table_name} AS SELECT * FROM read_csv_auto('{csv_path}')"
    )
    # Compter le nombre de lignes chargées pour pouvoir le rapporter
    row_count = con.execute(f"SELECT count(*) FROM {schema}.{table_name}").fetchone()[0]

    if should_close:
        con.close()
    return row_count


def load_all_raw_data() -> dict[str, int]:
    """Charger tous les fichiers de données brutes attendus dans la base en une fois.

    Parcourt chaque jeu de données attendu (polices, sinistres, contrats)
    et le charge dans la section "raw" de la base de données. Si un fichier
    est manquant, il est simplement ignoré.

    Returns:
        Un dictionnaire indiquant le nombre de lignes chargées par table.
        Par exemple : {"policies": 50000, "claims": 3200, "contracts": 3}
    """
    # Ouvrir une seule connexion et la réutiliser pour tous les chargements
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    results = {}
    # Tenter de charger chacun des trois fichiers de données attendus
    for name in ["policies", "claims", "contracts"]:
        parquet_path = RAW_DIR / f"{name}.parquet"
        if parquet_path.exists():
            count = load_parquet_to_duckdb(parquet_path, name, "raw", con)
            results[name] = count
            print(f"  Chargé {name} : {count:,} lignes")
        else:
            print(f"  Ignoré {name} : fichier non trouvé")

    con.close()
    return results


if __name__ == "__main__":
    print("Chargement des données brutes dans DuckDB...")
    load_all_raw_data()
    print("Terminé.")
