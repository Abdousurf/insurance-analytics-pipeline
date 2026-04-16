"""
Ingestion Open Data — ONISR Accidents Corporels (data.gouv.fr)
==============================================================
Source : Bases de données annuelles des accidents corporels de la circulation routière
URL    : https://www.data.gouv.fr/fr/datasets/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2022/

Licence : Licence Ouverte / Open Licence v2.0 (Etalab)

Les 4 fichiers ONISR par année :
  - caracteristiques : heure, lieu, conditions météo, lumière
  - lieux            : type de voie, intersection, surface
  - vehicules        : type, motorisation, manœuvre
  - usagers          : âge, sexe, gravité (tué / blessé grave / léger / indemne)

Pourquoi ces données pour un pipeline assurance IARD ?
  → Fréquence sinistres auto par segment (âge, région, météo)
  → Proxy de sévérité (gravité des accidents)
  → Enrichissement du générateur synthétique par des distributions réelles
"""

import logging
import zipfile
from pathlib import Path

import pandas as pd
import requests
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATA_DIR = Path(__file__).parent.parent / "data" / "raw" / "onisr"

# API data.gouv.fr — dataset ONISR (identifiant stable du dataset)
DATAGOUV_DATASET_ID = "5cebfa8c8b4c41648d634f9c"

# Années disponibles (archives annuelles)
YEARS = [2021, 2022]  # réduire pour CI rapide ; ajuster selon besoin

# Fichiers attendus par année
FILE_TYPES = ["caracteristiques", "lieux", "vehicules", "usagers"]

DATAGOUV_API = "https://www.data.gouv.fr/api/1"


# ---------------------------------------------------------------------------
# Fonctions utilitaires
# ---------------------------------------------------------------------------
def get_dataset_resources(dataset_id: str) -> list[dict]:
    """Récupère la liste des ressources via l'API data.gouv.fr."""
    url = f"{DATAGOUV_API}/datasets/{dataset_id}/"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json().get("resources", [])


def download_file(url: str, dest: Path, chunk_size: int = 8192) -> None:
    """Télécharge un fichier avec barre de progression."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        log.info("  ✓ Déjà téléchargé : %s", dest.name)
        return

    log.info("  ↓ Téléchargement : %s", url)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as bar:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)
                bar.update(len(chunk))


def extract_zip(zip_path: Path, extract_to: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(extract_to)
    log.info("  📂 Extrait dans %s", extract_to)


# ---------------------------------------------------------------------------
# Logique de téléchargement
# ---------------------------------------------------------------------------
def download_onisr(years: list[int] = YEARS) -> dict[int, dict[str, Path]]:
    """
    Télécharge les fichiers ONISR pour les années spécifiées.
    Retourne un dict {année: {type_fichier: Path}}.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    log.info(
        "📡 Récupération des ressources data.gouv.fr (dataset %s)…", DATAGOUV_DATASET_ID
    )

    try:
        resources = get_dataset_resources(DATAGOUV_DATASET_ID)
    except Exception as e:
        log.warning(
            "⚠️  API data.gouv.fr inaccessible (%s). Utilisation des URLs directes.", e
        )
        resources = _fallback_resources()

    downloaded: dict[int, dict[str, Path]] = {}

    for year in years:
        downloaded[year] = {}
        year_dir = DATA_DIR / str(year)
        year_dir.mkdir(exist_ok=True)

        for ftype in FILE_TYPES:
            # Cherche la ressource correspondant à l'année et au type
            candidates = [
                r
                for r in resources
                if str(year) in r.get("title", "")
                and ftype in r.get("title", "").lower()
            ]

            if not candidates:
                log.warning("  ⚠️  Ressource introuvable : %s %s", year, ftype)
                continue

            resource = candidates[0]
            url = resource["url"]
            ext = ".csv" if url.endswith(".csv") else ".zip"
            dest = year_dir / f"{ftype}_{year}{ext}"

            try:
                download_file(url, dest)
                if ext == ".zip":
                    extract_zip(dest, year_dir)
                downloaded[year][ftype] = dest
            except Exception as e:
                log.error("  ✗ Échec téléchargement %s %s : %s", year, ftype, e)

    return downloaded


def _fallback_resources() -> list[dict]:
    """URLs directes de secours pour les fichiers ONISR 2021-2022."""
    base = "https://static.data.gouv.fr/resources/bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2022"
    return [
        {
            "title": "caracteristiques_2022",
            "url": f"{base}/20231101-082336/ccaracteristiques-2022.csv",
        },
        {"title": "lieux_2022", "url": f"{base}/20231101-082336/lieux-2022.csv"},
        {
            "title": "vehicules_2022",
            "url": f"{base}/20231101-082336/vehicules-2022.csv",
        },
        {"title": "usagers_2022", "url": f"{base}/20231101-082336/usagers-2022.csv"},
        {
            "title": "caracteristiques_2021",
            "url": f"{base}/20231101-082336/cacteristiques-2021.csv",
        },
        {"title": "lieux_2021", "url": f"{base}/20231101-082336/lieux-2021.csv"},
        {
            "title": "vehicules_2021",
            "url": f"{base}/20231101-082336/vehicules-2021.csv",
        },
        {"title": "usagers_2021", "url": f"{base}/20231101-082336/usagers-2021.csv"},
    ]


# ---------------------------------------------------------------------------
# Transformation : créer un dataset sinistres enrichi
# ---------------------------------------------------------------------------
def build_claims_enriched(years: list[int] = YEARS) -> pd.DataFrame:
    """
    Fusionne les 4 tables ONISR en un dataset sinistres enrichi
    compatible avec le schéma du pipeline d'analytique assurance.

    Colonnes produites :
      accident_id, date, heure, departement, commune,
      conditions_meteo, luminosite, type_voie,
      nb_vehicules, nb_victimes, gravite_max,
      age_conducteur, sexe_conducteur
    """
    frames = []

    for year in years:
        year_dir = DATA_DIR / str(year)
        if not year_dir.exists():
            log.warning(
                "Données manquantes pour %s — exécuter download_onisr() d'abord.", year
            )
            continue

        # Lecture des CSVs (séparateur ';' dans les fichiers ONISR)
        def read(name: str) -> pd.DataFrame:
            for fname in year_dir.iterdir():
                if name in fname.name.lower() and fname.suffix == ".csv":
                    return pd.read_csv(
                        fname, sep=";", encoding="latin-1", low_memory=False
                    )
            return pd.DataFrame()

        carac = read("caract")
        lieux = read("lieux")
        vehic = read("vehic")
        usag = read("usag")

        if carac.empty:
            log.warning("Fichier caractéristiques introuvable pour %s", year)
            continue

        # Colonne clé ONISR : Num_Acc
        df = carac.copy()

        # Jointure lieux
        if not lieux.empty and "Num_Acc" in lieux.columns:
            df = df.merge(lieux[["Num_Acc", "catr", "surf"]], on="Num_Acc", how="left")

        # Agrégation usagers — gravité maximale par accident
        if not usag.empty and "Num_Acc" in usag.columns:
            grav = usag.groupby("Num_Acc")["grav"].max().reset_index()
            nb_vic = usag.groupby("Num_Acc").size().reset_index(name="nb_victimes")
            df = df.merge(grav, on="Num_Acc", how="left")
            df = df.merge(nb_vic, on="Num_Acc", how="left")

        # Agrégation véhicules — nombre par accident
        if not vehic.empty and "Num_Acc" in vehic.columns:
            nb_veh = vehic.groupby("Num_Acc").size().reset_index(name="nb_vehicules")
            df = df.merge(nb_veh, on="Num_Acc", how="left")

        df["annee"] = year
        frames.append(df)

    if not frames:
        log.error("Aucune donnée chargée — vérifier le répertoire %s", DATA_DIR)
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)

    # Renommage pour cohérence avec le schéma sinistres du pipeline
    rename_map = {
        "Num_Acc": "accident_id",
        "an": "annee",
        "mois": "mois",
        "jour": "jour",
        "hrmn": "heure",
        "dep": "departement",
        "com": "commune",
        "atm": "conditions_meteo",
        "lum": "luminosite",
        "catr": "type_voie",
        "surf": "etat_surface",
        "grav": "gravite_max",
    }
    result = result.rename(
        columns={k: v for k, v in rename_map.items() if k in result.columns}
    )

    out_path = DATA_DIR.parent / "processed" / "onisr_claims_enriched.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out_path, index=False)
    log.info("✅ Dataset enrichi sauvegardé : %s (%d lignes)", out_path, len(result))

    return result


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Télécharge et prépare les données ONISR open data"
    )
    parser.add_argument(
        "--years", nargs="+", type=int, default=YEARS, help="Années à télécharger"
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Ne pas re-télécharger si déjà présent",
    )
    args = parser.parse_args()

    if not args.skip_download:
        download_onisr(args.years)

    df = build_claims_enriched(args.years)
    if not df.empty:
        print(f"\n📊 Aperçu du dataset enrichi ({len(df)} accidents) :")
        print(df.head())
        print(f"\nColonnes : {list(df.columns)}")
