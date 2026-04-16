"""Générateur de données synthétiques d'assurance.

Crée des jeux de données d'assurance fictifs (mais réalistes) pour les tests et les démonstrations.
Il produit trois jeux de données :
    - policies : une liste de polices d'assurance (auto, habitation, santé)
    - claims : les sinistres déclarés par les assurés
    - contracts : les traités de réassurance (partage du risque par l'assureur)

Les chiffres sont calibrés pour paraître réalistes :
    - Ratio S/P cible : 68-75 % (plage typique du marché)
    - Fréquence sinistres : 4-8 % selon le type d'assurance
    - Coût moyen par sinistre : ajusté pour chaque branche

Exemple :
    Créer tous les jeux de données fictifs et les sauvegarder sur disque ::

        $ python ingestion/generate_synthetic_data.py
"""

# ───────────────────────────────────────────────────────
# CE QUE FAIT CE FICHIER :
#
# Ce fichier crée des données d'assurance fictives qui ressemblent
# à des données réelles. Il est utilisé pour tester le reste du
# pipeline sans avoir besoin d'informations clients réelles.
#
# Il génère :
#   1. 50 000 polices d'assurance (auto, habitation, RC, santé)
#   2. Des sinistres déclarés sur ces polices (aléatoires mais réalistes)
#   3. Quelques traités de réassurance (accords avec d'autres assureurs)
#
# Toutes les données sont sauvegardées en fichiers Parquet (un format
# de données compressé) dans le dossier data/raw/ pour que les autres
# étapes du pipeline puissent les utiliser.
# ───────────────────────────────────────────────────────

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import random

# Fixer une graine aléatoire pour que les données "aléatoires" soient identiques à chaque exécution
# Cela rend les résultats reproductibles pour les tests et les démonstrations
SEED = 42
np.random.seed(SEED)
random.seed(SEED)

# ── Paramètres pour chaque type d'assurance (branche) ───────────────
# Ces chiffres contrôlent la génération des données fictives :
#   - frequency : pourcentage de polices qui auront un sinistre
#   - avg_severity : coût moyen d'un sinistre en euros
#   - severity_std : dispersion du coût des sinistres autour de la moyenne
#   - premium_range : prime annuelle minimale et maximale qu'un client peut payer
LOB_PARAMS = {
    "Auto": {
        "frequency": 0.07,  # Taux de sinistralité de 7 %
        "avg_severity": 3_200,  # €
        "severity_std": 2_800,
        "premium_range": (400, 1_800),
    },
    "Home": {
        "frequency": 0.04,
        "avg_severity": 5_800,
        "severity_std": 8_000,
        "premium_range": (200, 900),
    },
    "Liability": {
        "frequency": 0.025,
        "avg_severity": 12_000,
        "severity_std": 25_000,
        "premium_range": (300, 2_500),
    },
    "Health": {
        "frequency": 0.18,
        "avg_severity": 1_400,
        "severity_std": 1_200,
        "premium_range": (800, 3_200),
    },
}

# Les régions françaises où vivent nos assurés fictifs
REGIONS = [
    "Île-de-France",
    "Auvergne-Rhône-Alpes",
    "Provence-Alpes-Côte d'Azur",
    "Occitanie",
    "Nouvelle-Aquitaine",
    "Grand Est",
    "Hauts-de-France",
    "Bretagne",
]

# Certaines régions sont plus risquées que d'autres — ce multiplicateur ajuste
# les primes et les taux de sinistralité. Par exemple, l'Île-de-France (région
# parisienne) est 25 % plus risquée.
REGION_RISK_FACTOR = {
    "Île-de-France": 1.25,
    "Auvergne-Rhône-Alpes": 1.05,
    "Provence-Alpes-Côte d'Azur": 1.10,
    "Occitanie": 0.95,
    "Nouvelle-Aquitaine": 0.90,
    "Grand Est": 1.00,
    "Hauts-de-France": 1.08,
    "Bretagne": 0.88,
}


def generate_policies(n: int = 50_000, start_date: str = "2021-01-01") -> pd.DataFrame:
    """Créer une liste de polices d'assurance fictives.

    Construit un ensemble réaliste d'enregistrements de polices. Chaque police
    reçoit un type d'assurance, une région, un âge client, des dates de début/fin
    et une prime, le tout attribué aléatoirement. Les primes sont ajustées en
    fonction du niveau de risque de la région.

    Args:
        n: Nombre de polices à créer (par défaut 50 000).
        start_date: Date la plus ancienne possible pour le début d'une police
            (format : "AAAA-MM-JJ").

    Returns:
        Un tableau avec une ligne par police, comprenant : policy_id, lob (branche),
        region, insured_age, inception_date, expiry_date, annual_premium, status, channel.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime(2024, 12, 31)

    # Définir la répartition des types d'assurance les plus courants
    # Auto est le plus populaire (45 %), puis Habitation (30 %), RC (15 %), Santé (10 %)
    lob_choices = list(LOB_PARAMS.keys())
    lob_weights = [0.45, 0.30, 0.15, 0.10]

    # Attribuer aléatoirement un type d'assurance, une région et un âge à chaque police
    lobs = np.random.choice(lob_choices, size=n, p=lob_weights)
    regions = np.random.choice(REGIONS, size=n)
    ages = np.random.randint(18, 75, size=n)

    # Choisir une date de début aléatoire pour chaque police, répartie sur la fenêtre de 4 ans
    inception_offsets = np.random.randint(0, (end - start).days, size=n)
    inception_dates = [start + timedelta(days=int(d)) for d in inception_offsets]
    # Chaque police dure exactement un an
    expiry_dates = [d + timedelta(days=365) for d in inception_dates]

    # Marquer chaque police comme "Active" si elle n'a pas encore expiré, sinon "Expired"
    today = datetime(2024, 12, 31)
    statuses = ["Active" if exp > today else "Expired" for exp in expiry_dates]

    # Calculer la prime annuelle pour chaque police
    # La prime dépend du type d'assurance et du niveau de risque de la région
    premiums = []
    for lob, region in zip(lobs, regions):
        low, high = LOB_PARAMS[lob]["premium_range"]
        risk_factor = REGION_RISK_FACTOR[region]
        premium = np.random.uniform(low, high) * risk_factor
        premiums.append(round(premium, 2))

    # Assembler le tout dans un seul tableau
    df = pd.DataFrame(
        {
            "policy_id": [f"POL{str(i).zfill(7)}" for i in range(1, n + 1)],
            "lob": lobs,
            "region": regions,
            "insured_age": ages,
            "inception_date": inception_dates,
            "expiry_date": expiry_dates,
            "annual_premium": premiums,
            "status": statuses,
            "channel": np.random.choice(
                ["Direct", "Broker", "Online", "Agent"],
                size=n,
                p=[0.3, 0.35, 0.25, 0.10],
            ),
        }
    )
    return df


def generate_claims(policies: pd.DataFrame) -> pd.DataFrame:
    """Créer des sinistres fictifs basés sur les polices générées.

    Pour chaque police, cette fonction décide aléatoirement si un sinistre
    survient (selon le taux de sinistralité du type d'assurance et de la région).
    Si un sinistre se produit, elle crée des détails réalistes : date de
    survenance, coût, montant payé à ce jour, et statut (ouvert ou clos).

    Elle simule également les sinistres déclarés tardivement (appelés IBNR) —
    des sinistres survenus dont l'assureur n'a pas encore connaissance car
    l'assuré ne les a pas encore déclarés.

    Args:
        policies: Le tableau des polices (issu de la fonction generate_policies).

    Returns:
        Un tableau avec une ligne par sinistre, comprenant : claim_id, policy_id,
        lob, region, claim_date, reporting_date, ultimate_cost, reserve,
        paid_amount, status, claim_type.
    """
    claims_rows = []
    claim_counter = 1

    # Parcourir chaque police une par une pour décider si elle a des sinistres
    for _, policy in policies.iterrows():
        lob = policy["lob"]
        params = LOB_PARAMS[lob]
        region_factor = REGION_RISK_FACTOR[policy["region"]]

        # Décider aléatoirement du nombre de sinistres pour cette police
        # Utilise une distribution de Poisson (méthode standard pour modéliser les événements rares)
        adj_frequency = params["frequency"] * region_factor
        n_claims = np.random.poisson(adj_frequency)

        # Créer chaque sinistre avec des détails réalistes
        for _ in range(n_claims):
            # Choisir une date aléatoire pendant la période de couverture de la police
            inception = pd.Timestamp(policy["inception_date"])
            expiry = pd.Timestamp(policy["expiry_date"])
            claim_offset = np.random.randint(0, max((expiry - inception).days, 1))
            claim_date = inception + timedelta(days=claim_offset)

            # Déterminer le coût du sinistre avec une distribution log-normale
            # (la plupart des sinistres sont petits, mais quelques-uns sont très coûteux)
            mu = np.log(params["avg_severity"])
            sigma = params["severity_std"] / params["avg_severity"]
            severity = np.random.lognormal(mu, min(sigma, 1.5))
            severity = round(max(severity, 100), 2)

            # Simuler le délai de déclaration du sinistre par l'assuré
            # La plupart déclarent rapidement, mais certains prennent des semaines ou des mois
            reporting_lag = int(np.random.exponential(15))
            reporting_date = claim_date + timedelta(days=max(1, reporting_lag))

            # Constituer la réserve et suivre le montant déjà payé
            reserve = round(severity * np.random.uniform(0.8, 1.3), 2)
            paid = (
                round(severity * np.random.uniform(0.0, 1.0), 2)
                if np.random.rand() > 0.3
                else 0.0
            )
            # Un sinistre est "Closed" une fois que l'essentiel du montant dû a été payé
            status = "Closed" if paid >= severity * 0.9 else "Open"

            claims_rows.append(
                {
                    "claim_id": f"CLM{str(claim_counter).zfill(8)}",
                    "policy_id": policy["policy_id"],
                    "lob": lob,
                    "region": policy["region"],
                    "claim_date": claim_date.date(),
                    "reporting_date": reporting_date.date(),
                    "ultimate_cost": severity,
                    "reserve": reserve,
                    "paid_amount": paid,
                    "status": status,
                    "claim_type": np.random.choice(
                        ["Material", "Bodily Injury", "Theft", "Natural Disaster"],
                        p=[0.5, 0.25, 0.15, 0.10],
                    ),
                }
            )
            claim_counter += 1

    return pd.DataFrame(claims_rows)


def generate_reinsurance_contracts() -> pd.DataFrame:
    """Créer un petit ensemble de traités de réassurance fictifs.

    La réassurance consiste pour une compagnie d'assurance à payer une autre
    compagnie pour l'aider à couvrir les sinistres importants. Cette fonction
    crée quelques contrats types pour l'année 2023.

    Returns:
        Un tableau avec une ligne par contrat, comprenant : contract_id, lob,
        treaty_type, retention (part conservée par l'assureur), limit (plafond),
        reinstatements, premium_rate, effective_date, expiry_date.
    """
    treaties = [
        {
            "contract_id": "XL-AUTO-2023",
            "lob": "Auto",
            "treaty_type": "Excess of Loss",
            "retention": 100_000,
            "limit": 500_000,
            "reinstatements": 2,
            "premium_rate": 0.045,
            "effective_date": "2023-01-01",
            "expiry_date": "2023-12-31",
        },
        {
            "contract_id": "XL-HOME-2023",
            "lob": "Home",
            "treaty_type": "Excess of Loss",
            "retention": 250_000,
            "limit": 2_000_000,
            "reinstatements": 1,
            "premium_rate": 0.032,
            "effective_date": "2023-01-01",
            "expiry_date": "2023-12-31",
        },
        {
            "contract_id": "QUOTA-LIAB-2023",
            "lob": "Liability",
            "treaty_type": "Quota Share",
            "retention": 0.70,
            "limit": None,
            "reinstatements": None,
            "premium_rate": 0.30,
            "effective_date": "2023-01-01",
            "expiry_date": "2023-12-31",
        },
    ]
    return pd.DataFrame(treaties)


def main():
    """Exécuter le processus complet de création de données fictives.

    Crée les trois jeux de données (polices, sinistres et traités de réassurance),
    les sauvegarde sous forme de fichiers compressés dans le dossier data/raw/,
    et affiche un résumé montrant le réalisme des chiffres produits.
    """
    # Créer le dossier de sortie s'il n'existe pas encore
    output_dir = Path(__file__).parent.parent / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Étape 1 : Générer les polices et les sauvegarder
    print("Génération des polices...")
    policies = generate_policies(n=50_000)
    policies.to_parquet(output_dir / "policies.parquet", index=False)
    print(f"  ✓ {len(policies):,} polices générées")

    # Étape 2 : Générer les sinistres pour ces polices et les sauvegarder
    print("Génération des sinistres...")
    claims = generate_claims(policies)
    claims.to_parquet(output_dir / "claims.parquet", index=False)
    print(f"  ✓ {len(claims):,} sinistres générés")

    # Étape 3 : Générer les traités de réassurance et les sauvegarder
    print("Génération des traités de réassurance...")
    contracts = generate_reinsurance_contracts()
    contracts.to_parquet(output_dir / "contracts.parquet", index=False)
    print(f"  ✓ {len(contracts)} traités générés")

    # Afficher un résumé par branche avec les métriques clés
    # S/P = ratio sinistres/primes, Freq = taux de sinistralité
    print("\n── Résumé actuariel ──────────────────────────────")
    for lob in ["Auto", "Home", "Liability", "Health"]:
        pol = policies[policies["lob"] == lob]
        clm = claims[claims["lob"] == lob]
        earned = pol["annual_premium"].sum()
        incurred = clm["ultimate_cost"].sum()
        lr = incurred / earned if earned > 0 else 0
        freq = len(clm) / len(pol) if len(pol) > 0 else 0
        print(
            f"  {lob:12s} | S/P: {lr:.1%} | Freq: {freq:.2%} | Polices: {len(pol):,}"
        )


if __name__ == "__main__":
    main()
