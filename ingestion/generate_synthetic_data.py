"""Synthetic Insurance Data Generator.

Generates realistic P&C insurance datasets:
    - policies: portfolio of active/expired policies
    - claims: declared claims with reserve and payment amounts
    - contracts: reinsurance contract treaties

Actuarial assumptions:
    - Loss ratio target: 68-75% (market reference)
    - Claims frequency: 4-8% depending on LOB
    - Average severity: calibrated per LOB

Example:
    Generate all datasets to the default output directory::

        $ python ingestion/generate_synthetic_data.py
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import random

SEED = 42
np.random.seed(SEED)
random.seed(SEED)

# ── Actuarial parameters per Line of Business ──────────────────────────────
LOB_PARAMS = {
    "Auto": {
        "frequency": 0.07,       # 7% claims rate
        "avg_severity": 3_200,   # €
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

REGIONS = ["Île-de-France", "Auvergne-Rhône-Alpes", "Provence-Alpes-Côte d'Azur",
           "Occitanie", "Nouvelle-Aquitaine", "Grand Est", "Hauts-de-France", "Bretagne"]

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
    """Generate a portfolio of insurance policies.

    Creates synthetic policy records with realistic premium distributions
    calibrated per line of business and adjusted by regional risk factors.

    Args:
        n: Number of policies to generate.
        start_date: Earliest possible inception date in YYYY-MM-DD format.

    Returns:
        DataFrame with columns: policy_id, lob, region, insured_age,
        inception_date, expiry_date, annual_premium, status, channel.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime(2024, 12, 31)

    lob_choices = list(LOB_PARAMS.keys())
    lob_weights = [0.45, 0.30, 0.15, 0.10]

    lobs = np.random.choice(lob_choices, size=n, p=lob_weights)
    regions = np.random.choice(REGIONS, size=n)
    ages = np.random.randint(18, 75, size=n)

    # Inception dates spread over 4 years
    inception_offsets = np.random.randint(0, (end - start).days, size=n)
    inception_dates = [start + timedelta(days=int(d)) for d in inception_offsets]
    expiry_dates = [d + timedelta(days=365) for d in inception_dates]

    # Status
    today = datetime(2024, 12, 31)
    statuses = ["Active" if exp > today else "Expired" for exp in expiry_dates]

    premiums = []
    for lob, region in zip(lobs, regions):
        low, high = LOB_PARAMS[lob]["premium_range"]
        risk_factor = REGION_RISK_FACTOR[region]
        premium = np.random.uniform(low, high) * risk_factor
        premiums.append(round(premium, 2))

    df = pd.DataFrame({
        "policy_id": [f"POL{str(i).zfill(7)}" for i in range(1, n + 1)],
        "lob": lobs,
        "region": regions,
        "insured_age": ages,
        "inception_date": inception_dates,
        "expiry_date": expiry_dates,
        "annual_premium": premiums,
        "status": statuses,
        "channel": np.random.choice(["Direct", "Broker", "Online", "Agent"],
                                     size=n, p=[0.3, 0.35, 0.25, 0.10]),
    })
    return df


def generate_claims(policies: pd.DataFrame) -> pd.DataFrame:
    """Generate claims based on actuarial frequency/severity assumptions.

    Simulates claim occurrences using a Poisson process with region-adjusted
    frequencies. Severities follow a lognormal distribution calibrated per LOB.
    Includes IBNR simulation via exponential reporting lag.

    Args:
        policies: DataFrame of policies as produced by ``generate_policies``.

    Returns:
        DataFrame with columns: claim_id, policy_id, lob, region, claim_date,
        reporting_date, ultimate_cost, reserve, paid_amount, status, claim_type.
    """
    claims_rows = []
    claim_counter = 1

    for _, policy in policies.iterrows():
        lob = policy["lob"]
        params = LOB_PARAMS[lob]
        region_factor = REGION_RISK_FACTOR[policy["region"]]

        # Number of claims for this policy (Poisson)
        adj_frequency = params["frequency"] * region_factor
        n_claims = np.random.poisson(adj_frequency)

        for _ in range(n_claims):
            # Claim date within policy period
            inception = pd.Timestamp(policy["inception_date"])
            expiry = pd.Timestamp(policy["expiry_date"])
            claim_offset = np.random.randint(0, max((expiry - inception).days, 1))
            claim_date = inception + timedelta(days=claim_offset)

            # Severity: lognormal
            mu = np.log(params["avg_severity"])
            sigma = params["severity_std"] / params["avg_severity"]
            severity = np.random.lognormal(mu, min(sigma, 1.5))
            severity = round(max(severity, 100), 2)

            # Reporting lag (IBNR simulation): 1-90 days
            reporting_lag = int(np.random.exponential(15))
            reporting_date = claim_date + timedelta(days=max(1, reporting_lag))

            # Reserve and payments
            reserve = round(severity * np.random.uniform(0.8, 1.3), 2)
            paid = round(severity * np.random.uniform(0.0, 1.0), 2) if np.random.rand() > 0.3 else 0.0
            status = "Closed" if paid >= severity * 0.9 else "Open"

            claims_rows.append({
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
                    p=[0.5, 0.25, 0.15, 0.10]
                ),
            })
            claim_counter += 1

    return pd.DataFrame(claims_rows)


def generate_reinsurance_contracts() -> pd.DataFrame:
    """Generate XL reinsurance treaty contracts.

    Creates a static set of Excess of Loss and Quota Share treaty definitions
    for the 2023 underwriting year.

    Returns:
        DataFrame with columns: contract_id, lob, treaty_type, retention,
        limit, reinstatements, premium_rate, effective_date, expiry_date.
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
    """Run the full synthetic data generation pipeline.

    Generates policies, claims, and reinsurance contracts, writes them as
    Parquet files to ``data/raw/``, and prints an actuarial summary.
    """
    output_dir = Path(__file__).parent.parent / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Generating policies...")
    policies = generate_policies(n=50_000)
    policies.to_parquet(output_dir / "policies.parquet", index=False)
    print(f"  ✓ {len(policies):,} policies generated")

    print("Generating claims...")
    claims = generate_claims(policies)
    claims.to_parquet(output_dir / "claims.parquet", index=False)
    print(f"  ✓ {len(claims):,} claims generated")

    print("Generating reinsurance contracts...")
    contracts = generate_reinsurance_contracts()
    contracts.to_parquet(output_dir / "contracts.parquet", index=False)
    print(f"  ✓ {len(contracts)} treaties generated")

    # Summary stats
    print("\n── Actuarial Summary ──────────────────────────────")
    for lob in ["Auto", "Home", "Liability", "Health"]:
        pol = policies[policies["lob"] == lob]
        clm = claims[claims["lob"] == lob]
        earned = pol["annual_premium"].sum()
        incurred = clm["ultimate_cost"].sum()
        lr = incurred / earned if earned > 0 else 0
        freq = len(clm) / len(pol) if len(pol) > 0 else 0
        print(f"  {lob:12s} | S/P: {lr:.1%} | Freq: {freq:.2%} | Policies: {len(pol):,}")


if __name__ == "__main__":
    main()
