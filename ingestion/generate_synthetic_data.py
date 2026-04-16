"""Synthetic Insurance Data Generator.

Creates fake (but realistic) insurance datasets for testing and demos.
It produces three datasets:
    - policies: a list of insurance policies (like car, home, health)
    - claims: insurance claims made by policyholders
    - contracts: reinsurance agreements (how the insurer shares risk)

The numbers are designed to look realistic:
    - Loss ratio target: 68-75% (a typical industry range)
    - Claims frequency: 4-8% depending on the type of insurance
    - Average claim cost: adjusted for each type of insurance

Example:
    Create all fake datasets and save them to disk::

        $ python ingestion/generate_synthetic_data.py
"""

# ───────────────────────────────────────────────────────
# WHAT THIS FILE DOES (in plain English):
#
# This file creates fake insurance data that looks like real data.
# It's used for testing the rest of the pipeline without needing
# actual customer information.
#
# It generates:
#   1. 50,000 insurance policies (car, home, liability, health)
#   2. Claims filed against those policies (random but realistic)
#   3. A few reinsurance contracts (agreements with other insurers)
#
# All the data gets saved as Parquet files (a compressed data format)
# in the data/raw/ folder so other parts of the pipeline can use it.
# ───────────────────────────────────────────────────────

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import random

# Set a fixed random seed so the "random" data comes out the same every time
# This makes results reproducible for testing and demos
SEED = 42
np.random.seed(SEED)
random.seed(SEED)

# ── Settings for each type of insurance (Line of Business) ───────────────
# These numbers control how the fake data is generated:
#   - frequency: what percentage of policies will have a claim
#   - avg_severity: the average cost of a single claim in euros
#   - severity_std: how much claim costs vary from the average
#   - premium_range: the lowest and highest annual premium a customer might pay
LOB_PARAMS = {
    "Auto": {
        "frequency": 0.07,  # 7% claims rate
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

# The French regions where our fake policyholders live
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

# Some regions are riskier than others — this multiplier adjusts premiums
# and claim rates. For example, Île-de-France (Paris area) is 25% riskier.
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
    """Create a list of fake insurance policies.

    Builds a realistic-looking set of policy records. Each policy gets a
    random insurance type, region, customer age, start/end date, and premium.
    Premiums are adjusted based on how risky the region is.

    Args:
        n: How many policies to create (default is 50,000).
        start_date: The earliest date a policy can start (format: "YYYY-MM-DD").

    Returns:
        A table with one row per policy, including: policy_id, lob (insurance type),
        region, insured_age, inception_date, expiry_date, annual_premium, status, channel.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime(2024, 12, 31)

    # Decide which types of insurance are most common
    # Auto is most popular (45%), then Home (30%), Liability (15%), Health (10%)
    lob_choices = list(LOB_PARAMS.keys())
    lob_weights = [0.45, 0.30, 0.15, 0.10]

    # Randomly assign an insurance type, region, and age to each policy
    lobs = np.random.choice(lob_choices, size=n, p=lob_weights)
    regions = np.random.choice(REGIONS, size=n)
    ages = np.random.randint(18, 75, size=n)

    # Pick a random start date for each policy, spread over the 4-year window
    inception_offsets = np.random.randint(0, (end - start).days, size=n)
    inception_dates = [start + timedelta(days=int(d)) for d in inception_offsets]
    # Each policy lasts exactly one year
    expiry_dates = [d + timedelta(days=365) for d in inception_dates]

    # Mark each policy as "Active" if it hasn't expired yet, otherwise "Expired"
    today = datetime(2024, 12, 31)
    statuses = ["Active" if exp > today else "Expired" for exp in expiry_dates]

    # Calculate the annual premium for each policy
    # The premium depends on the insurance type and the region's risk level
    premiums = []
    for lob, region in zip(lobs, regions):
        low, high = LOB_PARAMS[lob]["premium_range"]
        risk_factor = REGION_RISK_FACTOR[region]
        premium = np.random.uniform(low, high) * risk_factor
        premiums.append(round(premium, 2))

    # Put everything together into a single table
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
    """Create fake insurance claims based on the policies we generated.

    For each policy, this randomly decides if a claim happens (based on the
    claim rate for that insurance type and region). If a claim does happen,
    it creates realistic details: when it happened, how much it cost, how
    much has been paid so far, and whether it's still open.

    It also simulates "late-reported" claims (called IBNR) — claims that
    happened but the insurer doesn't know about yet because the customer
    hasn't reported them.

    Args:
        policies: The table of policies (from the generate_policies function).

    Returns:
        A table with one row per claim, including: claim_id, policy_id,
        lob, region, claim_date, reporting_date, ultimate_cost, reserve,
        paid_amount, status, claim_type.
    """
    claims_rows = []
    claim_counter = 1

    # Go through each policy one by one to decide if it has any claims
    for _, policy in policies.iterrows():
        lob = policy["lob"]
        params = LOB_PARAMS[lob]
        region_factor = REGION_RISK_FACTOR[policy["region"]]

        # Randomly decide how many claims this policy has
        # Uses a Poisson distribution (a standard way to model rare events)
        adj_frequency = params["frequency"] * region_factor
        n_claims = np.random.poisson(adj_frequency)

        # Create each claim with realistic details
        for _ in range(n_claims):
            # Pick a random date during the policy's coverage period
            inception = pd.Timestamp(policy["inception_date"])
            expiry = pd.Timestamp(policy["expiry_date"])
            claim_offset = np.random.randint(0, max((expiry - inception).days, 1))
            claim_date = inception + timedelta(days=claim_offset)

            # Decide how much this claim costs using a lognormal distribution
            # (most claims are small, but a few are very expensive)
            mu = np.log(params["avg_severity"])
            sigma = params["severity_std"] / params["avg_severity"]
            severity = np.random.lognormal(mu, min(sigma, 1.5))
            severity = round(max(severity, 100), 2)

            # Simulate how long it takes the customer to report the claim
            # Most report quickly, but some take weeks or months
            reporting_lag = int(np.random.exponential(15))
            reporting_date = claim_date + timedelta(days=max(1, reporting_lag))

            # Set the money aside (reserve) and track how much has been paid out
            reserve = round(severity * np.random.uniform(0.8, 1.3), 2)
            paid = (
                round(severity * np.random.uniform(0.0, 1.0), 2)
                if np.random.rand() > 0.3
                else 0.0
            )
            # A claim is "Closed" once we've paid out most of what we owe
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
    """Create a small set of fake reinsurance agreements.

    Reinsurance is when an insurance company pays another company to help
    cover big losses. This creates a few sample contracts for the year 2023.

    Returns:
        A table with one row per contract, including: contract_id, lob,
        treaty_type, retention (how much the insurer keeps), limit (max payout),
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
    """Run the complete fake data creation process.

    Creates all three datasets (policies, claims, and reinsurance contracts),
    saves them as compressed files in the data/raw/ folder, and prints a
    summary showing how realistic the numbers look.
    """
    # Create the output folder if it doesn't exist yet
    output_dir = Path(__file__).parent.parent / "data" / "raw"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Generate the policies and save them
    print("Generating policies...")
    policies = generate_policies(n=50_000)
    policies.to_parquet(output_dir / "policies.parquet", index=False)
    print(f"  ✓ {len(policies):,} policies generated")

    # Step 2: Generate claims for those policies and save them
    print("Generating claims...")
    claims = generate_claims(policies)
    claims.to_parquet(output_dir / "claims.parquet", index=False)
    print(f"  ✓ {len(claims):,} claims generated")

    # Step 3: Generate reinsurance contracts and save them
    print("Generating reinsurance contracts...")
    contracts = generate_reinsurance_contracts()
    contracts.to_parquet(output_dir / "contracts.parquet", index=False)
    print(f"  ✓ {len(contracts)} treaties generated")

    # Print a summary for each insurance type showing the key metrics
    # S/P = loss ratio (how much paid out vs. collected), Freq = claim rate
    print("\n── Actuarial Summary ──────────────────────────────")
    for lob in ["Auto", "Home", "Liability", "Health"]:
        pol = policies[policies["lob"] == lob]
        clm = claims[claims["lob"] == lob]
        earned = pol["annual_premium"].sum()
        incurred = clm["ultimate_cost"].sum()
        lr = incurred / earned if earned > 0 else 0
        freq = len(clm) / len(pol) if len(pol) > 0 else 0
        print(
            f"  {lob:12s} | S/P: {lr:.1%} | Freq: {freq:.2%} | Policies: {len(pol):,}"
        )


if __name__ == "__main__":
    main()
