"""Tests pour le générateur de données synthétiques."""

import pandas as pd
from ingestion.generate_synthetic_data import (
    LOB_PARAMS,
    generate_claims,
    generate_policies,
    generate_reinsurance_contracts,
)


class TestGeneratePolicies:
    """Tests sur la génération des polices."""

    def test_row_count(self, sample_policies):
        assert len(sample_policies) == 100

    def test_columns_present(self, sample_policies):
        expected = {
            "policy_id",
            "lob",
            "region",
            "insured_age",
            "inception_date",
            "expiry_date",
            "annual_premium",
            "status",
            "channel",
        }
        assert set(sample_policies.columns) == expected

    def test_policy_ids_unique(self, sample_policies):
        assert sample_policies["policy_id"].is_unique

    def test_lob_values_valid(self, sample_policies):
        valid_lobs = set(LOB_PARAMS.keys())
        assert set(sample_policies["lob"].unique()).issubset(valid_lobs)

    def test_premiums_positive(self, sample_policies):
        assert (sample_policies["annual_premium"] > 0).all()

    def test_age_range(self, sample_policies):
        assert (sample_policies["insured_age"] >= 18).all()
        assert (sample_policies["insured_age"] < 75).all()

    def test_status_values(self, sample_policies):
        assert set(sample_policies["status"].unique()).issubset({"Active", "Expired"})

    def test_inception_before_expiry(self, sample_policies):
        assert (sample_policies["expiry_date"] > sample_policies["inception_date"]).all()


class TestGenerateClaims:
    """Tests sur la génération des sinistres."""

    def test_returns_dataframe(self, sample_claims):
        assert isinstance(sample_claims, pd.DataFrame)

    def test_columns_present(self, sample_claims):
        expected = {
            "claim_id",
            "policy_id",
            "lob",
            "region",
            "claim_date",
            "reporting_date",
            "ultimate_cost",
            "reserve",
            "paid_amount",
            "status",
            "claim_type",
        }
        assert set(sample_claims.columns) == expected

    def test_claim_ids_unique(self, sample_claims):
        if len(sample_claims) > 0:
            assert sample_claims["claim_id"].is_unique

    def test_costs_positive(self, sample_claims):
        if len(sample_claims) > 0:
            assert (sample_claims["ultimate_cost"] > 0).all()

    def test_paid_not_negative(self, sample_claims):
        if len(sample_claims) > 0:
            assert (sample_claims["paid_amount"] >= 0).all()

    def test_reserve_not_negative(self, sample_claims):
        if len(sample_claims) > 0:
            assert (sample_claims["reserve"] >= 0).all()

    def test_status_values(self, sample_claims):
        if len(sample_claims) > 0:
            assert set(sample_claims["status"].unique()).issubset({"Open", "Closed"})

    def test_policies_exist(self, sample_policies, sample_claims):
        if len(sample_claims) > 0:
            valid_ids = set(sample_policies["policy_id"])
            assert set(sample_claims["policy_id"]).issubset(valid_ids)


class TestReinsuranceContracts:
    """Tests sur les traités de réassurance."""

    def test_returns_three_contracts(self):
        contracts = generate_reinsurance_contracts()
        assert len(contracts) == 3

    def test_contract_ids_unique(self):
        contracts = generate_reinsurance_contracts()
        assert contracts["contract_id"].is_unique

    def test_lob_values(self):
        contracts = generate_reinsurance_contracts()
        assert set(contracts["lob"]).issubset(set(LOB_PARAMS.keys()))
