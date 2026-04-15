-- assert_loss_ratio_positive.sql
-- Custom dbt singular test
--
-- Asserts that no row in int_claims_enriched has a negative loss_ratio.
--
-- A negative loss_ratio would indicate that either:
--   (a) ultimate_cost_eur is recorded as a negative value — data quality issue, or
--   (b) the macro division produced an arithmetic error.
--
-- Both cases represent a data integrity violation and should fail the pipeline.
--
-- dbt convention: this query must return 0 rows to PASS.
-- Any row returned is treated as a test failure.
--
-- Applies to: int_claims_enriched (grain = claim)
-- Related tests: assert_paid_not_exceeds_ultimate.sql, generic not_null / positive_value tests

select
    claim_id,
    policy_id,
    line_of_business,
    region,
    accident_year,
    ultimate_cost_eur,
    annual_premium_eur,
    loss_ratio
from {{ ref('int_claims_enriched') }}
where
    -- Only evaluate rows where a loss ratio can be computed
    annual_premium_eur is not null
    and annual_premium_eur > 0
    and loss_ratio is not null
    -- Violation: negative loss ratio
    and loss_ratio < 0
