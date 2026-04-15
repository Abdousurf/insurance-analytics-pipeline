-- int_portfolio_summary.sql
-- Intermediate model: portfolio performance summary by line of business
--
-- Aggregates int_claims_enriched to produce LOB-level actuarial KPIs
-- intended for consumption by mart models and the dashboard.
--
-- Grain: one row per (line_of_business, region, accident_year)
--
-- Key metrics:
--   earned_premium_eur   — total annual premiums for the segment
--   incurred_losses_eur  — sum of ultimate claim costs
--   portfolio_loss_ratio — segment-level S/P ratio
--   avg_burning_cost     — average burning cost per policy-year
--   claims_frequency     — claim count relative to policy count
--   avg_severity_eur     — mean ultimate cost per claim
--   herfindahl_index     — portfolio concentration proxy (by claim count)

{{ config(
    materialized='view',
    tags=['actuary', 'intermediate', 'portfolio']
) }}

with enriched as (
    select * from {{ ref('int_claims_enriched') }}
),

-- Aggregate at the (LOB, region, accident_year) grain
segment_agg as (
    select
        line_of_business,
        region,
        accident_year,

        -- Volume metrics
        count(distinct policy_id)                           as policy_count,
        count(claim_id)                                     as claim_count,
        count(case when claim_status = 'Open' then 1 end)   as open_claim_count,
        count(case when is_ibnr_candidate then 1 end)       as ibnr_candidate_count,

        -- Premium and loss aggregates
        sum(annual_premium_eur)                             as earned_premium_eur,
        sum(ultimate_cost_eur)                              as incurred_losses_eur,
        sum(paid_amount_eur)                                as paid_losses_eur,
        sum(reserve_eur)                                    as total_reserve_eur,
        sum(outstanding_reserve_eur)                        as outstanding_reserve_eur,

        -- Burning cost (exposure-adjusted)
        avg(burning_cost_eur)                               as avg_burning_cost_eur,
        sum(burning_cost_eur)                               as total_burning_cost_eur

    from enriched
    group by 1, 2, 3
),

-- Compute derived actuarial ratios
with_ratios as (
    select
        *,

        -- Portfolio-level Loss Ratio (S/P)
        case
            when earned_premium_eur > 0
            then round(incurred_losses_eur / earned_premium_eur, 4)
            else null
        end                                                 as portfolio_loss_ratio,

        -- Paid Loss Ratio
        case
            when earned_premium_eur > 0
            then round(paid_losses_eur / earned_premium_eur, 4)
            else null
        end                                                 as paid_loss_ratio,

        -- Claims Frequency (claims per policy)
        case
            when policy_count > 0
            then round(claim_count::float / policy_count, 4)
            else null
        end                                                 as claims_frequency,

        -- Average Severity per claim
        case
            when claim_count > 0
            then round(incurred_losses_eur / claim_count, 2)
            else null
        end                                                 as avg_severity_eur,

        -- IBNR candidate rate
        case
            when claim_count > 0
            then round(ibnr_candidate_count::float / claim_count, 4)
            else null
        end                                                 as ibnr_rate,

        -- Reserve adequacy ratio (reserve vs outstanding)
        case
            when incurred_losses_eur > 0
            then round(total_reserve_eur / incurred_losses_eur, 4)
            else null
        end                                                 as reserve_adequacy_ratio,

        -- Alert flag: loss ratio above the 85% threshold
        case
            when earned_premium_eur > 0
             and incurred_losses_eur / earned_premium_eur > 0.85
            then true
            else false
        end                                                 as high_loss_ratio_flag,

        current_timestamp                                   as _refreshed_at

    from segment_agg
),

-- LOB-level totals for Herfindahl concentration index
lob_totals as (
    select
        line_of_business,
        accident_year,
        sum(claim_count)                                    as lob_total_claims
    from segment_agg
    group by 1, 2
),

-- Herfindahl index: measures portfolio concentration within each LOB-year.
-- Higher values indicate fewer regions dominate the claims volume.
final as (
    select
        r.*,
        round(
            power(r.claim_count::float / nullif(t.lob_total_claims, 0), 2),
            4
        )                                                   as herfindahl_contribution
    from with_ratios r
    left join lob_totals t
        on  r.line_of_business = t.line_of_business
        and r.accident_year    = t.accident_year
)

select * from final
order by accident_year desc, portfolio_loss_ratio desc nulls last
