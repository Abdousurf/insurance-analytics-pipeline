-- mart_claims_frequency.sql
-- Mart métier : Analyse de la fréquence et de la sévérité des sinistres par segment

{{ config(
    materialized='table',
    tags=['actuary', 'kpi', 'daily']
) }}

with claims as (
    select * from {{ ref('int_claims_enriched') }}
),

policies as (
    select
        line_of_business,
        region,
        age_segment,
        inception_year,
        count(policy_id) as policy_count
    from {{ ref('stg_policies') }}
    group by 1, 2, 3, 4
),

claims_agg as (
    select
        line_of_business,
        region,
        age_segment,
        accident_year,
        claim_type,

        count(claim_id)                             as claim_count,
        sum(ultimate_cost_eur)                      as total_incurred_eur,
        avg(ultimate_cost_eur)                      as avg_severity_eur,
        percentile_cont(0.5) within group (order by ultimate_cost_eur)
                                                    as median_severity_eur,
        max(ultimate_cost_eur)                      as max_severity_eur,
        avg(reporting_lag_days)                      as avg_reporting_lag,
        count(case when is_ibnr_candidate then 1 end)
                                                    as ibnr_count,

        count(case when severity_band = 'Small' then 1 end)  as small_claims,
        count(case when severity_band = 'Medium' then 1 end) as medium_claims,
        count(case when severity_band = 'Large' then 1 end)  as large_claims,
        count(case when severity_band = 'Severe' then 1 end) as severe_claims

    from claims
    group by 1, 2, 3, 4, 5
),

final as (
    select
        ca.*,
        p.policy_count,

        case
            when p.policy_count > 0
            then round(ca.claim_count::float / p.policy_count, 6)
            else null
        end                                         as observed_frequency,

        current_timestamp                           as _refreshed_at

    from claims_agg ca
    left join policies p
        on  ca.line_of_business = p.line_of_business
        and ca.region = p.region
        and ca.age_segment = p.age_segment
        and ca.accident_year = p.inception_year
)

select * from final
order by accident_year desc, observed_frequency desc nulls last
