-- mart_portfolio_performance.sql
-- Business mart: Portfolio concentration and performance metrics

{{ config(
    materialized='table',
    tags=['actuary', 'kpi', 'daily']
) }}

with portfolio as (
    select * from {{ ref('int_portfolio') }}
),

claims as (
    select
        line_of_business,
        region,
        accident_year,
        count(claim_id)                             as claim_count,
        sum(ultimate_cost_eur)                      as incurred_eur,
        sum(paid_amount_eur)                        as paid_eur,
        sum(reserve_eur)                            as reserve_eur
    from {{ ref('int_claims_enriched') }}
    group by 1, 2, 3
),

total_premium as (
    select sum(total_premium_eur) as grand_total_premium
    from portfolio
),

final as (
    select
        pf.line_of_business,
        pf.region,
        pf.inception_year,
        pf.channel,
        pf.age_segment,

        pf.policy_count,
        pf.total_premium_eur,
        pf.avg_premium_eur,
        pf.active_policies,
        pf.expired_policies,
        pf.avg_insured_age,

        pf.treaty_type                              as reinsurance_type,
        pf.reinsurance_retention,
        pf.reinsurance_limit,

        coalesce(c.claim_count, 0)                  as claim_count,
        coalesce(c.incurred_eur, 0)                 as incurred_eur,
        coalesce(c.paid_eur, 0)                     as paid_eur,
        coalesce(c.reserve_eur, 0)                  as reserve_eur,

        case
            when pf.total_premium_eur > 0
            then round(coalesce(c.incurred_eur, 0) / pf.total_premium_eur, 4)
            else null
        end                                         as loss_ratio,

        -- Portfolio concentration: share of total premium
        case
            when tp.grand_total_premium > 0
            then round(pf.total_premium_eur / tp.grand_total_premium, 6)
            else null
        end                                         as premium_share,

        -- Herfindahl index component (squared share)
        case
            when tp.grand_total_premium > 0
            then power(pf.total_premium_eur / tp.grand_total_premium, 2)
            else null
        end                                         as herfindahl_component,

        current_timestamp                           as _refreshed_at

    from portfolio pf
    cross join total_premium tp
    left join claims c
        on  pf.line_of_business = c.line_of_business
        and pf.region = c.region
        and pf.inception_year = c.accident_year
)

select * from final
order by total_premium_eur desc
