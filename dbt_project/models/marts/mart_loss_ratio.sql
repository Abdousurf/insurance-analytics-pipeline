-- mart_loss_ratio.sql
-- Mart métier : Ratio S/P (Loss Ratio) par branche, région, année
-- KPI central pour le suivi actuariel et la souscription IARD

{{ config(
    materialized='table',
    tags=['actuary', 'kpi', 'daily']
) }}

with policies_earned as (
    select
        line_of_business,
        region,
        inception_year                              as accident_year,
        count(policy_id)                            as policy_count,
        sum(annual_premium_eur)                     as earned_premium_eur
    from {{ ref('stg_policies') }}
    group by 1, 2, 3
),

claims_incurred as (
    select
        c.line_of_business,
        c.region,
        date_part('year', c.claim_date)             as accident_year,
        count(c.claim_id)                           as claim_count,
        sum(c.ultimate_cost_eur)                    as incurred_losses_eur,
        sum(c.paid_amount_eur)                      as paid_losses_eur,
        sum(c.reserve_eur)                          as total_reserve_eur,
        count(case when c.is_ibnr_candidate then 1 end) as ibnr_claims_count
    from {{ ref('stg_claims') }} c
    group by 1, 2, 3
),

combined as (
    select
        coalesce(p.line_of_business, c.line_of_business)   as line_of_business,
        coalesce(p.region, c.region)                       as region,
        coalesce(p.accident_year, c.accident_year)         as accident_year,
        coalesce(p.policy_count, 0)                        as policy_count,
        coalesce(p.earned_premium_eur, 0)                  as earned_premium_eur,
        coalesce(c.claim_count, 0)                         as claim_count,
        coalesce(c.incurred_losses_eur, 0)                 as incurred_losses_eur,
        coalesce(c.paid_losses_eur, 0)                     as paid_losses_eur,
        coalesce(c.total_reserve_eur, 0)                   as total_reserve_eur,
        coalesce(c.ibnr_claims_count, 0)                   as ibnr_claims_count

    from policies_earned p
    full outer join claims_incurred c
        on  p.line_of_business = c.line_of_business
        and p.region = c.region
        and p.accident_year = c.accident_year
),

final as (
    select
        *,

        -- Ratio S/P (Loss Ratio)
        case
            when earned_premium_eur > 0
            then round(incurred_losses_eur / earned_premium_eur, 4)
            else null
        end                                         as loss_ratio,

        -- Ratio sinistres payés / primes
        case
            when earned_premium_eur > 0
            then round(paid_losses_eur / earned_premium_eur, 4)
            else null
        end                                         as paid_loss_ratio,

        -- Fréquence sinistres
        case
            when policy_count > 0
            then round(claim_count::float / policy_count, 4)
            else null
        end                                         as claims_frequency,

        -- Sévérité moyenne
        case
            when claim_count > 0
            then round(incurred_losses_eur / claim_count, 2)
            else null
        end                                         as avg_severity_eur,

        -- Taux IBNR
        case
            when claim_count > 0
            then round(ibnr_claims_count::float / claim_count, 4)
            else null
        end                                         as ibnr_rate,

        -- Indicateur d'alerte : ratio S/P supérieur à 85 %
        case
            when earned_premium_eur > 0
             and incurred_losses_eur / earned_premium_eur > 0.85
            then true
            else false
        end                                         as high_loss_ratio_flag,

        current_timestamp                           as _refreshed_at

    from combined
)

select * from final
order by accident_year desc, loss_ratio desc nulls last
