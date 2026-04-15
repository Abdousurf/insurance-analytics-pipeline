-- int_claims_enriched.sql
-- Intermediate: join claims with policy details for enriched analysis

{{ config(materialized='view') }}

with claims as (
    select * from {{ ref('stg_claims') }}
),

policies as (
    select * from {{ ref('stg_policies') }}
),

enriched as (
    select
        c.claim_id,
        c.policy_id,
        c.line_of_business,
        c.region,
        c.claim_date,
        c.reporting_date,
        c.ultimate_cost_eur,
        c.reserve_eur,
        c.paid_amount_eur,
        c.claim_status,
        c.claim_type,
        c.reporting_lag_days,
        c.is_ibnr_candidate,

        p.annual_premium_eur,
        p.policy_status,
        p.channel,
        p.age_segment,
        p.inception_year,
        p.insured_age,

        date_part('year', c.claim_date)             as accident_year,
        date_part('month', c.claim_date)            as accident_month,

        case
            when c.ultimate_cost_eur < 1000 then 'Small'
            when c.ultimate_cost_eur < 10000 then 'Medium'
            when c.ultimate_cost_eur < 100000 then 'Large'
            else 'Severe'
        end                                         as severity_band,

        c.ultimate_cost_eur / nullif(p.annual_premium_eur, 0)
                                                    as claim_to_premium_ratio

    from claims c
    left join policies p on c.policy_id = p.policy_id
)

select * from enriched
