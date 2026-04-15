-- int_portfolio.sql
-- Intermediate: portfolio-level aggregation by segment

{{ config(materialized='view') }}

with policies as (
    select * from {{ ref('stg_policies') }}
),

contracts as (
    select * from {{ ref('stg_contracts') }}
),

portfolio as (
    select
        p.line_of_business,
        p.region,
        p.inception_year,
        p.channel,
        p.age_segment,

        count(p.policy_id)                          as policy_count,
        sum(p.annual_premium_eur)                   as total_premium_eur,
        avg(p.annual_premium_eur)                   as avg_premium_eur,
        min(p.annual_premium_eur)                   as min_premium_eur,
        max(p.annual_premium_eur)                   as max_premium_eur,
        avg(p.insured_age)                          as avg_insured_age,

        count(case when p.policy_status = 'Active' then 1 end)
                                                    as active_policies,
        count(case when p.policy_status = 'Expired' then 1 end)
                                                    as expired_policies

    from policies p
    group by 1, 2, 3, 4, 5
)

select
    pf.*,

    c.treaty_type,
    c.retention                                     as reinsurance_retention,
    c.treaty_limit                                  as reinsurance_limit,
    c.premium_rate                                  as reinsurance_rate

from portfolio pf
left join contracts c
    on pf.line_of_business = c.line_of_business
