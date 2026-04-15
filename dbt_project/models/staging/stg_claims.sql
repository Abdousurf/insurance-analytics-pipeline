-- stg_claims.sql
-- Staging model: clean and type-cast raw claims data
-- Applies basic normalization without business logic

with source as (
    select * from {{ source('raw', 'claims') }}
),

renamed as (
    select
        claim_id,
        policy_id,
        lower(lob)                                  as line_of_business,
        region,
        cast(claim_date as date)                    as claim_date,
        cast(reporting_date as date)                as reporting_date,
        cast(ultimate_cost as decimal(15, 2))       as ultimate_cost_eur,
        cast(reserve as decimal(15, 2))             as reserve_eur,
        cast(paid_amount as decimal(15, 2))         as paid_amount_eur,
        initcap(status)                             as claim_status,
        claim_type,

        -- Derived: IBNR flag (reported > 30 days after claim)
        datediff('day', claim_date, reporting_date) as reporting_lag_days,
        case
            when datediff('day', claim_date, reporting_date) > 30 then true
            else false
        end                                         as is_ibnr_candidate,

        -- Audit
        current_timestamp                           as _loaded_at

    from source
    where claim_id is not null
      and policy_id is not null
      and claim_date is not null
),

validated as (
    select *
    from renamed
    where ultimate_cost_eur >= 0
      and paid_amount_eur >= 0
      and paid_amount_eur <= ultimate_cost_eur * 1.05  -- allow 5% overpayment tolerance
)

select * from validated
