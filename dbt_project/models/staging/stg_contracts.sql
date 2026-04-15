-- stg_contracts.sql
-- Staging model: normalize reinsurance treaty contracts

with source as (
    select * from {{ source('raw', 'contracts') }}
),

renamed as (
    select
        contract_id,
        lower(lob)                                  as line_of_business,
        treaty_type,
        cast(retention as decimal(15, 2))           as retention,
        cast("limit" as decimal(15, 2))             as treaty_limit,
        reinstatements,
        cast(premium_rate as decimal(8, 6))         as premium_rate,
        cast(effective_date as date)                as effective_date,
        cast(expiry_date as date)                   as expiry_date,

        datediff('day', effective_date, expiry_date) as treaty_duration_days,

        current_timestamp                           as _loaded_at

    from source
    where contract_id is not null
)

select * from renamed
