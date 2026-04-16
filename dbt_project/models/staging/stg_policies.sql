-- stg_policies.sql
-- Modèle staging : normalisation et enrichissement des données brutes de polices

with source as (
    select * from {{ source('raw', 'policies') }}
),

renamed as (
    select
        policy_id,
        lower(lob)                                  as line_of_business,
        region,
        insured_age,
        cast(inception_date as date)                as inception_date,
        cast(expiry_date as date)                   as expiry_date,
        cast(annual_premium as decimal(12, 2))      as annual_premium_eur,
        initcap(status)                             as policy_status,
        channel,

        -- Champs dérivés
        datediff('day', inception_date, expiry_date) / 365.25 as duration_years,

        case
            when insured_age between 18 and 25 then 'Young Driver'
            when insured_age between 26 and 40 then '26-40'
            when insured_age between 41 and 60 then '41-60'
            when insured_age > 60              then 'Senior'
            else 'Unknown'
        end                                         as age_segment,

        date_part('year', inception_date)           as inception_year,

        current_timestamp                           as _loaded_at

    from source
    where policy_id is not null
      and annual_premium > 0
      and inception_date < expiry_date
)

select * from renamed
