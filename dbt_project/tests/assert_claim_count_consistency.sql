-- Test personnalisé : le nombre de sinistres dans le mart doit correspondre au total du staging

with mart_total as (
    select sum(claim_count) as total from {{ ref('mart_loss_ratio') }}
),
staging_total as (
    select count(*) as total from {{ ref('stg_claims') }}
)

select
    m.total as mart_claims,
    s.total as staging_claims
from mart_total m
cross join staging_total s
where m.total != s.total
