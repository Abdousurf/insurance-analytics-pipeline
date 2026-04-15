-- Custom test: premiums must always be positive in the mart

select
    line_of_business,
    region,
    accident_year,
    earned_premium_eur
from {{ ref('mart_loss_ratio') }}
where earned_premium_eur < 0
