-- Custom test: no segment should have a loss ratio exceeding 500%
-- (sanity check for data quality issues)

select
    line_of_business,
    region,
    accident_year,
    loss_ratio
from {{ ref('mart_loss_ratio') }}
where loss_ratio > 5.0
  and earned_premium_eur > 10000
