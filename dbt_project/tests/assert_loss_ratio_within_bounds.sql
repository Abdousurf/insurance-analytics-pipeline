-- Test personnalisé : aucun segment ne doit avoir un ratio S/P supérieur à 500 %
-- (contrôle de cohérence pour détecter les problèmes de qualité de données)

select
    line_of_business,
    region,
    accident_year,
    loss_ratio
from {{ ref('mart_loss_ratio') }}
where loss_ratio > 5.0
  and earned_premium_eur > 10000
