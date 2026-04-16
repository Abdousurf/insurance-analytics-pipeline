-- assert_loss_ratio_positive.sql
-- Test singulier dbt personnalisé
--
-- Vérifie qu'aucune ligne de int_claims_enriched n'a un ratio S/P négatif.
--
-- Un ratio S/P négatif indiquerait que :
--   (a) ultimate_cost_eur est enregistré avec une valeur négative — problème de qualité de données, ou
--   (b) la division dans la macro a produit une erreur arithmétique.
--
-- Les deux cas représentent une violation d'intégrité des données et doivent
-- faire échouer le pipeline.
--
-- Convention dbt : cette requête doit retourner 0 ligne pour PASSER.
-- Toute ligne retournée est traitée comme un échec de test.
--
-- S'applique à : int_claims_enriched (grain = sinistre)
-- Tests associés : assert_paid_not_exceeds_ultimate.sql, tests génériques not_null / positive_value

select
    claim_id,
    policy_id,
    line_of_business,
    region,
    accident_year,
    ultimate_cost_eur,
    annual_premium_eur,
    loss_ratio
from {{ ref('int_claims_enriched') }}
where
    -- Évaluer uniquement les lignes où un ratio S/P peut être calculé
    annual_premium_eur is not null
    and annual_premium_eur > 0
    and loss_ratio is not null
    -- Violation : ratio S/P négatif
    and loss_ratio < 0
