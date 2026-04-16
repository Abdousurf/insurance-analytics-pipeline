-- int_portfolio_summary.sql
-- Modèle intermédiaire : résumé de la performance du portefeuille par branche
--
-- Agrège int_claims_enriched pour produire des KPIs actuariels par branche,
-- destinés à être consommés par les modèles marts et le dashboard.
--
-- Grain : une ligne par (line_of_business, region, accident_year)
--
-- Métriques clés :
--   earned_premium_eur   — total des primes annuelles du segment
--   incurred_losses_eur  — somme des coûts ultimes des sinistres
--   portfolio_loss_ratio — ratio S/P au niveau du segment
--   avg_burning_cost     — burning cost moyen par police-année
--   claims_frequency     — nombre de sinistres rapporté au nombre de polices
--   avg_severity_eur     — coût ultime moyen par sinistre
--   herfindahl_index     — proxy de concentration du portefeuille (par nombre de sinistres)

{{ config(
    materialized='view',
    tags=['actuary', 'intermediate', 'portfolio']
) }}

with enriched as (
    select * from {{ ref('int_claims_enriched') }}
),

-- Agrégation au grain (branche, région, année de survenance)
segment_agg as (
    select
        line_of_business,
        region,
        accident_year,

        -- Métriques de volume
        count(distinct policy_id)                           as policy_count,
        count(claim_id)                                     as claim_count,
        count(case when claim_status = 'Open' then 1 end)   as open_claim_count,
        count(case when is_ibnr_candidate then 1 end)       as ibnr_candidate_count,

        -- Agrégats primes et sinistres
        sum(annual_premium_eur)                             as earned_premium_eur,
        sum(ultimate_cost_eur)                              as incurred_losses_eur,
        sum(paid_amount_eur)                                as paid_losses_eur,
        sum(reserve_eur)                                    as total_reserve_eur,
        sum(outstanding_reserve_eur)                        as outstanding_reserve_eur,

        -- Burning cost (ajusté par l'exposition)
        avg(burning_cost_eur)                               as avg_burning_cost_eur,
        sum(burning_cost_eur)                               as total_burning_cost_eur

    from enriched
    group by 1, 2, 3
),

-- Calcul des ratios actuariels dérivés
with_ratios as (
    select
        *,

        -- Ratio S/P au niveau du portefeuille
        case
            when earned_premium_eur > 0
            then round(incurred_losses_eur / earned_premium_eur, 4)
            else null
        end                                                 as portfolio_loss_ratio,

        -- Ratio sinistres payés / primes
        case
            when earned_premium_eur > 0
            then round(paid_losses_eur / earned_premium_eur, 4)
            else null
        end                                                 as paid_loss_ratio,

        -- Fréquence sinistres (sinistres par police)
        case
            when policy_count > 0
            then round(claim_count::float / policy_count, 4)
            else null
        end                                                 as claims_frequency,

        -- Sévérité moyenne par sinistre
        case
            when claim_count > 0
            then round(incurred_losses_eur / claim_count, 2)
            else null
        end                                                 as avg_severity_eur,

        -- Taux de candidats IBNR
        case
            when claim_count > 0
            then round(ibnr_candidate_count::float / claim_count, 4)
            else null
        end                                                 as ibnr_rate,

        -- Ratio d'adéquation des réserves (réserves vs encours)
        case
            when incurred_losses_eur > 0
            then round(total_reserve_eur / incurred_losses_eur, 4)
            else null
        end                                                 as reserve_adequacy_ratio,

        -- Indicateur d'alerte : ratio S/P supérieur au seuil de 85 %
        case
            when earned_premium_eur > 0
             and incurred_losses_eur / earned_premium_eur > 0.85
            then true
            else false
        end                                                 as high_loss_ratio_flag,

        current_timestamp                                   as _refreshed_at

    from segment_agg
),

-- Totaux par branche pour l'indice de concentration de Herfindahl
lob_totals as (
    select
        line_of_business,
        accident_year,
        sum(claim_count)                                    as lob_total_claims
    from segment_agg
    group by 1, 2
),

-- Indice de Herfindahl : mesure la concentration du portefeuille au sein de chaque branche-année.
-- Des valeurs plus élevées indiquent que moins de régions concentrent le volume de sinistres.
final as (
    select
        r.*,
        round(
            power(r.claim_count::float / nullif(t.lob_total_claims, 0), 2),
            4
        )                                                   as herfindahl_contribution
    from with_ratios r
    left join lob_totals t
        on  r.line_of_business = t.line_of_business
        and r.accident_year    = t.accident_year
)

select * from final
order by accident_year desc, portfolio_loss_ratio desc nulls last
