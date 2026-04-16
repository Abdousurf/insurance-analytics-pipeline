{#
  actuarial_metrics.sql
  =====================
  Macros dbt réutilisables pour les KPIs actuariels centraux.

  Macros définies ici :
    - loss_ratio(losses, premiums)       → ratio S/P (null-safe)
    - burning_cost(losses, exposure)     → coût normalisé par unité d'exposition

  Exemples d'utilisation :
    {{ loss_ratio('ultimate_cost_eur', 'annual_premium_eur') }}
    {{ burning_cost('ultimate_cost_eur', 'duration_years') }}

  Les deux macros retournent NULL lorsque le dénominateur est zéro ou NULL,
  ce qui empêche les erreurs de division par zéro sans masquer silencieusement
  les problèmes de données.
#}


{# ─────────────────────────────────────────────────────────────────────────────
   loss_ratio(losses, premiums)
   ─────────────────────────────────────────────────────────────────────────────
   Calcule le ratio S/P (Sinistres sur Primes) : le rapport entre les sinistres
   encourus et les primes acquises.

   Référence marché : 68–75 % pour un portefeuille IARD équilibré.

   Args :
     losses   : Expression de colonne pour les sinistres encourus/ultimes (EUR).
     premiums : Expression de colonne pour les primes acquises (EUR).
     decimals : Nombre de décimales pour l'arrondi du résultat (par défaut : 4).

   Retourne :
     Decimal(precision=decimals) ou NULL lorsque les primes = 0.
#}
{% macro loss_ratio(losses, premiums, decimals=4) %}
    case
        when {{ premiums }} is not null
         and {{ premiums }} > 0
        then round(
            cast({{ losses }} as decimal(18, 6)) /
            cast({{ premiums }} as decimal(18, 6)),
            {{ decimals }}
        )
        else null
    end
{% endmacro %}


{# ─────────────────────────────────────────────────────────────────────────────
   burning_cost(losses, exposure)
   ─────────────────────────────────────────────────────────────────────────────
   Calcule le Burning Cost : sinistres encourus divisés par la mesure d'exposition
   (typiquement police-années ou nombre d'unités de risque).

   Le burning cost est utilisé en tarification et en experience rating pour
   exprimer le coût « pur » par unité d'exposition, en éliminant l'effet
   des chargements sur la prime.

   Args :
     losses   : Expression de colonne pour les sinistres encourus/ultimes (EUR).
     exposure : Expression de colonne pour la mesure d'exposition (ex. duration_years).
     decimals : Nombre de décimales pour l'arrondi du résultat (par défaut : 2).

   Retourne :
     Decimal(precision=decimals) représentant l'EUR par unité d'exposition,
     ou NULL lorsque l'exposition = 0.
#}
{% macro burning_cost(losses, exposure, decimals=2) %}
    case
        when {{ exposure }} is not null
         and {{ exposure }} > 0
        then round(
            cast({{ losses }} as decimal(18, 6)) /
            cast({{ exposure }} as decimal(18, 6)),
            {{ decimals }}
        )
        else null
    end
{% endmacro %}


{# ─────────────────────────────────────────────────────────────────────────────
   combined_ratio(losses, expenses, premiums)
   ─────────────────────────────────────────────────────────────────────────────
   Macro de commodité qui additionne le ratio S/P et le ratio de frais pour
   produire le Ratio Combiné — l'indicateur global de rentabilité d'un assureur IARD.

   Ratio Combiné < 100 % = bénéfice technique.
   Ratio Combiné > 100 % = perte technique (peut être compensée par les revenus financiers).

   Args :
     losses   : Expression de colonne pour les sinistres encourus.
     expenses : Expression de colonne pour les frais de souscription.
     premiums : Expression de colonne pour les primes acquises.
     decimals : Précision de l'arrondi (par défaut : 4).
#}
{% macro combined_ratio(losses, expenses, premiums, decimals=4) %}
    case
        when {{ premiums }} is not null
         and {{ premiums }} > 0
        then round(
            (cast({{ losses }} as decimal(18, 6)) +
             cast({{ expenses }} as decimal(18, 6))) /
            cast({{ premiums }} as decimal(18, 6)),
            {{ decimals }}
        )
        else null
    end
{% endmacro %}
