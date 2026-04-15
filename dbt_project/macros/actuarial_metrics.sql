{#
  actuarial_metrics.sql
  =====================
  Reusable dbt macros for core actuarial KPIs.

  Macros defined here:
    - loss_ratio(losses, premiums)       → incurred S/P ratio (null-safe)
    - burning_cost(losses, exposure)     → normalised cost per unit of exposure

  Usage examples:
    {{ loss_ratio('ultimate_cost_eur', 'annual_premium_eur') }}
    {{ burning_cost('ultimate_cost_eur', 'duration_years') }}

  Both macros return NULL when the denominator is zero or NULL, preventing
  division-by-zero errors without silently masking data issues.
#}


{# ─────────────────────────────────────────────────────────────────────────────
   loss_ratio(losses, premiums)
   ─────────────────────────────────────────────────────────────────────────────
   Calculates the Loss Ratio (also called S/P — Sinistres sur Primes in French
   actuarial convention): the ratio of incurred losses to earned premiums.

   Industry benchmark: 68–75% for a balanced P&C portfolio.

   Args:
     losses   : Column expression for incurred/ultimate losses (EUR).
     premiums : Column expression for earned premiums (EUR).
     decimals : Number of decimal places to round the result (default: 4).

   Returns:
     Decimal(precision=decimals) or NULL when premiums = 0.
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
   Calculates the Burning Cost: incurred losses divided by the exposure measure
   (typically policy-years or number of risk units).

   Burning cost is used in pricing and experience rating to express the "pure"
   cost per unit of exposure, stripping out the effect of premium loadings.

   Args:
     losses   : Column expression for incurred/ultimate losses (EUR).
     exposure : Column expression for the exposure measure (e.g. duration_years).
     decimals : Number of decimal places to round the result (default: 2).

   Returns:
     Decimal(precision=decimals) representing EUR per unit of exposure,
     or NULL when exposure = 0.
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
   Convenience macro that adds the loss ratio and expense ratio to produce the
   Combined Ratio — the overall profitability indicator for a P&C insurer.

   Combined Ratio < 100% = underwriting profit.
   Combined Ratio > 100% = underwriting loss (may still be offset by investment income).

   Args:
     losses   : Incurred losses column expression.
     expenses : Underwriting expenses column expression.
     premiums : Earned premiums column expression.
     decimals : Rounding precision (default: 4).
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
