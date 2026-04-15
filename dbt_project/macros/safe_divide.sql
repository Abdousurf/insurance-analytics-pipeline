{% macro safe_divide(numerator, denominator, precision=4) %}
    case
        when {{ denominator }} is not null and {{ denominator }} != 0
        then round({{ numerator }}::float / {{ denominator }}, {{ precision }})
        else null
    end
{% endmacro %}
