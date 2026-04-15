{% macro generate_year_spine(start_year, end_year) %}
    select unnest(generate_series({{ start_year }}, {{ end_year }})) as year_value
{% endmacro %}
