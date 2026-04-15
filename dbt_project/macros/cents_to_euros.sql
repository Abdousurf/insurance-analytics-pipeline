{% macro cents_to_euros(column_name) %}
    round({{ column_name }} / 100.0, 2)
{% endmacro %}
