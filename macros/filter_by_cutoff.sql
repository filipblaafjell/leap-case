{% macro filter_by_cutoff(date_column, start_date="'2018-01-01'", end_date="'2022-12-31'") %}
    {{ date_column }} between {{ start_date }} and {{ end_date }}
{% endmacro %}