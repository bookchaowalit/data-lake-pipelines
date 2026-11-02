{#- Generic test: no two rows share the same values in `columns`.
    (Small local stand-in for dbt_utils.unique_combination_of_columns, so the
    project keeps zero package dependencies.) -#}
{% test unique_combination_of_columns(model, columns) %}
    select {{ columns | join(', ') }}
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}
