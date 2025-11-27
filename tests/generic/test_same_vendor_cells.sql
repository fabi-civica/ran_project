
{% test same_vendor_cell(model, node_column, cell_column) %}

    select
        *
    from {{ model }}
    where substr({{ cell_column }}, 1, 4) <> substr({{ node_column }}, 1, 4)
      and {{ cell_column }} is not null
      and {{ node_column }} is not null

{% endtest %}

