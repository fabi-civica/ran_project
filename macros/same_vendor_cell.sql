
{% macro same_vendor_cell(node_col, cell_col) %}

    substring({{ cell_col }}, 1, 4) = substring({{ node_col }}, 1, 4)
    
{% endmacro %}
