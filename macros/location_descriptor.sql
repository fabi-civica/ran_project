
{% macro location_descriptor(field) %}

    coalesce(
        regexp_substr({{ field }}, 'Specific Problem=([^,]+)', 1, 1, 'e', 0),
        regexp_substr({{ field }}, 'ThresholdName=([^,]+)',   1, 1, 'e', 0),
        regexp_substr({{ field }}, 'Service Type=([^,]+)',    1, 1, 'e', 0),
        regexp_substr({{ field }}, 'Peer Mode=([^,]+)',       1, 1, 'e', 0),
        regexp_substr({{ field }}, 'Description=([^,]+)',     1, 1, 'e', 0),
        regexp_substr({{ field }}, 'ObjectName="([^"]+)"',    1, 1, 'e', 0),
        {{ field }}
    )

{% endmacro %}
