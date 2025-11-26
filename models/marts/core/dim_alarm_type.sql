-- unique_key = 'alarm_id'     -- de cara a futuros incrementales / SCD2
{{ config(
    materialized = 'table'
) }}

with alarm_details as (

    select
        *
    from {{ ref('stg_alumno15__alarms_details') }}

)

select

    alarm_id,
    alarm_id_by_vendor,
    vendor_id,
    alarm_name,
    severity,
    source_system,
    alarm_type,
    auto_clear,
    object_type,
    datetime_row_loaded as dim_created_at

from alarm_details
