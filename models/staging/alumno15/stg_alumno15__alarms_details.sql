
{{ config(materialized='view') }}

with source_raw_alarm_logs as (

    select * from {{ source('alumno15', 'raw_alarm_logs') }}

),

renamed_alarm_log as (

    select distinct
        alarm_id as alarm_id_by_vendor,
        {{ dbt_utils.generate_surrogate_key(['alarm_id']) }} as alarm_id,
        {{ dbt_utils.generate_surrogate_key(["'Huawei'"]) }} as vendor_id,
        listagg(distinct name, '/') within group (order by name) as alarm_name,
        listagg(distinct severity, '/') within group (order by severity) as severity,
        listagg(distinct source_system, '/') within group (order by source_system) as source_system,
        listagg(distinct type, '/') within group (order by type) as alarm_type,
        listagg(distinct auto_clear, '/') within group (order by auto_clear) as auto_clear,
        listagg(distinct object_type, '/') within group (order by object_type) as object_type,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded

    from source_raw_alarm_logs
    group by alarm_id

)

select * from renamed_alarm_log
