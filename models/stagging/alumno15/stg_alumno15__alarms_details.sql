
{{ config(materialized='view') }}

with source_raw_alarm_logs as (

    select * from {{ source('alumno15', 'raw_alarm_logs') }}

),

transform_alarm_log as (

    select 
        *
    from source_raw_alarm_logs
),

renamed_alarm_log as (

    select distinct
        alarm_id,
        trim(severity) as severity,
        name::varchar(100) as alarm_name,
        trim(source_system) as source_system,
        trim(type) as alarm_type,
        trim(auto_clear) as auto_clear,
        trim(object_type) as object_type


    from transform_alarm_log

)

select * from renamed_alarm_log
 --24