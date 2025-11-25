
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

    select
        {{ dbt_utils.generate_surrogate_key(['alarm_id']) }} as alarm_id,
        {{ dbt_utils.generate_surrogate_key(['alarm_source']) }} as ne_id,
        Associate_type::varchar(20) as associate_type,
        convert_timezone('Europe/Madrid',  coalesce(
            try_to_timestamp_ntz(regexp_replace(occurred_on_nt, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz(occurred_on_nt, 'MM/DD/YYYY HH24:MI'),
            to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI'))) as occurred_on,
        convert_timezone('Europe/Madrid',  coalesce(
            try_to_timestamp_ntz(regexp_replace(cleared_on_nt, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz(cleared_on_nt, 'MM/DD/YYYY HH24:MI'),
            to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI'))) as cleared_on,
        case when clearance_status = 'Uncleared' then FALSE ELSE TRUE END AS is_cleared,
        log_serial_number::number(15,0) as log_serial_number,
        toggling_times::number(3,0) as toggling_times,
        convert_timezone('Europe/Madrid',  coalesce(
            try_to_timestamp_ntz(regexp_replace( received_on_st, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz( received_on_st, 'MM/DD/YYYY HH24:MI'),
            to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI'))) as  received_on


        

    from transform_alarm_log

)

select * from renamed_alarm_log
