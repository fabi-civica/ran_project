
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='alarm_event_id'
) }}

with source_raw_alarm_logs as (

    select * from {{ source('alumno15', 'raw_alarm_logs') }}

),

renamed_alarm_log as (

    select
        {{ dbt_utils.generate_surrogate_key(['alarm_id', 'occurred_on_nt']) }} as alarm_event_id,
        {{ dbt_utils.generate_surrogate_key(['alarm_id']) }} as alarm_id,
        {{ dbt_utils.generate_surrogate_key(['alarm_source']) }} as bs_id,
        Associate_type::varchar(20) as associate_type,
        convert_timezone('Europe/Madrid',  coalesce(
            try_to_timestamp_ntz(regexp_replace(occurred_on_nt, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz(occurred_on_nt, 'MM/DD/YYYY HH24:MI'),
            to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI'))) as occurred_on,
        convert_timezone('Europe/Madrid',  coalesce(
            try_to_timestamp_ntz(regexp_replace(cleared_on_nt, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz(cleared_on_nt, 'MM/DD/YYYY HH24:MI'),
            to_timestamp_ntz('2100-01-01 00:01', 'YYYY-MM-DD HH24:MI'))) as cleared_on,
        {{ dbt_utils.generate_surrogate_key(['cleared_by']) }} as cleared_by_id,
        case when clearance_status = 'Uncleared' then FALSE ELSE TRUE END AS is_cleared,
        log_serial_number::number(15,0) as log_serial_number,
        convert_timezone('Europe/Madrid',  coalesce(
            try_to_timestamp_ntz(regexp_replace( received_on_st, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz( received_on_st, 'MM/DD/YYYY HH24:MI'),
            to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI'))) as  received_on,
        location_information as location_information_raw,
        {{ location_descriptor('location_information') }} as location_descriptor,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded

    from source_raw_alarm_logs

    {% if is_incremental() %}
      where convert_timezone('Europe/Madrid',  coalesce(
                try_to_timestamp_ntz(regexp_replace(occurred_on_nt, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
                try_to_timestamp_ntz(occurred_on_nt, 'MM/DD/YYYY HH24:MI'),
                to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI')
            )) >
            (
              select coalesce(
                  max(occurred_on),
                  to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI')
              )
              from {{ this }}
            )
    {% endif %}

)

select * from renamed_alarm_log
