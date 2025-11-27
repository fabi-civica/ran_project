
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'alarm_event_id'
) }}

with alarm_log  as (

    select
        *
    from {{ ref('stg_alumno15__alarm_log') }}

    {% if is_incremental() %}
      where datetime_row_loaded >
            (select coalesce(max(fact_created_at), to_timestamp('1900-01-01','YYYY-MM-DD'))
             from {{ this }})
    {% endif %}

),

dim_date as (

    select
        *
    from {{ ref('dim_date') }}

),

dim_time_hour as (
    
    select 
        * 
    from {{ ref('dim_time_hour') }}
),

dim_alarm as (

    select
        *
    from {{ ref('dim_alarm_type') }}

),

dim_bs as (

    select
        *
    from {{ ref('dim_bs') }}

),

final as (

    select
        al.alarm_event_id,
        da.alarm_id,
        bs.bs_id,
        d_occ.date_id  as occurred_date_id,
        d_clr.date_id  as cleared_date_id,
        d_rec.date_id  as received_date_id,
        t_occ.time_hour_id as occurred_hour_id,
        t_clr.time_hour_id as cleared_hour_id,
        t_rec.time_hour_id as received_hour_id,
        al.log_serial_number,
        al.location_descriptor,
        al.location_information_raw,
        al.associate_type,
        1 as alarm_count,
        al.is_cleared,
        al.cleared_by_id, -- crear dim_cleared
        datediff('minute', al.occurred_on,  al.cleared_on)   as duration_to_solve_minutes,
        datediff('minute', al.occurred_on,  al.received_on)  as time_to_receive_alarm_minutes,
        al.occurred_on,
        al.cleared_on,
        al.received_on,
        al.datetime_row_loaded as fact_created_at

    from alarm_log al
    left join dim_alarm da          on al.alarm_id = da.alarm_id
    left join dim_bs    bs          on al.bs_id = bs.bs_id
    left join dim_date d_occ        on date(al.occurred_on) = d_occ.date_day
    left join dim_date d_clr        on al.cleared_on is not null
       and date(al.cleared_on) = d_clr.date_day
    left join dim_date d_rec        on al.received_on is not null
       and date(al.received_on) = d_rec.date_day
    left join dim_time_hour t_occ   on date_part('hour', al.occurred_on) = t_occ.time_hour_id
    left join dim_time_hour t_clr   on al.cleared_on is not null
       and date_part('hour', al.cleared_on) = t_clr.time_hour_id
    left join dim_time_hour t_rec   on al.received_on is not null
       and date_part('hour', al.received_on) = t_rec.time_hour_id

)

select *
from final