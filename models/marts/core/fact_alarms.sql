-- unique_key = 'alarm_id'     -- de cara a futuros incrementales / SCD2
{{ config(
    materialized = 'incremental'
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
        al.log_serial_number,
        al.location_descriptor,
        al.location_information_raw,
        al.associate_type,
        1              as alarm_count,
        al.is_cleared,
        datediff('minutes', al.occurred_on, al.cleared_on)   as duration_to_solve_minutes,
        datediff('minutes', al.occurred_on, al.received_on)  as time_to_receive_alarm_minutes,
        al.occurred_on,
        al.cleared_on,
        al.received_on,
        al.cleared_by_id,
        al.datetime_row_loaded  as fact_created_at

    from alarm_log al
    left join dim_alarm da
        on al.alarm_id = da.alarm_id
    left join dim_bs bs
        on al.bs_id = bs.bs_id
    left join dim_date d_occ
        on date(al.occurred_on) = d_occ.date_day
    left join dim_date d_clr
        on date(al.cleared_on)  = d_clr.date_day
    left join dim_date d_rec
        on date(al.received_on) = d_rec.date_day

)

select
    *
from final
