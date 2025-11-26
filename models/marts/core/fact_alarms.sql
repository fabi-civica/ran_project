
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'alarm_event_id'
) }}

with src as (

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

final as (

    select
        s.alarm_event_id,
        s.alarm_id,
        s.bs_id,
        d_occ.date_id  as occurred_date_id,
        d_clr.date_id  as cleared_date_id,
        s.log_serial_number,
        s.associate_type,
        s.location_descriptor,
        s.location_information_raw,
        s.occurred_on,
        s.cleared_on,
        s.received_on,
        s.cleared_by_id,
        s.is_cleared,
        case
            when s.is_cleared
                then datediff('minute', s.occurred_on, s.cleared_on)
            else null
        end as duration_minutes,
        s.datetime_row_loaded as fact_created_at

    from src s
    left join dim_date d_occ
        on d_occ.date_day = cast(s.occurred_on as date)
    left join dim_date d_clr
        on d_clr.date_day = cast(s.cleared_on as date)

)

select
    *
from final
