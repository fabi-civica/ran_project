
{{ config(
    materialized = 'table'
) }}

with raw_spine as (

    {{ dbt_utils.date_spine(
        datepart = "hour",
        start_date = "to_timestamp('2025-01-01 00:00:00')",
        end_date   = "to_timestamp('2025-01-02 00:00:00')"
    ) }}

),

hours as (

    select
        date_part('hour', date_hour) as hour_24
    from raw_spine

),

final as (

    select distinct
        hour_24                                        as time_hour_id,
        hour_24                                        as hour_24,
        case when hour_24 = 0 then 24 else hour_24 end as hour_24_1_24,
        to_char(time_from_parts(hours.hour_24, 0, 0), 'HH24:MI')  as hour_label_24,
        to_char(time_from_parts(hours.hour_24, 0, 0), 'HH:MI AM') as hour_label_12,
        case
            when hour_24 between 0 and 5  then 'NIGHT'
            when hour_24 between 6 and 11 then 'MORNING'
            when hour_24 between 12 and 17 then 'AFTERNOON'
            else 'EVENING'
        end as day_part
    from hours

)

select *
from final
order by hour_24