
{{ config(materialized='table') }}

with time_spine as (

    {{ dbt_utils.date_spine(
        datepart = "second",
        start_date = "cast('2000-01-01 00:00:00' as timestamp)",
        end_date   = "cast('2000-01-02 00:00:00' as timestamp)"
    ) }}

),

times as (

    select
        date_second,
        to_char(date_second, 'HH24:MI:SS') as time_natural_key,
        date_part('hour', date_second)   as hour_of_day,
        date_part('minute', date_second) as minute_of_hour,
        date_part('second', date_second) as second_of_minute,
        to_char(date_second, 'HH24') as hour_label,
        to_char(date_second, 'HH24:MI') as minute_label

    from time_spine

)

select

    {{ dbt_utils.generate_surrogate_key(['time_natural_key']) }} as time_id,
    time_natural_key,
    hour_of_day,
    minute_of_hour,
    second_of_minute,
    hour_label,
    minute_label

from times
