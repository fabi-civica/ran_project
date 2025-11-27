
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'statistic_traffic_id'
) }}

with stats as (

    select
        *,
        date(measure_time) as date_day,
        date_part('hour', measure_time) as time_hour_id

    from {{ ref('stg_alumno15__statistics_traffic') }}

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

dim_cell as (

    select
        *
    from {{ ref('dim_cell') }}

),

dim_time_hour as (
    
    select 
        * 
    from {{ ref('dim_time_hour') }}
),

stats_mod as (
    
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_id

    from stats
),

joined as (

    select
        s.statistic_traffic_id,
        d.date_id as measure_date_id,
        t.time_hour_id as measure_hour_id,
        c.cell_id,

        s.measure_time,
        s.measure_period,

        s.rb_utilizing_rate_dl_pct,
        s.erab_estab_succ_rate_pct,
        s.call_setup_succ_rate_pct,
        s.cell_dl_avg_thp_kbps,
        s.cell_ul_avg_thp_kbps,
        s.dl_cell_thp_mbps,
        s.dl_user_thp_mbps,
        s.trafico_dl,
        s.datetime_row_loaded as fact_created_at

    from stats_mod s
    left join dim_date d        on s.date_day       = d.date_day
    left join dim_cell c        on s.cell_id        = c.cell_id
    left join dim_time_hour t   on s.time_hour_id   = t.time_hour_id

)

select * from joined

