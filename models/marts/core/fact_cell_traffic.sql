
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'statistic_traffic_id'
) }}

with stats as (

    select
        *
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
        -- y otros atributos si quieres arrastrarlos como degenerate
    from {{ ref('dim_cell') }}

),

joined as (

    select
        s.statistic_traffic_id                         as traffic_event_key,
        d.date_id                                      as measure_date_id,
        c.cell_id                                      as cell_key,

        -- Atributos de contexto
        s.measure_time,
        s.measure_period,

        -- Métricas (cast explícito a numérico si hace falta)
        s.rb_utilizing_rate_dl_pct::number(5,2)        as rb_utilizing_rate_dl_pct,
        s.erab_estab_succ_rate_pct::number(5,2)        as erab_estab_succ_rate_pct,
        s.call_setup_succ_rate_pct::number(5,2)        as call_setup_succ_rate_pct,
        s.cell_dl_avg_thp_kbps::number(18,2)           as cell_dl_avg_thp_kbps,
        s.cell_ul_avg_thp_kbps::number(18,2)           as cell_ul_avg_thp_kbps,
        s.dl_cell_thp_mbps::number(18,2)               as dl_cell_thp_mbps,
        s.dl_user_thp_mbps::number(18,2)               as dl_user_thp_mbps,
        s.trafico_dl::number(18,2)                     as trafico_dl,

        s.datetime_row_loaded                          as fact_created_at

    from stats s
    left join dim_date d
        on date(s.measure_time) = d.date_day
    left join dim_cell c
        on s.cell_id = c.cell_id

)

select * from joined

