-- models/marts/performance/hot_cells_last_7_days.sql
-- TOP celdas más cargadas últimos 7 días

{{ config(
    materialized = 'view'
) }}

with last_7_days as (

    -- límite temporal basado en la dimensión de fechas
    select
        max(measure_date) as max_date
    from {{ ref('cell_daily_traffic') }}

),

filtered as (

    -- filtramos cell_daily_traffic a los últimos 7 días
    select
        cdt.*
    from {{ ref('cell_daily_traffic') }} cdt
    join last_7_days l7
        on cdt.measure_date > dateadd('day', -7, l7.max_date)
       and cdt.measure_date <= l7.max_date

),

aggregated_7d as (

    -- agregamos por celda en la ventana de 7 días
    select
        cell_key,
        -- información de dimensión (las mantenemos tal cual vienen de cell_daily_traffic)
        cell_name,
        local_cell_id,
        tac,
        freqband_id,
        dlearfcn,
        bandwidth_id,
        cell_status,
        cell_topology_type,
        bs_name,
        ne_id,
        rat_technology,
        home_subnet_id,
        software_version_id,
        vendor_name,

        -- ventana temporal
        min(measure_date) as start_date,
        max(measure_date) as end_date,

        -- métricas agregadas
        sum(samples_count)                       as samples_count_7d,
        sum(trafico_dl_sum)                      as trafico_dl_sum_7d,
        avg(trafico_dl_avg)                      as trafico_dl_avg_7d,
        avg(dl_cell_thp_mbps_avg)                as dl_cell_thp_mbps_avg_7d,
        avg(dl_user_thp_mbps_avg)                as dl_user_thp_mbps_avg_7d,
        avg(rb_utilizing_rate_dl_pct_avg)        as rb_utilizing_rate_dl_pct_avg_7d,
        avg(erab_estab_succ_rate_pct_avg)        as erab_estab_succ_rate_pct_avg_7d,
        avg(call_setup_succ_rate_pct_avg)        as call_setup_succ_rate_pct_avg_7d,
        max(dl_cell_thp_mbps_max)                as dl_cell_thp_mbps_max_7d,
        max(trafico_dl_max)                      as trafico_dl_max_7d

    from filtered
    group by
        cell_key,
        cell_name,
        local_cell_id,
        tac,
        freqband_id,
        dlearfcn,
        bandwidth_id,
        cell_status,
        cell_topology_type,
        bs_name,
        ne_id,
        rat_technology,
        home_subnet_id,
        software_version_id,
        vendor_name

),

ranked as (

    select
        a.*,

        -- ranking global por tráfico DL en 7 días
        rank() over (
            order by trafico_dl_sum_7d desc
        ) as global_rank_by_traffic,

        -- ranking por vendor
        rank() over (
            partition by vendor_name
            order by trafico_dl_sum_7d desc
        ) as vendor_rank_by_traffic

    from aggregated_7d a

)

select
    *
from ranked
-- opcional: quedarte solo con las TOP 50 globales
-- where global_rank_by_traffic <= 50
