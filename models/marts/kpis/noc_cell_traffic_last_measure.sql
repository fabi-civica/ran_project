-- models/marts/performance/noc_cell_traffic_last_measure.sql
-- Vista operacional: última medida de tráfico por celda

{{ config(
    materialized = 'view'
) }}

with last_measure_per_cell as (

    -- Para cada celda, obtenemos el último timestamp de medida
    select
        cell_key,
        max(measure_time) as last_measure_time
    from {{ ref('fact_cell_traffic') }}
    group by cell_key

),

fact_last as (

    -- Filtramos fact_cell_traffic a solo la última medida por celda
    select
        f.*
    from {{ ref('fact_cell_traffic') }} f
    join last_measure_per_cell lm
        on  f.cell_key     = lm.cell_key
        and f.measure_time = lm.last_measure_time

),

dim_cell as (

    select
        c.cell_id       as cell_key,
        c.cell_name,
        c.local_cell_id,
        c.bs_id,
        c.tac,
        c.freqband_id,
        c.dlearfcn,
        c.bandwidth_id,
        c.cell_status,
        c.cell_topology_type
    from {{ ref('dim_cell') }} c

),

dim_bs as (

    select
        b.bs_id,
        b.bs_name,
        b.ne_id,
        b.rat_technology,
        b.home_subnet_id,
        b.software_version_id,
        b.vendor_id
    from {{ ref('dim_bs') }} b

),

dim_vendor as (

    select
        v.vendor_id,
        v.vendor_name
    from {{ ref('dim_vendor') }} v

)

select
    -- Claves técnicas
    f.traffic_event_key,
    f.cell_key,
    f.measure_date_id,
    f.measure_time,

    -- Identificación celda / nodo / vendor
    c.cell_name,
    c.local_cell_id,
    c.tac,
    c.freqband_id,
    c.dlearfcn,
    c.bandwidth_id,
    c.cell_status,
    c.cell_topology_type,

    b.bs_name,
    b.ne_id,
    b.rat_technology,
    b.home_subnet_id,
    b.software_version_id,

    v.vendor_name,

    -- Métricas de performance
    f.rb_utilizing_rate_dl_pct,
    f.erab_estab_succ_rate_pct,
    f.call_setup_succ_rate_pct,
    f.cell_dl_avg_thp_kbps,
    f.cell_ul_avg_thp_kbps,
    f.dl_cell_thp_mbps,
    f.dl_user_thp_mbps,
    f.trafico_dl

from fact_last f
join dim_cell   c on f.cell_key = c.cell_key
join dim_bs     b on c.bs_id    = b.bs_id
join dim_vendor v on b.vendor_id = v.vendor_id
