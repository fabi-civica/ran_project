-- models/marts/performance/noc_cell_traffic_timeseries.sql
-- Vista operacional: todas las medidas de tráfico por celda (serie temporal completa)

{{ config(
    materialized = 'view'
) }}

with fact as (

    select
        *
    from {{ ref('fact_cell_traffic') }}

),

dim_date as (

    select
        date_id,
        date_day
        -- si en tu dim_date tienes más atributos (año, mes, etc.)
        -- puedes añadirlos aquí sin problema
    from {{ ref('dim_date') }}

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
    -- Claves
    f.traffic_event_key,
    f.cell_key,
    f.measure_date_id,
    d.date_day          as measure_date,
    f.measure_time,
    f.measure_period,

    -- Dimensión celda
    c.cell_name,
    c.local_cell_id,
    c.tac,
    c.freqband_id,
    c.dlearfcn,
    c.bandwidth_id,
    c.cell_status,
    c.cell_topology_type,

    -- Dimensión BS
    b.bs_name,
    b.ne_id,
    b.rat_technology,
    b.home_subnet_id,
    b.software_version_id,

    -- Vendor
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

from fact f
join dim_date  d on f.measure_date_id = d.date_id
join dim_cell  c on f.cell_key        = c.cell_key
join dim_bs    b on c.bs_id           = b.bs_id
join dim_vendor v on b.vendor_id      = v.vendor_id
