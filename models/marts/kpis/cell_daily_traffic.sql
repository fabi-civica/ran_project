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
    from {{ ref('dim_date') }}

),

aggregated as (

    select
        f.cell_key,
        f.measure_date_id,
        d.date_day as measure_date,

        -- número de muestras ese día
        count(*)                                 as samples_count,

        -- agregaciones de tráfico
        sum(f.trafico_dl)                        as trafico_dl_sum,
        avg(f.trafico_dl)                        as trafico_dl_avg,

        -- agregaciones de KPIs
        avg(f.dl_cell_thp_mbps)                  as dl_cell_thp_mbps_avg,
        avg(f.dl_user_thp_mbps)                  as dl_user_thp_mbps_avg,
        avg(f.rb_utilizing_rate_dl_pct)          as rb_utilizing_rate_dl_pct_avg,
        avg(f.erab_estab_succ_rate_pct)          as erab_estab_succ_rate_pct_avg,
        avg(f.call_setup_succ_rate_pct)          as call_setup_succ_rate_pct_avg,

        max(f.dl_cell_thp_mbps)                  as dl_cell_thp_mbps_max,
        max(f.trafico_dl)                        as trafico_dl_max

    from fact f
    join dim_date d
        on f.measure_date_id = d.date_id
    group by
        f.cell_key,
        f.measure_date_id,
        d.date_day

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
    -- claves
    a.cell_key,
    a.measure_date_id,
    a.measure_date,

    -- dim celda
    c.cell_name,
    c.local_cell_id,
    c.tac,
    c.freqband_id,
    c.dlearfcn,
    c.bandwidth_id,
    c.cell_status,
    c.cell_topology_type,

    -- dim BS
    b.bs_name,
    b.ne_id,
    b.rat_technology,
    b.home_subnet_id,
    b.software_version_id,

    -- vendor
    v.vendor_name,

    -- métricas agregadas
    a.samples_count,
    a.trafico_dl_sum,
    a.trafico_dl_avg,
    a.dl_cell_thp_mbps_avg,
    a.dl_user_thp_mbps_avg,
    a.rb_utilizing_rate_dl_pct_avg,
    a.erab_estab_succ_rate_pct_avg,
    a.call_setup_succ_rate_pct_avg,
    a.dl_cell_thp_mbps_max,
    a.trafico_dl_max

from aggregated a
join dim_cell   c on a.cell_key = c.cell_key
join dim_bs     b on c.bs_id    = b.bs_id
join dim_vendor v on b.vendor_id = v.vendor_id
