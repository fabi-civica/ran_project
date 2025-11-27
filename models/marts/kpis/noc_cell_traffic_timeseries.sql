
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
        *
    from {{ ref('dim_date') }}

),

dim_time_hour as (

    select
        time_hour_id,
        hour_24,
        hour_label_24,
        day_part
    from {{ ref('dim_time_hour') }}

),

dim_cell as (

    select
        *
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
    f.measure_date_id,
    d.date_day as measure_date,
    f.measure_hour_id,
    t.hour_24,
    t.hour_label_24,
    t.day_part,
    f.measure_time,
    f.measure_period,
    f.cell_id,
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
    f.rb_utilizing_rate_dl_pct,
    f.erab_estab_succ_rate_pct,
    f.call_setup_succ_rate_pct,
    f.cell_dl_avg_thp_kbps,
    f.cell_ul_avg_thp_kbps,
    f.dl_cell_thp_mbps,
    f.dl_user_thp_mbps,
    f.trafico_dl

from fact f
join dim_date      d on f.measure_date_id = d.date_id
join dim_time_hour t on f.measure_hour_id = t.time_hour_id
join dim_cell      c on f.cell_id        = c.cell_id
join dim_bs        b on c.bs_id           = b.bs_id
join dim_vendor    v on b.vendor_id       = v.vendor_id
