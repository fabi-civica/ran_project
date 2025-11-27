
{{ config(
    materialized = 'view'
) }}

with last_measure_per_cell as (

    select
        cell_id,
        max(measure_time) as last_measure_time
    from {{ ref('fact_cell_traffic') }}
    group by cell_id

),

fact_last as (

    select
        f.*
    from {{ ref('fact_cell_traffic') }} f
    join last_measure_per_cell lm
      on f.cell_id     = lm.cell_id
     and f.measure_time = lm.last_measure_time

),

dim_cell as (

    select
        c.cell_id,
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

),

dim_date as (

    select
        date_id,
        date_day
    from {{ ref('dim_date') }}

),

dim_time_hour as (

    select
        time_hour_id,
        hour_24,
        hour_label_24,
        day_part
    from {{ ref('dim_time_hour') }}

)

select
    f.cell_id,
    c.cell_name,
    c.local_cell_id,
    b.bs_id,
    b.bs_name,
    v.vendor_name,
    f.measure_time,
    f.measure_date_id,
    d.date_day       as measure_date,
    f.measure_hour_id,
    t.hour_24,
    t.hour_label_24,
    t.day_part,
    f.measure_period,
    c.tac,
    c.freqband_id,
    c.dlearfcn,
    c.bandwidth_id,
    c.cell_status,
    c.cell_topology_type,
    b.ne_id,
    b.rat_technology,
    b.home_subnet_id,
    b.software_version_id,
    f.rb_utilizing_rate_dl_pct,
    f.erab_estab_succ_rate_pct,
    f.call_setup_succ_rate_pct,
    f.cell_dl_avg_thp_kbps,
    f.cell_ul_avg_thp_kbps,
    f.dl_cell_thp_mbps,
    f.dl_user_thp_mbps,
    f.trafico_dl

from fact_last f
join dim_cell    c on f.cell_id        = c.cell_id
join dim_bs      b on c.bs_id           = b.bs_id
join dim_vendor  v on b.vendor_id       = v.vendor_id
left join dim_date      d on f.measure_date_id  = d.date_id
left join dim_time_hour t on f.measure_hour_id  = t.time_hour_id
