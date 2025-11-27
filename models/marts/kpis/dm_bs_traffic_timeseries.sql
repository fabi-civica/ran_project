
{{ config(
    materialized = 'view'
) }}

with fact as (

    select
        measure_date_id,
        measure_hour_id,
        cell_id,
        trafico_dl,
        dl_cell_thp_mbps,
        dl_user_thp_mbps,
        rb_utilizing_rate_dl_pct,
        erab_estab_succ_rate_pct,
        call_setup_succ_rate_pct
    from {{ ref('fact_cell_traffic') }}

),

dim_cell as (

    select
        c.cell_id as cell_id,
        c.bs_id
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

),

fact_with_bs as (

    select
        f.measure_date_id,
        f.measure_hour_id,
        c.bs_id,
        f.trafico_dl,
        f.dl_cell_thp_mbps,
        f.dl_user_thp_mbps,
        f.rb_utilizing_rate_dl_pct,
        f.erab_estab_succ_rate_pct,
        f.call_setup_succ_rate_pct
    from fact f
    join dim_cell c
      on f.cell_id = c.cell_id

),

agg_bs_hour as (

    select
        bs_id,
        measure_date_id,
        measure_hour_id,
        count(*)                      as samples_count,
        sum(trafico_dl)               as trafico_dl_sum,
        avg(trafico_dl)               as trafico_dl_avg,
        avg(dl_cell_thp_mbps)         as dl_cell_thp_mbps_avg,
        max(dl_cell_thp_mbps)         as dl_cell_thp_mbps_max,
        avg(dl_user_thp_mbps)         as dl_user_thp_mbps_avg,
        avg(rb_utilizing_rate_dl_pct)    as rb_utilizing_rate_dl_pct_avg,
        avg(erab_estab_succ_rate_pct)    as erab_estab_succ_rate_pct_avg,
        avg(call_setup_succ_rate_pct)    as call_setup_succ_rate_pct_avg

    from fact_with_bs
    group by
        bs_id,
        measure_date_id,
        measure_hour_id

)

select
    a.bs_id,
    a.measure_date_id,
    d.date_day               as measure_date,
    a.measure_hour_id,
    t.hour_24,
    t.hour_label_24,
    t.day_part,
    b.bs_name,
    b.ne_id,
    b.rat_technology,
    b.home_subnet_id,
    b.software_version_id,
    v.vendor_name,
    a.samples_count,
    a.trafico_dl_sum,
    a.trafico_dl_avg,
    a.dl_cell_thp_mbps_avg,
    a.dl_cell_thp_mbps_max,
    a.dl_user_thp_mbps_avg,
    a.rb_utilizing_rate_dl_pct_avg,
    a.erab_estab_succ_rate_pct_avg,
    a.call_setup_succ_rate_pct_avg

from agg_bs_hour a
join dim_bs       b on a.bs_id            = b.bs_id
join dim_vendor   v on b.vendor_id        = v.vendor_id
join dim_date     d on a.measure_date_id  = d.date_id
join dim_time_hour t on a.measure_hour_id = t.time_hour_id
