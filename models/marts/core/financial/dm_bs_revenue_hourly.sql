
{{ config(
    materialized = 'view'
) }}

with bs_traffic as (

    select
        bs_id,
        measure_date_id,
        measure_date,
        measure_hour_id,
        hour_24,
        hour_label_24,
        day_part,

        bs_name,
        ne_id,
        rat_technology,
        home_subnet_id,
        software_version_id,
        vendor_name,

        samples_count,
        trafico_dl_sum,
        trafico_dl_avg,
        dl_cell_thp_mbps_avg,
        dl_cell_thp_mbps_max,
        dl_user_thp_mbps_avg,
        rb_utilizing_rate_dl_pct_avg,
        erab_estab_succ_rate_pct_avg,
        call_setup_succ_rate_pct_avg
    from {{ ref('dm_bs_traffic_timeseries') }}

),

params as (

    select
        {{ var('traffic_revenue_coef', 0.05) }}::number(18,6) as traffic_revenue_coef

),

final as (

    select
        t.bs_id,
        t.measure_date_id,
        t.measure_date,
        t.measure_hour_id,
        t.hour_24,
        t.hour_label_24,
        t.day_part,

        t.bs_name,
        t.ne_id,
        t.rat_technology,
        t.home_subnet_id,
        t.software_version_id,
        t.vendor_name,

        t.samples_count,
        t.trafico_dl_sum,
        t.trafico_dl_avg,
        t.dl_cell_thp_mbps_avg,
        t.dl_cell_thp_mbps_max,
        t.dl_user_thp_mbps_avg,
        t.rb_utilizing_rate_dl_pct_avg,
        t.erab_estab_succ_rate_pct_avg,
        t.call_setup_succ_rate_pct_avg,

        p.traffic_revenue_coef,

        (t.trafico_dl_sum * p.traffic_revenue_coef)::number(18,2) as revenue_traffic_hourly

    from bs_traffic t
    cross join params p

)

select *
from final
