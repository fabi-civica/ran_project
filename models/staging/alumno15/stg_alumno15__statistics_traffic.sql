
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'stats_cell_traffic') }}

),

renamed as (

    select
        convert_timezone('Europe/Madrid', to_timestamp(start_time, 'MM/DD/YYYY HH24:MI')) as measure_time,
        period_min::number(3,0) as measure_period,
        TRIM(SPLIT_PART(
                SPLIT_PART(cell, 'Local Cell ID=', 2), 
                ',', 1)) AS local_cell_id,
        TRIM(SPLIT_PART(
                SPLIT_PART(cell, 'Cell Name=', 2), 
                ',', 1)) AS cell_name,
        case when rb_utilizing_rate_dl_pct = 'NIL' then Null ELSE rb_utilizing_rate_dl_pct END AS rb_utilizing_rate_dl_pct,
        case when erab_estab_succ_rate_pct = 'NIL' then Null ELSE erab_estab_succ_rate_pct END AS erab_estab_succ_rate_pct,
        case when call_setup_succ_rate_pct = 'NIL' then Null ELSE call_setup_succ_rate_pct END AS call_setup_succ_rate_pct,
        case when cell_dl_avg_thp_kbps = 'NIL' then Null ELSE cell_dl_avg_thp_kbps END AS cell_dl_avg_thp_kbps,
        case when cell_ul_avg_thp_kbps = 'NIL' then Null ELSE cell_ul_avg_thp_kbps END AS cell_ul_avg_thp_kbps,
        case when dl_cell_thp_mbps = 'NIL' then Null ELSE dl_cell_thp_mbps END AS dl_cell_thp_mbps,
        case when dl_user_thp_mbps = 'NIL' then Null ELSE dl_user_thp_mbps END AS dl_user_thp_mbps,
        case when trafico_dl = 'NIL' then Null ELSE trafico_dl END AS trafico_dl

    from source

)

select * from renamed
