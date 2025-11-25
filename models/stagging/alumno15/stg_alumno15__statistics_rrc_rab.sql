
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'stats_cell_rrc_rab') }}

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
        case when l_rrc_connreq_att = 'NIL' then Null ELSE l_rrc_connreq_att END AS l_rrc_connreq_att,
        case when l_rrc_connreq_succ = 'NIL' then Null ELSE l_rrc_connreq_succ END AS l_rrc_connreq_succ,
        case when l_rrc_setupfail_rej = 'NIL' then Null ELSE l_rrc_setupfail_rej END AS l_rrc_setupfail_rej,
        case when l_rrc_setupfail_resfail = 'NIL' then Null ELSE l_rrc_setupfail_resfail END AS l_rrc_setupfail_resfail,
        case when l_erab_failest_mme = 'NIL' then Null ELSE l_erab_failest_mme END AS l_erab_failest_mme,
        case when l_erab_abnormrel_other = 'NIL' then Null ELSE l_erab_abnormrel_other END AS l_erab_abnormrel_other,
        case when l_erab_abnormrel_radio = 'NIL' then Null ELSE l_erab_abnormrel_radio END AS l_erab_abnormrel_radio,
        case when l_erab_abnormrel_mme = 'NIL' then Null ELSE l_erab_abnormrel_mme END AS l_erab_abnormrel_mme,
        case when l_erab_abnormrel = 'NIL' then Null ELSE l_erab_abnormrel END AS l_erab_abnormrel,
        case when l_erab_rel_mme = 'NIL' then Null ELSE l_erab_rel_mme END AS l_erab_rel_mme,
        case when l_erab_normrel = 'NIL' then Null ELSE l_erab_normrel END AS l_erab_normrel,
        case when l_cell_avail_dur_s = 'NIL' then Null ELSE l_cell_avail_dur_s END AS l_cell_avail_dur_s

    from source

)

select * from renamed