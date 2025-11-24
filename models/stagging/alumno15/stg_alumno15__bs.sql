
{{ config(materialized='view') }}

with source_base_alumno15_bs_element_info as (

    select * from {{ ref('base_alumno15_bs_element_info') }}

),

source_base_alumno15_bs_ethport as (

    select distinct
        bs_id,
        ne_id
    from {{ ref('base_alumno15_bs_ethport') }}

),

renamed_bs_info as (

    select
        a.bs_id,
        ne_id,
        {{ dbt_utils.generate_surrogate_key(['ne_type', 'rat_tecnology']) }} as rat_id,
        base_station_id as bs_id_by_rat,
        om_ip_address,
        {{ dbt_utils.generate_surrogate_key(['version']) }} as software_version_id,
        case when ne_connection_status = 'Online' then TRUE ELSE FALSE END AS is_connected,
        {{ dbt_utils.generate_surrogate_key(['vendor_name']) }} as vendor_id,
        coalesce(case when base_station_rnc = '' then 'None' ELSE base_station_rnc END,
           'None')  AS rnc_by_rat,
        {{ dbt_utils.generate_surrogate_key(['home_subnet']) }} as home_subnet_id,
        case when ne_maintenance_mode = 'NORMAL' then TRUE ELSE FALSE END AS is_enabled,
        convert_timezone('Europe/Madrid',  coalesce(
            try_to_timestamp_ntz(regexp_replace(creation_time, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz(creation_time, 'MM/DD/YYYY HH24:MI'),
            to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI'))) as creation_time,
        convert_timezone('Europe/Madrid',  coalesce(
            try_to_timestamp_ntz(regexp_replace(first_connection_time, ' DST$', ''), 'YYYY-MM-DD HH24:MI:SS'),
            try_to_timestamp_ntz(first_connection_time, 'MM/DD/YYYY HH24:MI'),
            to_timestamp_ntz('2000-01-01 00:01', 'YYYY-MM-DD HH24:MI'))) as first_connection_time,
        datetime_row_loaded

    from source_base_alumno15_bs_element_info as a
    left join source_base_alumno15_bs_ethport as b
    on a.bs_id = b.bs_id

)

select * from renamed_bs_info
