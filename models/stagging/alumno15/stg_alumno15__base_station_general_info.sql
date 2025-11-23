{{ config(materialized='view') }}

with source_raw_network_elements_info as (

    select * from {{ source('alumno15', 'raw_network_elements_info') }}

),

transform_bs_info as (

    select 
        *,
        regexp_replace(base_station_rat, '\\([^)]*\\)', '') as rat_tecnology,
        'Huawei' as vendor_name,
        replace(base_station_rnc, '//', '') as base_station_rnc_mod
    from source_raw_network_elements_info
),

renamed_base_station_general_info as (

    select
        {{ dbt_utils.generate_surrogate_key(['ne_name']) }} as bs_id,
        ne_name::varchar(15),
        {{ dbt_utils.generate_surrogate_key(['ne_type', 'rat_tecnology']) }} as rat_id,
        ip_address_1::varchar(25) as om_ip_address,
        {{ dbt_utils.generate_surrogate_key(['version']) }} as soft_ver_id,
        longitude,
        latitude,
        CASE WHEN ne_connection_status = 'Online' THEN TRUE ELSE FALSE END AS is_connected,
        replace(base_station_id, '//', '') as base_station_id,
        COALESCE(CASE WHEN base_station_rnc_mod = '' THEN 'None' ELSE base_station_rnc_mod END,
           'None')  AS base_station_rnc,
        home_subnet::varchar(50),
        {{ dbt_utils.generate_surrogate_key(['vendor_name']) }} as vendor_id,
        creation_time,
        first_connection_time
    from transform_bs_info

)

select * from renamed_base_station_general_info

/*
        product_type

        CASE
        WHEN fecha_col ILIKE '%DST' THEN 
            -- Fecha con indicación DST → GMT+02
            CONVERT_TIMEZONE('GMT+02', 'UTC',
                TO_TIMESTAMP_NTZ(REPLACE(fecha_col, 'DST', ''))
            )
        ELSE
            -- Fecha sin DST → GMT+01
            CONVERT_TIMEZONE('GMT+01', 'UTC',
                TO_TIMESTAMP_NTZ(fecha_col)
            )
    END AS fecha_utc
*/
