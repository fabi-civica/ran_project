
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_network_elements_info') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['ne_name']) }} as bs_id,
        ne_name::varchar(15) as bs_name,
        ne_type,
        regexp_replace(base_station_rat, '\\([^)]*\\)', '') as rat_tecnology,
        ip_address_1::varchar(25) as om_ip_address,
        version,
        'Huawei' as vendor_name,
        description,
        ne_connection_status,
        replace(base_station_id, '//', '') as base_station_id,
        replace(base_station_rnc, '//', '') as base_station_rnc,
        home_subnet::varchar(50) as home_subnet,
        ne_maintenance_mode,
        creation_time,
        first_connection_time,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded

    from source

)

select * from renamed