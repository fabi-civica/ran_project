{{ config(materialized='view') }}

with source_raw_network_elements_info as (

    select  distinct
        version,
    from {{ source('alumno15', 'raw_network_elements_info') }}

),

renamed_bs_software_version as (

    select
        {{ dbt_utils.generate_surrogate_key(['version']) }} as soft_ver_id,
        version as soft_ver,
        'https://docs.google.com/software/' || REPLACE(version, ' ', '_') || '/edit?usp=drive_link' as soft_url
    from source_raw_network_elements_info

)

select * from renamed_bs_software_version

/*

        ne_type,
        ip_address_1,
        ip_address_2,
        version,
        med_partition,
        subarea_ip,
        timezone,
        physical_location,
        vendor,
        description,
        district,
        longitude,
        latitude,
        capacity,
        maintenance_region,
        ne_connection_status,
        base_station_rat,
        base_station_id,
        base_station_rnc,
        home_subnet,
        product_type,
        ne_maintenance_mode,
        creation_time,
        first_connection_time,
        password_of_om_connection_administrator,
        recently_modified_on,
        sctp_link_status
*/
