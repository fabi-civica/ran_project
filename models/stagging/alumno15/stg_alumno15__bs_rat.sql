{{ config(materialized='view') }}

with source_raw_network_elements_info as (

    select  distinct
        ne_type,
        base_station_rat,
    from {{ source('alumno15', 'raw_network_elements_info') }}

),

transform_bs_rat as (

    select distinct
        ne_type,
        regexp_replace(base_station_rat, '\\([^)]*\\)', '') as rat_tecnology
    from source_raw_network_elements_info
),

renamed_bs_rat as (

    select
        {{ dbt_utils.generate_surrogate_key(['ne_type', 'rat_tecnology']) }} as rat_id,
        ne_type,
        rat_tecnology,
        rat_tecnology like '%G%' as is_gsm,
        rat_tecnology like '%U%' as is_umts,
        rat_tecnology like '%L%' as is_lte,
        rat_tecnology like '%N%' as is_nr,
    from transform_bs_rat
    order by ne_type, rat_tecnology

)

select * from renamed_bs_rat

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
