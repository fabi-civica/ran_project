
{{ config(materialized='view') }}

with source_raw_network_elements_info as (

    select  distinct home_subnet
    from {{ ref('base_alumno15_bs_element_info') }}

),

renamed_bs_home_subnet as (

    select
        {{ dbt_utils.generate_surrogate_key(['home_subnet']) }} as home_subnet_id,
        home_subnet,
        split_part(home_subnet, '/', 1) AS root_subnet,
        split_part(home_subnet, '/', 2) AS region_subnet,
        split_part(home_subnet, '/', 3) AS sub_region_subnet,
        split_part(home_subnet, '/', 4) AS integrated_subnet,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded
    from source_raw_network_elements_info

)

select * from renamed_bs_home_subnet
