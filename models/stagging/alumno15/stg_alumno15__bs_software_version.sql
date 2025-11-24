{{ config(materialized='view') }}

with source_raw_network_elements_info as (

    select  distinct version
    from {{ ref('base_alumno15_bs_element_info') }}

),

renamed_bs_software_version as (

    select
        {{ dbt_utils.generate_surrogate_key(['version']) }} as soft_ver_id,
        version as soft_ver,
        'https://docs.google.com/software/' || REPLACE(version, ' ', '_') || '/edit?usp=drive_link' as soft_url,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded
    from source_raw_network_elements_info

)

select * from renamed_bs_software_version
