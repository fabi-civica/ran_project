
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'bs_by_cell_config_bandwidth') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['bandwidth_id']) }} as bandwidth_id,
        bandwidth_id::NUMBER(3,0) as bandwidth_id_huawei,
        bandwidth_in_mhz,
        description as bandwidth_description,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded

    from source

)

select * from renamed
