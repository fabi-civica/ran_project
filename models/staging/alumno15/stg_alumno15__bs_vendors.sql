{{ config(materialized='view') }}

with source_raw_vendors as (

    select * from {{ source('alumno15', 'raw_vendors') }}

),

renamed_bs_vendors as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendor_name']) }} as vendor_id,
        *
    from source_raw_vendors

)

select * from renamed_bs_vendors

