
{{ config(materialized='view') }}

with source_raw_vendors as (

    select * from {{ source('alumno15', 'raw_vendors') }}

),

renamed_bs_vendors as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendor_name']) }} as vendor_id,
        vendor_name,
        tel_soporte::NUMBER(15,0) as support_phone,
        contacto as contact_name,
        tel_contacto ::NUMBER(15,0) as contact_phone,
        correo_contacto as contact_email,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded
    from source_raw_vendors

)

select * from renamed_bs_vendors
