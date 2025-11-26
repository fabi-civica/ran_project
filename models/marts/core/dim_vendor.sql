-- unique_key = 'vendor_id'     -- de cara a futuros incrementales / SCD2
{{ config(
    materialized = 'table'
) }}

with vendors as (

    select
        *
    from {{ ref('stg_alumno15__bs_vendors') }}

)

select

    vendor_id,
    vendor_name,
    support_tel,
    contact_name,
    contact_tel,
    datetime_row_loaded as dim_created_at

from vendors
