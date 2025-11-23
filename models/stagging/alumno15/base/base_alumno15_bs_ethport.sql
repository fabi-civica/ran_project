
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_bs_ethport') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        node_name::varchar(15) as ne_name, -- no se si debo quitar esta columna paraevitar deduplicados
        ne as ne_id, -- no se si debo quitar esta columna paraevitar deduplicados
        cabinet_no::NUMBER(1,0),
        subrack_no::NUMBER(1,0),
        slot_no::NUMBER(1,0),
        port_no::NUMBER(1,0),
        port_attribute::varchar(10),
        port_status::varchar(5),
        maximum_transmission_unit_byte::NUMBER(4,0),
        local_negotiation_mode::varchar(25),
        local_speed::varchar(25),
        local_duplex::varchar(25),
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(datetime_loaded, 'MM/DD/YYYY HH24:MI')) as datetime_row_loaded

    from source

)

select * from renamed
