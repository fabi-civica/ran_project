
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_bs_ethport') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        ne as ne_id,
        cabinet_no::NUMBER(1,0) as cabinet_no,
        subrack_no::NUMBER(1,0) as subrack_no,
        slot_no::NUMBER(1,0) as slot_no,
        port_no::NUMBER(1,0)as port_no,
        port_attribute::varchar(10) as port_attribute,
        port_status::varchar(5) as port_status,
        maximum_transmission_unit_byte::NUMBER(4,0) as mtu, 
        local_negotiation_mode::varchar(25) as local_negotiation_mode,
        local_speed::varchar(25) as local_speed,
        local_duplex::varchar(25) as local_duplex,
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(datetime_loaded, 'MM/DD/YYYY HH24:MI')) as datetime_row_loaded

    from source

)

select * from renamed
