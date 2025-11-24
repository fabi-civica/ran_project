
{{ config(materialized='view') }}

with source_base_alumno15_bs_ethport as (

    select * from {{ ref('base_alumno15_bs_ethport') }}

),

renamed_bs_ethport_details as (

    select
        bs_id,
        listagg(distinct cabinet_no, '/') within group (order by cabinet_no) as cabinet_no,
        listagg(distinct subrack_no, '/') within group (order by subrack_no) as subrack_no,
        listagg(distinct slot_no, '/') within group (order by slot_no) as slot_no,
        listagg(distinct port_no, '/') within group (order by port_no) as port_no,
        listagg(distinct port_attribute, '/') within group (order by port_attribute)   as port_attribute,
        listagg(distinct mtu, '/') within group (order by mtu) as mtu,
        listagg(distinct local_negotiation_mode, '/') within group (order by local_negotiation_mode) as local_negotiation_mode,
        listagg(distinct local_speed, '/') as local_speed,
        listagg(distinct local_duplex, '/') within group (order by local_duplex) as local_duplex,
        listagg(distinct datetime_row_loaded, '/') within group (order by datetime_row_loaded) as datetime_row_loaded

    from source_base_alumno15_bs_ethport
    where port_status = 'Up'
    group by bs_id

)

select * from renamed_bs_ethport_details
