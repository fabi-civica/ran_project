-- unique_key = 'bs_key'     -- de cara a futuros incrementales / SCD2
{{ config(
    materialized = 'table'
) }}

with bs as (

    select
        *
    from {{ ref('stg_alumno15__bs') }}

),

rat as (

    select
        *
    from {{ ref('stg_alumno15__bs_rat') }}

),

home_subnet as (

    select
        *
    from {{ ref('stg_alumno15__bs_home_subnet') }}

),

software_version as (

    select
        *
    from {{ ref('stg_alumno15__bs_software_version') }}

),

vendor as (

    select
        *
    from {{ ref('stg_alumno15__bs_vendors') }}

),

joined as (

    select
        b.bs_id,
        b.bs_name,
        b.ne_id,
        r.rat_id,
        r.bs_type,
        r.rat_technology,
        h.home_subnet_id,
        h.home_subnet,
        s.software_version_id,
        s.software_version,
        v.vendor_id,
        v.vendor_name,
        b.bs_id_by_rat,
        b.om_ip_address,
        b.is_connected,
        b.is_enabled,
        b.port_profile,
        b.rnc_by_rat,
        b.creation_time,
        b.first_connection_time,
        r.is_gsm,
        r.is_umts,
        r.is_lte,
        r.is_nr,
        b.datetime_row_loaded as dim_created_at
        
    from bs b
    left join rat               r on b.rat_id               = r.rat_id
    left join home_subnet       h on b.home_subnet_id       = h.home_subnet_id
    left join software_version  s on b.software_version_id  = s.software_version_id
    left join vendor            v on b.vendor_id            = v.vendor_id

)

select *
from joined
