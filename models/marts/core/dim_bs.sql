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
        b.rnc_by_rat,
        b.creation_time,
        b.first_connection_time,
        b.datetime_row_loaded as dim_created_at
        
    from bs b
    left join rat               r on b.rat_id               = r.rat_id
    left join home_subnet       h on b.home_subnet_id       = h.home_subnet_id
    left join software_version  s on b.software_version_id  = s.software_version_id
    left join vendor            v on b.vendor_id            = v.vendor_id

)

select *
from joined

/*
models:
  - name: dim_bs
    description: "Dimensión de estaciones base (BS). Una fila por BS, uniendo RAT, subred y versión SW."
    columns:
      - name: bs_id
        description: "Clave surrogate de la BS (derivada de ne_name en Silver)."
        tests:
          - not_null
          - unique

      - name: bs_name
        description: "Nombre de la estación base (ne_name)."
        tests:
          - not_null

      - name: ne_id
        description: "Identificador técnico del elemento de red."

      - name: rat_id
        description: "Clave surrogate de la combinación tipo de nodo + RAT."
      - name: ne_type
        description: "Tipo de nodo (eNodeB, NodeB, BTS, etc.)."
      - name: rat_technology
        description: "Tecnología de acceso radio (2G, 3G, 4G, etc.)."

      - name: home_subnet_id
        description: "Clave surrogate de la subred asociada a la BS."
      - name: home_subnet
        description: "Subred (CIDR / nombre lógico) de la BS."

      - name: software_version_id
        description: "Clave surrogate de la versión de software."
      - name: software_version
        description: "Versión de software instalada en el nodo."

      - name: vendor_id
        description: "Identificador del vendor de la BS."

      - name: om_ip_address
        description: "IP O&M del nodo."

      - name: is_connected
        description: "Flag que indica si el nodo está online."
        tests:
          - not_null

      - name: rnc_by_rat
        description: "RNC asociado (para RAT 3G), si aplica."

      - name: creation_time
        description: "Fecha/hora de creación del nodo en el OSS."
      - name: first_connection_time
        description: "Fecha/hora de primera conexión del nodo."

      - name: dim_created_at
        description: "Timestamp de carga en la dimensión BS."
        */