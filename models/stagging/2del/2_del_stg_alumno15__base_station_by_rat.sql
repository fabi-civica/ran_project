{{ config(materialized='view') }}

with source_raw_network_elements_info as (

    select  * from {{ source('alumno15', 'raw_network_elements_info') }} limit 2

),

source_stg_alumno15__bs_rat as (

    select * from {{ ref('stg_alumno15__bs_rat') }}

),

transform_base_station_generaby_rat as (

    select *,
        replace(base_station_id, '//', '') as base_station_id_clean
    from source_raw_network_elements_info
),

renamed_base_station_generaby_rat as (
/*
    select
        ne_name::varchar(15),
        rat_tecnology
    

    from source_raw_network_elements_info as e
    join source_stg_alumno15__bs_rat as r 
      on r.ne_id = s.ne_id
*/
    select
        *
    from transform_base_station_generaby_rat sd
    JOIN LATERAL 
        SPLIT_TO_TABLE(sd.base_station_id_clean, '/') AS s ON TRUE

)

select * from renamed_base_station_generaby_rat






/*

exploded_rat as ()


-- 2. Explotar Base Station ID: '93/2506/957732/2957732' → filas con posición
id_segments as (
    select
        n.ne_id,
        f.seq as id_pos,
        f.value::string as segment_id
    from ne n,
    lateral flatten(input => split(n.base_station_id_clean, '/')) f
),

-- 3. Juntar por posición RAT ↔ segmento
joined as (
    select
        r.ne_id,
        r.rat_code,
        s.segment_id as ran_node_id
    from exploded_rat r
    join id_segments s
      on r.ne_id = s.ne_id
     and r.rat_pos = s.id_pos
)

-- 4. Añadir info del diccionario sil_rat (opcional pero recomendable)
select
    j.ne_id,
    j.rat_code,
    sr.rat_name,
    sr.rat_generation,
    j.ran_node_id
from joined j
left join sil_rat sr
  on j.rat_code = sr.rat_code
order by ne_id, rat_code;
*/
