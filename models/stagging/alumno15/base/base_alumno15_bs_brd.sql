
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_bs_brd') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        node_name::varchar(15) as ne_name, -- no se si debo quitar esta columna paraevitar deduplicados
        ne as ne_id, -- no se si debo quitar esta columna paraevitar deduplicados
        fan_count::NUMBER(1,0),
        upeu_count::NUMBER(1,0),
        umpt_count::NUMBER(1,0),
        ubbp_count::NUMBER(1,0),
        lbbp_count::NUMBER(1,0),
        mrru_count::NUMBER(2,0),
        lrru_count::NUMBER(2,0),
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(datetime_loaded, 'MM/DD/YYYY HH24:MI')) as datetime_row_loaded

    from source

)

select * from renamed
