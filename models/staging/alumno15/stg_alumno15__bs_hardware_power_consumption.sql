{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_brd_consumo') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['board_name']) }} as board_id,
        {{ dbt_utils.generate_surrogate_key(['vendor']) }} as vendor_id,
        board_name::varchar(15) as board_name,
        consumo_minimo_w::number(5,0) as consumo_minimo_w,
        consumo_medio_w::number(5,0) as consumo_medio_w,
        consumo_tipico_w::number(5,0)  as consumo_tipico_w,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded

    from source

)

select * from renamed
