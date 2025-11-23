
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_bs_id') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        node_name::varchar(15) as ne_name, -- no se si debo quitar esta columna paraevitar deduplicados
        ne as ne_id, -- no se si debo quitar esta columna paraevitar deduplicados
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded

    from source

)

select * from renamed
