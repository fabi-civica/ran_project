
{{ config(
    materialized='view'

) }}

with source as (

    select * from {{ ref("stg_alumno15__alarm_log") }}

),

renamed as (

    select
        alarm_id,
        --ne_id
        


    from source

)

select * from renamed
