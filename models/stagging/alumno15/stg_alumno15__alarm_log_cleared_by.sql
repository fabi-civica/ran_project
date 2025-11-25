
{{ config(materialized='view') }}

with source as (

    select distinct cleared_by
    from {{ source('alumno15', 'raw_alarm_logs') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['cleared_by']) }} as cleared_by_id,
        cleared_by
    from source

)

select * from renamed
