
{{ config(materialized='view') }}

with source as (

    select distinct cleared_by
    from {{ source('alumno15', 'raw_alarm_logs') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['cleared_by']) }} as cleared_by_id,
        cleared_by,
        case
            when cleared_by = '		-' then 'not_cleared'
            when cleared_by = '< NE operator >' then 'system'
            else 'oss_user' end as cleared_by_type
    from source

)

select * from renamed
