
{{ config(materialized='table') }}

with raw_generated_data as (

    {{ dbt_date.get_date_dimension("2018-01-01", "2050-12-31") }}

)

select

    {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_id,
    *

from raw_generated_data