
{{ config(materialized='view') }}

with source_base_alumno15_bs_element_info as (

    select distinct
        ne_type,
        rat_tecnology as rat_technology
    from {{ ref('base_alumno15_bs_element_info') }}

),

renamed_bs_rat as (

    select
        {{ dbt_utils.generate_surrogate_key(['ne_type', 'rat_technology']) }} as rat_id,
        ne_type as bs_type,
        rat_technology,
        rat_technology like '%G%' as is_gsm,
        rat_technology like '%U%' as is_umts,
        rat_technology like '%L%' as is_lte,
        rat_technology like '%N%' as is_nr,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded
    from source_base_alumno15_bs_element_info
    order by ne_type, rat_technology

)

select * from renamed_bs_rat
