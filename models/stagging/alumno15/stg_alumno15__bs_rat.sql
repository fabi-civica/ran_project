
{{ config(materialized='view') }}

with source_base_alumno15_bs_element_info as (

    select distinct
        ne_type,
        rat_tecnology
    from {{ ref('base_alumno15_bs_element_info') }}

),

renamed_bs_rat as (

    select
        {{ dbt_utils.generate_surrogate_key(['ne_type', 'rat_tecnology']) }} as rat_id,
        ne_type as bs_type,
        rat_tecnology,
        rat_tecnology like '%G%' as is_gsm,
        rat_tecnology like '%U%' as is_umts,
        rat_tecnology like '%L%' as is_lte,
        rat_tecnology like '%N%' as is_nr,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded
    from source_base_alumno15_bs_element_info
    order by ne_type, rat_tecnology

)

select * from renamed_bs_rat
