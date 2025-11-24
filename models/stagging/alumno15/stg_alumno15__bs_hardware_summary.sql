
{{ config(materialized='view') }}

with source_raw_bs_brd as (

    select * from {{ source('alumno15', 'raw_bs_brd') }}

),

renamed_bs_hardware_summary as (

    select
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        fan_count::NUMBER(1,0) as fan_count,
        upeu_count::NUMBER(1,0) as upeu_count,
        umpt_count::NUMBER(1,0) as umpt_count,
        ubbp_count::NUMBER(1,0) as ubbp_count,
        lbbp_count::NUMBER(1,0) as lbbp_count,
        mrru_count::NUMBER(2,0) as mrru_count,
        lrru_count::NUMBER(2,0) as mrru_count,
        bbu3900_count::NUMBER(1,0) as bbu3900_count,
        bbu5900_count::NUMBER(1,0) as bbu5900_count,
        convert_timezone('Europe/Madrid', to_timestamp(datetime_loaded, 'MM/DD/YYYY HH24:MI')) as datetime_row_loaded

    from source_raw_bs_brd

)

select * from renamed_bs_hardware_summary
