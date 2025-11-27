
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_bs_by_cell_status') }}
    where {{ same_vendor_cell('node_name', 'cell_name') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['cell_name']) }} as cell_id,
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        local_cell_id::NUMBER(9,0) as local_cell_id,
        cell_name::varchar(25) as cell_name,
        cell_instance_state as cell_status,
        reason_for_latest_state_change,
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(cell_latest_setup_time, 'YYYY-MM-DD HH24:MI:SS')) as cell_latest_setup_time,
        Cell_latest_setup_operate_type as cell_latest_setup_operate_type,
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(Cell_latest_remove_time, 'YYYY-MM-DD HH24:MI:SS')) as cell_latest_remove_time,
        Cell_latest_remove_operate_type as cell_latest_remove_operate_type,
        primary_bbp_information::varchar(5) as primary_bbp_information,
        cell_topology_type::varchar(15) as cell_topology_type,
        maximum_transmit_power_0_1dbm::NUMBER(3,0) as maximum_transmit_power_0_1dbm,
        cell_plmn_info::varchar(10) as cell_plmn_info,
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(datetime_loaded, 'YYYY-MM-DD HH24:MI:SS')) as datetime_row_loaded

    from source

)

select * from renamed
