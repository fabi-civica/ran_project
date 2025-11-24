
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_bs_by_cell_status') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        local_cell_id::NUMBER(9,0),
        cell_name::varchar(25),
        cell_instance_state as cell_status,
        reason_for_latest_state_change,
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(cell_latest_setup_time, 'YYYY-MM-DD HH24:MI:SS')) as cell_latest_setup_time,
        Cell_latest_setup_operate_type,
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(Cell_latest_remove_time, 'YYYY-MM-DD HH24:MI:SS')) as Cell_latest_remove_time,
        Cell_latest_remove_operate_type,
        primary_bbp_information::varchar(5),
        cell_topology_type::varchar(15),
        maximum_transmit_power_0_1dbm::NUMBER(3,0),
        cell_plmn_info::varchar(10),
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(datetime_loaded, 'YYYY-MM-DD HH24:MI:SS')) as datetime_row_loaded

    from source

)

select * from renamed
