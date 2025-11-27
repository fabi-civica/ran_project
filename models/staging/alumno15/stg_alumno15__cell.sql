
{{ config(materialized='view') }}

with source_base_alumno15_bs_by_cell_configuration as (

    select * from {{ ref('base_alumno15_bs_by_cell_configuration') }}

),

source_base_alumno15_bs_by_cell_status as (

    select * from {{ ref('base_alumno15_bs_by_cell_status') }}

),

renamed_bs_info as (

    select
        a.cell_id,
        a.bs_id,
        a.cell_name,
        a.local_cell_id,
        phycellid,
        rootsequenceidx,
        freqband_id,
        dlearfcn,
        {{ dbt_utils.generate_surrogate_key(['bandwidth_id']) }} as bandwidth_id,
        cell_active_by_config,
        {{ dbt_utils.generate_surrogate_key(['tx_rx_mode_id']) }} as tx_rx_mode_id,
        tac,
        sector_eqm_id,
        cell_status,
        reason_for_latest_state_change,
        cell_latest_setup_time,
        cell_latest_setup_operate_type ,
        cell_latest_remove_time,
        cell_latest_remove_operate_type,
        primary_bbp_information,
        cell_topology_type,
        maximum_transmit_power_0_1dbm,
        cell_plmn_info,
        a.datetime_row_loaded

    from source_base_alumno15_bs_by_cell_configuration as a
    left join source_base_alumno15_bs_by_cell_status as b
    on a.cell_id = b.cell_id

)

select * from renamed_bs_info
