
{{ config(
    materialized = 'table'
) }}

with cell as (

    select * from {{ ref('stg_alumno15__cell') }}

),

bandwidth as (
    select * from {{ ref('stg_alumno15__cell_config_bandwidthd') }}
),

tx_mode as (
    select * from {{ ref('stg_alumno15__cell_config_tx_rx_mode') }}
),

joined as (

    select
        c.cell_id,
        c.cell_name,
        c.bs_id,
        c.local_cell_id,
        c.freqband_id,
        c.dlearfcn,
        c.bandwidth_id,
        b.bandwidth_in_mhz,
        b.bandwidth_description,    
        c.tx_rx_mode_id,
        t.tx_rx_mode_id_huawei,
        t.num_tx_ports,
        t.num_rx_ports,
        t.tx_rx_mode_description,
        c.tac,
        c.cell_active_by_config,
        c.cell_status,
        c.cell_topology_type,
        c.maximum_transmit_power_0_1dbm,
        c.cell_plmn_info,
        c.phycellid,
        c.rootsequenceidx,
        c.sector_eqm_id,
        c.reason_for_latest_state_change,
        c.cell_latest_setup_time,
        c.cell_latest_setup_operate_type,
        c.cell_latest_remove_time,
        c.cell_latest_remove_operate_type,
        c.primary_bbp_information,
        c.datetime_row_loaded

    from cell c
    left join bandwidth b   on c.bandwidth_id   = b.bandwidth_id
    left join tx_mode t     on c.tx_rx_mode_id  = t.tx_rx_mode_id

)

select
    *
from joined
