-- unique_key = 'cell_id'     -- de cara a futuros incrementales / SCD2
{{ config(
    materialized = 'table'
) }}

with cell as (

    select
        *
    from {{ ref('stg_alumno15__cell') }}

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
        c.cell_active_by_config,
        c.tx_rx_mode_id,
        c.tac,
        c.cell_status,
        c.cell_topology_type,
        c.maximum_transmit_power_0_1dbm,
        c.cell_plmn_info,
        c.datetime_row_loaded

    from cell c

)

select
    *
from joined

