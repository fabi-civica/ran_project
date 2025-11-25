
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_bs_by_cell_config') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['cell_name']) }} as cell_id,
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        cell_name::varchar(25) as cell_name,
        local_cell_id::number(8,0) as local_cell_id,
        phycellid::number(8,0) as phycellid,
        rootsequenceidx::number(8,0) as rootsequenceidx,
        freqband::number(5,0) as freqband_id,
        dlearfcn::number(8,0) as dlearfcn,
        ulbandwidth::number(2,0) as bandwidth_id,
        cellactivestate::boolean as cell_active_by_config,
        txrxmode::number(2,0) as tx_rx_mode_id,
        nbcellflag, -- cell_topology_type in base_alumno15_bs_by_cell_status
        tac::number(10,0) as tac,
        sectoreqmid as sector_eqm_id,
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(datetime_loaded, 'MM/DD/YYYY HH24:MI')) as datetime_row_loaded

    from source

)

select * from renamed
