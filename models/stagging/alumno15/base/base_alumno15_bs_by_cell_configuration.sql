
{{ config(materialized='view') }}

with source as (

    select * from {{ source('alumno15', 'raw_bs_by_cell_config') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['node_name']) }} as bs_id,
        local_cell_id::number(8,0),
        cell_name::varchar(25),
        local_cell_id::number(8,0),
        phycellid::number(8,0),
        rootsequenceidx::number(8,0),
        freqband::number(5,0) as freqband_id,
        dlearfcn::number(8,0),
        ulbandwidth::number(2,0) as bandwidth_id,
        cellactivestate::boolean as cell_active_by_config,
        txrxmode::number(2,0) as tx_rx_mode_id,
        nbcellflag, -- cell_topology_type in base_alumno15_bs_by_cell_status
        tac::number(10,0),
        sectoreqmid as sector_eqm_id,
        convert_timezone('Europe/Madrid', TO_TIMESTAMP(datetime_loaded, 'YYYY-MM-DD HH24:MI:SS')) as datetime_row_loaded 

    from source

)

select * from renamed
