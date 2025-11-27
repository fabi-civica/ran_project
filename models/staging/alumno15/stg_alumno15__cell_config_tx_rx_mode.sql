
{{ config(materialized='view') }}

with source as (

    select *,
    tx_rx_mode_id as tx_rx_mode_id_huawei
    
    from {{ source('alumno15', 'bs_by_cell_config_tx_rx_mode') }}

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['tx_rx_mode_id']) }} as tx_rx_mode_id,
        tx_rx_mode_id_huawei::NUMBER(2,0) as tx_rx_mode_id_huawei,
        num_tx_ports::NUMBER(2,0) as num_tx_ports,
        num_rx_ports::NUMBER(2,0) as num_rx_ports,
        description as tx_rx_mode_description,
        convert_timezone('Europe/Madrid', current_timestamp()) as datetime_row_loaded

    from source

)

select * from renamed
