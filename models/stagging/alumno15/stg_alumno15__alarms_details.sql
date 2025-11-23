{{ config(materialized='view') }}

with source_raw_alarm_logs as (

    select * from {{ source('alumno15', 'raw_alarm_logs') }}

),

transform_alarm_log as (

    select 
        *
    from source_raw_alarm_logs
),

renamed_alarm_log as (

    select
        

    from transform_alarm_log

)

select * from renamed_alarm_log

/*
associate_type,
        severity,
        alarm_id,
        name,
        ne_type,
        alarm_source,
        mo_name,
        location_information,
        occurred_on_nt,
        cleared_on_nt,
        acknowledged_on_st,
        cleared_by,
        acknowledged_by,
        clearance_status,
        rru_name,
        rf_site_name,
        cell_name,
        acknowledgement_status,
        bbu_name,
        enodeb_id,
        gnodeb_id,
        log_serial_number,
        user_label,
        equipment_alarm_serial_number,
        additional_information,
        maintenance_status,
        subnet,
        source_system,
        dev_root_csn,
        trouble_ticket_id,
        maintenance_region,
        type,
        comment,
        request_id,
        common_alarm_identifier,
        toggling_times,
        external_resource_id,
        link_name,
        link_type,
        auto_clear,
        clearance_type,
        object_type,
        operation_impact_flag,
        received_on_st,
        ne_address,
        alarm_duration,
        associated_alarm_group_id,
        ne_virtual_mark,
        occurrence_notify_time_nt,
        clearance_notify_time_nt
        */