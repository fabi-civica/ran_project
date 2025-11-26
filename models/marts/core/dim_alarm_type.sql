-- unique_key = 'alarm_id'     -- de cara a futuros incrementales / SCD2
{{ config(
    materialized = 'table'
) }}

with alarm_details as (

    select
        *
    from {{ ref('stg_alumno15__alarms_details') }}

),

dedup as (
    select
        *,
        row_number() over (
            partition by alarm_id
            order by datetime_row_loaded desc
        ) as rn
    from alarm_details
),

current_alarm_type as (

    select *
    from dedup
    where rn = 1

)

select
    alarm_id,
    alarm_name,
    alarm_specific_problem, --esta mal, de donde lo saaste?
    alarm_service_type,
    alarm_threshold_name,
    alarm_category,
    severity,
    vendor_name,
    alarm_description,
    datetime_row_loaded as dim_created_at
from current_alarm_type;

/*
version: 2

models:
  - name: dim_alarm_type
    description: "Dimensión de tipos de alarma. Una fila por tipo de alarma."
    columns:
      - name: alarm_id
        description: "Clave surrogate del tipo de alarma."
        tests:
          - not_null
          - unique

      - name: alarm_name
        description: "Nombre estándar de la alarma."
        tests:
          - not_null

      - name: alarm_specific_problem
        description: "Problema específico reportado por el sistema."

      - name: alarm_service_type
        description: "Tipo de servicio asociado de la alarma."

      - name: alarm_threshold_name
        description: "Nombre del umbral asociado a la alarma."

      - name: severity
        description: "Severidad de la alarma (Critical, Major, Minor)."
        tests:
          - not_null

      - name: alarm_category
        description: "Categoría definida para el tipo de alarma."

      - name: vendor_name
        description: "Vendor del equipo que genera esta alarma."

      - name: dim_created_at
        description: "Fecha de carga de la dimensión."
*/
