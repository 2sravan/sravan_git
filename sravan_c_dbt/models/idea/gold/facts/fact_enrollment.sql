{{ config(
    materialized='incremental',
    unique_key=['student_id', 'enrollment_date'],
    incremental_strategy='merge',
    partition_by=['enrollment_date'],
    tags=['idea','gold','fact']
) }}

select
    student_id,
    school_id,
    enrollment_date,
    is_active,
    enrollment_status

from {{ ref('stg_enrollment') }}