{{ config(
    materialized='incremental',
    unique_key=['student_id', 'attendance_date'],
    incremental_strategy='merge',
    partition_by=['attendance_date'],
    tags=['idea','gold','fact']
) }}

select
    student_id,
    school_id,
    attendance_date,
    attendance_flag,
    1 as total_days

from {{ ref('stg_attendance') }}

{% if is_incremental() %}
where attendance_date > (select max(attendance_date) from {{ this }})
{% endif %}