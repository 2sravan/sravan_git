{{ config(
    materialized='table',
    tags=['idea','kpi','gold']
) }}

select
    student_id,
    sum(attendance_flag) as days_present,
    count(*) as total_days,
    round(sum(attendance_flag) * 100.0 / count(*), 2) as attendance_rate_pct
from {{ ref('fact_attendance') }}
group by student_id