{{ config(
    materialized='table',
    tags=['idea','kpi','gold']
) }}

select
    count(distinct student_id) as active_students
from {{ ref('fact_enrollment') }}
where enrollment_status = "ACTIVE"