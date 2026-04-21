{{ config(materialized='table', tags=['idea','intermediate']) }}

select
    s.student_id,
    s.first_name,
    s.last_name,
    s.grade_level,
    e.school_id,
    e.enrollment_status,
    e.is_active,
    e.enrollment_date

from {{ ref('stg_students') }} s
left join {{ ref('stg_enrollment') }} e
    on s.student_id = e.student_id