{{ config(materialized='table', tags=['idea','intermediate']) }}

select
    a.student_id,
    a.school_id,
    s.first_name,
    s.last_name,
    sc.school_name,
    a.attendance_date,
    a.status,
    a.attendance_flag

from {{ ref('stg_attendance') }} a
left join {{ ref('stg_students') }} s
    on a.student_id = s.student_id
left join {{ ref('stg_schools') }} sc
    on a.school_id = sc.school_id   