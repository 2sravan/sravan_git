{{ config(materialized='table', tags=['idea','intermediate']) }}

select
    a.student_id,
    s.first_name,
    s.last_name,
    sc.school_name,
    a.subject,
    a.score,
    a.result_status,
    a.assessment_date

from {{ ref('stg_assessments') }} a
left join {{ ref('stg_students') }} s
    on a.student_id = s.student_id
left join {{ ref('stg_schools') }} sc
    on a.school_id = sc.school_id