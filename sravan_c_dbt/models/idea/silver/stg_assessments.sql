{{ config(materialized='table', tags=['idea','silver']) }}

with cleaned as (

    select
        student_id,
        school_id,
        upper(trim(assessment_name)) as assessment_name,
        upper(trim(subject)) as subject,
        cast(score as float) as score,
        assessment_date,
        ingestion_timestamp

    from {{ ref('bronze_assessments') }}

    where assessment_date is not null
      and student_id is not null

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by student_id, assessment_name, assessment_date
            order by ingestion_timestamp desc
        ) as rn
    from cleaned

)

select
    student_id,
    school_id,
    assessment_name,
    subject,
    score,
    assessment_date,

    case
        when score >= 70 then 'PASS'
        else 'FAIL'
    end as result_status,

    ingestion_timestamp

from deduplicated
where rn = 1