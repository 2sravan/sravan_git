{{ config(materialized='table', tags=['idea','silver']) }}

with deduplicated as (

    select
        student_id,
        first_name,
        last_name,
        gender,
        ethnicity,
        date_of_birth,
        grade_level,
        home_language,
        ingestion_timestamp,

        row_number() over (
            partition by student_id
            order by ingestion_timestamp desc
        ) as rn

    from {{ ref('bronze_students') }}

)

select
    student_id,
    upper(first_name) as first_name,
    upper(last_name) as last_name,
    gender,
    ethnicity,
    date_of_birth,
    grade_level,
    upper(home_language) as home_language,
    ingestion_timestamp
from deduplicated
where rn = 1