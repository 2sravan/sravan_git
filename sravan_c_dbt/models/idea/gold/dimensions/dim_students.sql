{{ config(materialized='table', tags=['idea','gold','dim']) }}

select
    student_id,
    first_name,
    last_name,
    gender,
    ethnicity,
    grade_level,
    home_language,
    dbt_valid_from as effective_from,
    dbt_valid_to as effective_to

from {{ ref('snap_students') }}
where dbt_valid_to is null