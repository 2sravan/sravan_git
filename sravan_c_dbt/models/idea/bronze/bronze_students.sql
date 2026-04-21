{{ config(materialized='table', tags=['idea','bronze']) }}

select
    cast(student_id as int) as student_id,
    trim(first_name) as first_name,
    trim(last_name) as last_name,
    gender,
    ethnicity,
    cast(date_of_birth as date) as date_of_birth,
    grade_level,
    home_language,

    current_timestamp() as ingestion_timestamp
from {{ source('idea_raw', 'students') }}