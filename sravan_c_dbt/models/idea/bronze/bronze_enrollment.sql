{{ config(materialized='table', tags=['idea','bronze']) }}

select
    cast(student_id as int) as student_id,
    cast(school_id as int) as school_id,
    state,
    cast(enrollment_date as date) as enrollment_date,
    cast(enrollment_end_date as date) as enrollment_end_date,
    enrollment_flag,
    current_timestamp() as ingestion_timestamp
from {{ source('idea_raw', 'enrollment') }}