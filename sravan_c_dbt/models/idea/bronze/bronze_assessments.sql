{{ config(materialized='table', tags=['idea','bronze']) }}

select
    cast(student_id as int) as student_id,
    cast(school_id as int) as school_id,
    upper(assessment_name) as assessment_name,
    trim(subject) as subject,
    cast(score as float) as score,
    --upper(assessment_type) as assessment_type,
    cast(assessment_date as date) as assessment_date,
    current_timestamp() as ingestion_timestamp
from {{ source('idea_raw', 'assessments') }}