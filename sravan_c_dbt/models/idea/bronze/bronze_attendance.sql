{{ config(materialized='table', tags=['idea','bronze']) }}

select
    cast(student_id as int) as student_id,
    cast(school_id as int) as school_id,
    cast(date as date) as attendance_date,
    upper(status) as status,
       current_timestamp() as ingestion_timestamp
from {{ source('idea_raw', 'attendance') }}