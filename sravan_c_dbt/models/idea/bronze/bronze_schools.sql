{{ config(materialized='table', tags=['idea','bronze']) }}

select
    cast(school_id as int) as school_id,
    trim(school_name) as school_name,
    state,
    region,
    school_type,
    current_timestamp() as ingestion_timestamp
from {{ source('idea_raw', 'schools') }}