{{ config(materialized='table', tags=['idea','gold','dim']) }}

select
    school_id,
    school_name,
    state,
    region,
    school_type

from {{ ref('stg_schools') }}