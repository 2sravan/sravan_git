{{ config(materialized='table', tags=['idea','silver']) }}

with base as (

    select
        school_id,
        upper(trim(school_name)) as school_name,
        state,
        region,
        upper(trim(school_type)) as school_type,
        ingestion_timestamp
    from {{ ref('bronze_schools') }}

    where school_id is not null

),

dedup as (

    select
        *,
        row_number() over (
            partition by school_id
            order by ingestion_timestamp desc
        ) as rn
    from base

)

select
    school_id,
    school_name,
    state,
    region,
    school_type,
    ingestion_timestamp

from dedup
where rn = 1