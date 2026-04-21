{{ config(materialized='table', tags=['idea','silver']) }}

with base as (

    select
        student_id,
        school_id,
        state,
        enrollment_date,
        enrollment_end_date,
        enrollment_flag,
        ingestion_timestamp
    from {{ ref('bronze_enrollment') }}

    where student_id is not null
      and enrollment_date is not null

),

dedup as (

    select
        *,
        row_number() over (
            partition by student_id, school_id, enrollment_date, enrollment_end_date
            order by ingestion_timestamp desc
        ) as rn
    from base

)

select
    student_id,
    school_id,
    state,
    enrollment_date,
    enrollment_end_date,

    --  derive clean business meaning in Silver
    case
        when enrollment_flag = 1 then 'ACTIVE'
        else 'INACTIVE'
    end as enrollment_status,

    case
        when enrollment_flag = 1 then 1
        else 0
    end as is_active,

    ingestion_timestamp

from dedup
where rn = 1