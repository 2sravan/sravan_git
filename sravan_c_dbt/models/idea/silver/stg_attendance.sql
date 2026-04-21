{{ config(materialized='table', tags=['idea','silver']) }}

with base as (

    select
        student_id,
        school_id,
        attendance_date,
        upper(trim(status)) as status,
        ingestion_timestamp
    from {{ ref('bronze_attendance') }}
    where attendance_date is not null
      and student_id is not null

),

dedup as (

    select
        *,
        row_number() over (
            partition by student_id, attendance_date
            order by ingestion_timestamp desc
        ) as rn
    from base

)

select
    student_id,
    school_id,
    attendance_date,

    case
        when status in ('PRESENT','ABSENT','LATE') then status
        else 'UNKNOWN'
    end as status,

     case
        when status = 'PRESENT' then 1
        else 0
    end as attendance_flag,

    case
        when status = 'PRESENT' then 'ATTENDED'
        else 'MISSED'
    end as attendance_category,

    ingestion_timestamp

from dedup
where rn = 1