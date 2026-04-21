{{ config(
    materialized='incremental',
    unique_key=['student_id', 'assessment_date', 'subject'],
    incremental_strategy='merge',
    partition_by=['assessment_date'],
    tags=['idea','gold','fact']
) }}

select
    student_id,
    school_id,
    assessment_date,
    subject,
    score,
    result_status

from {{ ref('stg_assessments') }}

{% if is_incremental() %}
where assessment_date > (select max(assessment_date) from {{ this }})
{% endif %}