{{ config(
    materialized='table',
    tags=['idea','kpi','gold']
) }}

select
    subject,
    round(avg(score), 2) as avg_score
from {{ ref('fact_assessments') }}
group by subject
order by avg_score desc