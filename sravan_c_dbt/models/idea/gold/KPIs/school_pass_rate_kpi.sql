{{ config(
    materialized='table',
    tags=['idea','kpi','gold']
) }}

select
    s.school_id,
    s.school_name,
    count(*) as total_tests,
    sum(case when a.result_status = 'PASS' then 1 else 0 end) as passed_tests,
    round(
        sum(case when a.result_status = 'PASS' then 1 else 0 end) * 100.0 / count(*),
        2
    ) as pass_rate_pct
from {{ ref('fact_assessments') }} a
join {{ ref('dim_schools') }} s
    on a.school_id = s.school_id
group by s.school_id, s.school_name