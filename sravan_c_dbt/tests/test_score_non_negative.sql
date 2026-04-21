select *
from {{ ref('fact_assessments') }}
where score < 0