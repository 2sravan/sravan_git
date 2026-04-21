select
    d.year,
    d.month,
    s.school_name,
    round(sum(f.attendance_flag) * 100.0 / count(*), 2) as attendance_rate
from {{ ref('fact_attendance') }} f
join {{ ref('dim_schools') }} s
    on f.school_id = s.school_id
join {{ ref('dim_date') }} d
    on f.attendance_date = d.date
group by d.year, d.month, s.school_name
order by d.year, d.month