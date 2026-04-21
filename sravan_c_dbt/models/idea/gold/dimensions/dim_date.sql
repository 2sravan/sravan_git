{{ config(materialized='table', tags=['idea','gold','dim']) }}

with dates as (

    select explode(
        sequence(
            date('2023-01-01'),
            date('2026-12-31'),
            interval 1 day
        )
    ) as date_day

)

select
    date_day as date,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    quarter(date_day) as quarter,
    weekofyear(date_day) as week

from dates