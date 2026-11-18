-- Top 20 tags of the last complete week (weeks start on Monday; the week in progress is left out).
with week as (
    select cast(date_trunc('week', max(as_of_date)) as date) - 7 as posted_week
    from empire_gold.jobs.jobs_board_health
)
select s.posted_week, s.demand_rank, s.tag, s.postings
from empire_gold.jobs.jobs_skills_demand s
join week w on s.posted_week = w.posted_week
order by s.demand_rank, s.tag
limit 20
