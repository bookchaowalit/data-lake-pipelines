-- Gold mart: one row per company (case-insensitive). The 30-day window is
-- anchored on the newest capture in the snapshot, like jobs_board_health.

with as_of as (
    select cast(max(seen_at) as date) as as_of_date
    from {{ ref('stg_job_postings') }}
)

select
    jobs.company_key,
    min(jobs.company) as company,
    count(*) as postings_total,
    count(*) filter (where jobs.posted_date >= as_of.as_of_date - interval 30 day) as postings_30d,
    list_sort(list_distinct(list(jobs.board))) as boards,
    min(jobs.posted_date) as first_posted_date,
    max(jobs.posted_date) as last_posted_date
from {{ ref('stg_job_postings') }} as jobs
cross join as_of
where jobs.company_key <> ''
group by 1
order by 1
