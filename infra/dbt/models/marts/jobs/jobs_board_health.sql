-- Gold mart: one row per board. "Recent" is measured against the newest
-- capture in the snapshot, not the wall clock, so a rebuild of the same
-- Silver run always yields the same rows.

with as_of as (
    select cast(max(seen_at) as date) as as_of_date
    from {{ ref('stg_job_postings') }}
)

select
    jobs.board,
    count(*) as postings_total,
    count(distinct jobs.company_key) as companies,
    max(jobs.posted_date) as last_posted_date,
    max(jobs.seen_at) as last_seen_at,
    count(*) filter (where jobs.posted_date >= as_of.as_of_date - interval 7 day) as postings_7d,
    any_value(as_of.as_of_date) as as_of_date
from {{ ref('stg_job_postings') }} as jobs
cross join as_of
group by 1
order by 1
