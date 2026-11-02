-- Gold mart: tag demand per ISO week (Monday start), top 50 tags per week.

with tagged as (
    select
        cast(date_trunc('week', posted_date) as date) as posted_week,
        lower(trim(tag)) as tag
    from {{ ref('stg_job_postings') }},
        unnest(tags) as t(tag)
    where posted_date is not null
      and trim(tag) <> ''
),

counted as (
    select posted_week, tag, count(*) as postings
    from tagged
    group by 1, 2
)

select
    posted_week,
    tag,
    postings,
    rank() over (partition by posted_week order by postings desc) as demand_rank
from counted
qualify demand_rank <= 50
order by 1, 4, 2
