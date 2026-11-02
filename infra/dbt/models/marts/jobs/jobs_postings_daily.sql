-- Gold mart: postings per posting date and board.

select
    posted_date,
    board,
    count(*) as postings,
    count(distinct company_key) as companies,
    count(salary_text) as with_salary
from {{ ref('stg_job_postings') }}
where posted_date is not null
group by 1, 2
order by 1, 2
