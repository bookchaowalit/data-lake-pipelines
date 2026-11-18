-- Overview tiles, as jobs-insights shows them. Windows end on the newest capture date, not today.
with as_of as (select max(as_of_date) as as_of_date from empire_gold.jobs.jobs_board_health)
select
    a.as_of_date,
    (select coalesce(sum(postings_total), 0) from empire_gold.jobs.jobs_board_health) as postings,
    (select coalesce(sum(d.postings), 0) from empire_gold.jobs.jobs_postings_daily d
        where d.posted_date > a.as_of_date - 90 and d.posted_date <= a.as_of_date) as postings_90d,
    (select coalesce(sum(postings_7d), 0) from empire_gold.jobs.jobs_board_health) as postings_7d,
    (select count(*) from empire_gold.jobs.jobs_board_health) as boards,
    (select count(*) from empire_gold.jobs.jobs_company_hiring) as companies
from as_of a
