-- Postings per day and board, the 90 days ending on the newest capture date.
with as_of as (select max(as_of_date) as as_of_date from empire_gold.jobs.jobs_board_health)
select d.posted_date, d.board, d.postings
from empire_gold.jobs.jobs_postings_daily d, as_of a
where d.posted_date > a.as_of_date - 90 and d.posted_date <= a.as_of_date
order by d.posted_date, d.board
