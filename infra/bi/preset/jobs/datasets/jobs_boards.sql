-- Job boards by postings, as the Boards page lists them.
select board, postings_total, postings_7d, companies, last_posted_date, last_seen_at
from empire_gold.jobs.jobs_board_health
order by postings_total desc, board
