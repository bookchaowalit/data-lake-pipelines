-- Companies by postings in the 30 days ending on the newest capture date.
select
    company,
    postings_30d,
    postings_total,
    array_to_string(boards, ', ') as boards,
    last_posted_date
from empire_gold.jobs.jobs_company_hiring
order by postings_30d desc, postings_total desc, company_key
