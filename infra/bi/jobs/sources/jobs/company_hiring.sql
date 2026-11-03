-- boards is a list in Gold; the site only shows it, so flatten it to text.
select company_key, company, postings_total, postings_30d,
    array_to_string(boards, ', ') as boards, first_posted_date, last_posted_date
from jobs_company_hiring
