-- Jobs marts must be internally consistent.

select 'daily' as mart, board as key
from {{ ref('jobs_postings_daily') }}
where postings < 1 or companies > postings or with_salary > postings

union all

select 'board_health', board
from {{ ref('jobs_board_health') }}
where postings_7d > postings_total or companies > postings_total

union all

select 'company_hiring', company_key
from {{ ref('jobs_company_hiring') }}
where postings_30d > postings_total or len(boards) < 1
