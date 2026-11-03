---
title: Boards
---

```sql boards
select
    board,
    postings_total,
    postings_7d,
    companies,
    last_posted_date,
    last_seen_at,
    case
        when last_posted_date is null then 'No posted date'
        when last_posted_date < as_of_date - interval 14 day then 'Quiet 14+ days'
        else 'Active'
    end as status
from jobs.board_health
order by postings_total desc
```

A board is **quiet** when its newest posting is more than 14 days older than the newest capture.
"Last seen" is when the scraper last captured a posting from that board (UTC).

<DataTable data={boards} rows=all>
    <Column id=board title="Board" />
    <Column id=status title="Status" />
    <Column id=postings_total title="Postings" />
    <Column id=postings_7d title="Last 7 days" />
    <Column id=companies title="Companies" />
    <Column id=last_posted_date title="Last posted" fmt="yyyy-mm-dd" />
    <Column id=last_seen_at title="Last seen (UTC)" fmt="yyyy-mm-dd H:MM" />
</DataTable>

<BarChart data={boards} x=board y=postings_total swapXY=true title="Postings by board" />
