---
title: Companies hiring
---

```sql companies
select company, postings_30d, postings_total, boards, last_posted_date
from jobs.company_hiring
order by postings_30d desc, postings_total desc, company
```

Companies by postings in the 30 days ending on the newest capture date.

<DataTable data={companies} search=true rows=25>
    <Column id=company title="Company" />
    <Column id=postings_30d title="Last 30 days" />
    <Column id=postings_total title="All postings" />
    <Column id=boards title="Boards" />
    <Column id=last_posted_date title="Last posted" fmt="yyyy-mm-dd" />
</DataTable>
