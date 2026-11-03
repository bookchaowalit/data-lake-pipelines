---
title: Skills demand
---

```sql weeks
-- The newest week is usually still in progress, so the views use the last
-- complete week (Monday start) before the newest capture date.
select
    cast(date_trunc('week', as_of_date) - interval 7 day as date) as last_week,
    cast(date_trunc('week', as_of_date) - interval 28 day as date) as first_of_4
from jobs.gold_meta
```

```sql last_week_tags
select tag, postings, demand_rank
from jobs.skills_demand
where posted_week = (select last_week from ${weeks})
order by demand_rank, tag
limit 20
```

```sql four_week_tags
select tag, sum(postings) as postings, count(*) as weeks_in_top_50
from jobs.skills_demand
where posted_week >= (select first_of_4 from ${weeks})
  and posted_week <= (select last_week from ${weeks})
group by tag
order by postings desc, tag
limit 30
```

Tags as the job boards list them, each counted once per posting.
Weeks start on Monday; the week in progress is left out.

## Week of <Value data={weeks} column=last_week fmt="yyyy-mm-dd" />: top 20 tags

<BarChart data={last_week_tags} x=tag y=postings swapXY=true sort=false yAxisTitle="Postings" />

## Last 4 complete weeks

<DataTable data={four_week_tags} rows=30>
    <Column id=tag title="Tag" />
    <Column id=postings title="Postings" />
    <Column id=weeks_in_top_50 title="Weeks in the top 50" />
</DataTable>
