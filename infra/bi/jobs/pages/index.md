---
title: Jobs overview
---

```sql meta
select gold_run_id, silver_run_id, as_of_date, max_seen_at from jobs.gold_meta
```

```sql totals
select
    sum(postings_total) as postings,
    count(*) as boards,
    sum(postings_7d) as postings_7d
from jobs.board_health
```

```sql companies
select count(*) as companies from jobs.company_hiring
```

```sql daily_90d
select posted_date, board, postings
from jobs.postings_daily
where posted_date > (select as_of_date from jobs.gold_meta) - interval 90 day
  and posted_date <= (select as_of_date from jobs.gold_meta)
order by posted_date
```

Data as of <Value data={meta} column=as_of_date fmt="yyyy-mm-dd" /> (newest capture <Value data={meta} column=max_seen_at fmt="yyyy-mm-dd H:MM" /> UTC).

<Grid cols=4>
    <BigValue data={totals} value=postings title="Postings" />
    <BigValue data={totals} value=postings_7d title="Posted in the last 7 days" />
    <BigValue data={totals} value=boards title="Boards" />
    <BigValue data={companies} value=companies title="Companies" />
</Grid>

## Postings per day, last 90 days

<BarChart
    data={daily_90d}
    x=posted_date
    y=postings
    series=board
    type=stacked
    xAxisTitle="Posted date"
    yAxisTitle="Postings"
/>

- [Boards](/boards): health and freshness of each job board
- [Skills](/skills): which tags are in demand each week
- [Companies](/companies): who is hiring

<small>Built from Gold run <Value data={meta} column=gold_run_id /> (Silver <Value data={meta} column=silver_run_id />). This site reads only the jobs Gold marts on the R2 lake.</small>
