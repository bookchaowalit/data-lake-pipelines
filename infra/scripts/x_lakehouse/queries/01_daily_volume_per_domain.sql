-- Daily signal volume per domain (ICT days), last 30 days. Deduped by post_id (raw keeps every sighting).
with s as (
    select post_id, domain, alert_tier, collected_at
    from read_parquet('{lake}/raw/x_signals/date=*/*.parquet', hive_partitioning = true)
    qualify row_number() over (partition by post_id order by collected_at) = 1
)
select
    cast(timezone('Asia/Bangkok', collected_at) as date) as day_ict,
    domain,
    count(*)                                                as signals,
    count(*) filter (where alert_tier <> 'none')            as alerts
from s
where collected_at >= now() - interval 30 day
group by all
order by day_ict desc, signals desc;
