-- Fastest-growing watched accounts: followers now vs first snapshot in the last 14 days.
with a as (
    select handle, user_id, snapshot_at, followers_count
    from read_parquet('{lake}/raw/x_account_snapshots/date=*/*.parquet', hive_partitioning = true)
    where snapshot_at >= now() - interval 14 day and account_status = 'ok'
)
select handle, user_id,
       arg_min(followers_count, snapshot_at) as followers_first,
       arg_max(followers_count, snapshot_at) as followers_last,
       arg_max(followers_count, snapshot_at) - arg_min(followers_count, snapshot_at) as gained,
       count(*) as snapshots
from a
group by handle, user_id
having count(*) >= 2
order by gained desc
limit 25;
