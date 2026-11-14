-- Daily post volume per watch (ICT days), last 30 days, from raw X Feed only.
-- Replaces 01_daily_volume_per_domain (2026-11-14): the bots send no X Signals since 2026-11-13 (raw only).
-- A post counts once per watch and day (raw keeps every sighting); engagement uses that day's newest counts.
with f as (
    select post_id, coalesce(watch_type, '') || ':' || coalesce(watch_value, '') as watch,
           cast(timezone('Asia/Bangkok', collected_at) as date) as day_ict,
           likes, reposts, replies, collected_at
    from read_parquet('{lake}/raw/x_feed/date=*/*.parquet', hive_partitioning = true, union_by_name = true)
    where collected_at >= {now} - interval 30 day and collected_at <= {now}
      and coalesce(post_id, '') <> ''
),
d as (
    select * from f
    qualify row_number() over (partition by day_ict, watch, post_id order by collected_at desc) = 1
)
select
    day_ict,
    watch,
    count(*)                                                                    as posts,
    sum(coalesce(likes, 0) + 2 * coalesce(reposts, 0) + coalesce(replies, 0))   as engagement
from d
group by all
order by day_ict desc, posts desc, watch;
