-- Top accounts by distinct posts the watches collected (last 30 days), from raw X Feed only.
-- Replaces 02_top_accounts_by_signals (2026-11-14): the bots send no X Signals since 2026-11-13 (raw only).
-- Counts come from each post's newest observation; a post seen by several watches counts once.
with f as (
    select post_id, author_handle, coalesce(watch_type, '') || ':' || coalesce(watch_value, '') as watch,
           likes, reposts, views, collected_at
    from read_parquet('{lake}/raw/x_feed/date=*/*.parquet', hive_partitioning = true, union_by_name = true)
    where collected_at >= {now} - interval 30 day
      and coalesce(post_id, '') <> '' and coalesce(author_handle, '') <> ''
),
latest as (
    select * from f
    qualify row_number() over (partition by post_id order by collected_at desc) = 1
),
w as (
    select author_handle, count(distinct watch) as watches, mode(watch) as main_watch
    from f
    group by author_handle
)
select
    l.author_handle,
    count(*)                 as posts,
    sum(l.likes)             as total_likes,
    sum(l.reposts)           as total_reposts,
    sum(l.views)             as total_views,
    any_value(w.watches)     as watches,
    any_value(w.main_watch)  as main_watch
from latest l
join w using (author_handle)
group by l.author_handle
order by posts desc, total_views desc, l.author_handle
limit 25;
