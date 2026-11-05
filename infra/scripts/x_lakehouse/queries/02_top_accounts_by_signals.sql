-- Top accounts by signal count (last 30 days): X Signals joined to X Feed for the author.
with s as (
    select post_id, domain, cred_tier, route, collected_at
    from read_parquet('{lake}/raw/x_signals/date=*/*.parquet', hive_partitioning = true)
    qualify row_number() over (partition by post_id order by collected_at) = 1
),
f as (
    select post_id, author_handle, likes, views
    from read_parquet('{lake}/raw/x_feed/date=*/*.parquet', hive_partitioning = true)
    qualify row_number() over (partition by post_id order by collected_at desc) = 1
)
select
    f.author_handle,
    count(*)                                         as signals,
    count(*) filter (where s.route = 'alert_digest') as alert_digest,
    mode(s.domain)                                   as main_domain,
    min(s.cred_tier)                                 as best_cred_tier,
    sum(f.views)                                     as total_views
from s
join f using (post_id)
where s.collected_at >= now() - interval 30 day
group by f.author_handle
order by signals desc, total_views desc
limit 25;
