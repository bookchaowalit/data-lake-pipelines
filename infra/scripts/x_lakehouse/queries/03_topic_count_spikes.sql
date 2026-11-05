-- Spike detection over X Topic Counts. Two views of a spike per (topic, snapshot):
--   bot_spike   the bot's own flag (ratio of the latest hour vs the prior-6-day median)
--   z_24h       24 h volume vs the same topic's previous up-to-14 snapshots (z-score)
-- A row is flagged when either says so and the topic has enough history.
with t as (
    select query_id, topic, snapshot_at, total_24h, max_hour_24h, ratio, spike as bot_spike
    from read_parquet('{lake}/raw/x_topic_counts/date=*/*.parquet', hive_partitioning = true)
    qualify row_number() over (partition by query_id, snapshot_at order by _ingested_at desc) = 1
),
w as (
    select *,
        avg(total_24h)         over prior as mean_prev,
        stddev_samp(total_24h) over prior as sd_prev,
        count(*)               over prior as n_prev
    from t
    window prior as (partition by query_id order by snapshot_at rows between 14 preceding and 1 preceding)
)
select
    timezone('Asia/Bangkok', snapshot_at) as snapshot_ict,
    query_id, topic, total_24h, max_hour_24h, ratio, bot_spike,
    round((total_24h - mean_prev) / nullif(sd_prev, 0), 2) as z_24h,
    n_prev
from w
where bot_spike
   or (n_prev >= 3 and (total_24h - mean_prev) / nullif(sd_prev, 0) >= 2)
order by snapshot_at desc, z_24h desc nulls last;
