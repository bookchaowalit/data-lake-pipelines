-- Gold mart: one row per publisher. "7d" is the 7 dates ending on the newest
-- capture date (Bangkok) in the snapshot, not the wall clock, so a rebuild
-- of one Silver run always yields the same rows.

with as_of as (
    select cast(timezone('{{ var("news_timezone") }}', max(seen_at)::timestamptz) as date) as as_of_date
    from {{ ref('stg_news_articles') }}
)

select
    articles.publisher,
    count(*) as articles_total,
    count(*) filter (where articles.published_date > as_of.as_of_date - 7) as articles_7d,
    max(articles.published_at) as last_published_at,
    max(articles.seen_at) as last_seen_at,
    any_value(as_of.as_of_date) as as_of_date
from {{ ref('stg_news_articles') }} as articles
cross join as_of
where articles.published_date <= as_of.as_of_date
group by 1
