-- Gold mart: articles per (Bangkok publish date, publisher, section).
-- Articles published after the as-of date never count (see stg/as-of rules).

with as_of as (
    select cast(timezone('{{ var("news_timezone") }}', max(seen_at)::timestamptz) as date) as as_of_date
    from {{ ref('stg_news_articles') }}
)

select
    articles.published_date,
    articles.publisher,
    articles.section,
    count(*) as articles,
    count(*) filter (where articles.has_image) as with_image
from {{ ref('stg_news_articles') }} as articles
cross join as_of
where articles.published_date <= as_of.as_of_date
group by 1, 2, 3
