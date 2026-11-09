-- Gold mart: the newest 30 articles per publisher. Headline and link only;
-- article text, author and image stay out of Gold.

with as_of as (
    select cast(timezone('{{ var("news_timezone") }}', max(seen_at)::timestamptz) as date) as as_of_date
    from {{ ref('stg_news_articles') }}
),

ranked as (
    select
        articles.publisher,
        articles.section,
        articles.published_at,
        articles.headline,
        articles.canonical_url,
        row_number() over (
            partition by articles.publisher
            order by articles.published_at desc, articles.record_id
        ) as rank
    from {{ ref('stg_news_articles') }} as articles
    cross join as_of
    where articles.published_date <= as_of.as_of_date
)

select * from ranked where rank <= 30
