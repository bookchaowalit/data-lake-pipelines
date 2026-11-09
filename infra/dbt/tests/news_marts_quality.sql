-- News marts must be internally consistent.

select 'daily' as mart, publisher as key
from {{ ref('news_articles_daily') }}
where articles < 1 or with_image > articles

union all

select 'feed_health', publisher
from {{ ref('news_feed_health') }}
where articles_7d > articles_total or last_published_at is null

union all

select 'latest', publisher
from {{ ref('news_latest_articles') }}
where rank < 1 or rank > 30
