-- Staging model: the news Silver snapshot, typed for the news marts
-- (docs/specs/news-marts.md). Like stg_job_postings, the caller must name
-- exactly one Silver part as `news_silver_part`; news_marts.py passes it.
-- Only valid rows are in this dataset (rejects live in articles_rejects).
-- `summary` and `author` stay in Silver: no mart selects them.

{#- Only building the model needs the part; compile/docs render a placeholder. -#}
{% set silver_part = var("news_silver_part", "") %}
{% if not silver_part and execute and flags.WHICH in ("build", "run") %}
    {{ exceptions.raise_compiler_error(
        "stg_news_articles needs --vars '{news_silver_part: <one Silver part>}'; "
        "run infra/scripts/data_lake/news_marts.py instead of a bare dbt build"
    ) }}
{% endif %}

with source as (
    select *
    from read_parquet('{{ silver_part or "news_silver_part-unset.parquet" }}')
    where quality_status = 'valid'
),

deduped as (
    -- One part has one row per record_id already; this guards the contract.
    select *
    from source
    qualify row_number() over (partition by record_id order by received_at desc) = 1
),

typed as (
    select
        record_id,
        publisher,
        coalesce(nullif(trim(section), ''), 'unspecified') as section,
        headline,
        canonical_url,
        nullif(trim(image_url), '') is not null as has_image,
        -- UTC instants stored naive (the profile pins TimeZone=UTC).
        cast(published_at as timestamptz) at time zone 'UTC' as published_at,
        try_cast(event_time as timestamptz) at time zone 'UTC' as seen_at,
        silver_run_id
    from deduped
)

select
    *,
    cast(timezone('{{ var("news_timezone") }}', published_at::timestamptz) as date) as published_date
from typed
