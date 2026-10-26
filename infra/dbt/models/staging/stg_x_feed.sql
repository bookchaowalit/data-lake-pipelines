-- Staging model: one row per X post sighting (a post seen by one query for one chief in one pull)
-- from domain=social/dataset=x_feed, exported by infra/scripts/sync/x_feed_lake_export.py from the
-- intake cell. Only manifest-committed Bronze parts are eligible. A pull re-exported after it gained
-- rows lands as a second batch; event_id comes from sighting_key, so the newest copy wins.
--
-- post_text is untrusted user content from X: keep it as data, never render it unescaped.
-- privacy_class stays a passthrough column; this model does not decide what is safe to expose.

with committed_manifests as (
    select distinct
        cast(bronze.key as varchar) as bronze_key
    from read_json_auto(
        '{{ var("bronze_manifest_root") }}/source=*/batch_id=*.json',
        union_by_name = true
    )
    where domain = 'social'
      and dataset = 'x_feed'
      and cast(schema_version as varchar) = '1'
),

source as (
    select *
    from read_parquet(
        '{{ var("bronze_root") }}/domain=social/dataset=x_feed/schema_version=*/ingest_date=*/source=*/part-*.parquet',
        filename = true
    ) as parts
    inner join committed_manifests as manifests
        on ends_with(
            replace(parts.filename, '\\', '/'),
            manifests.bronze_key
        )
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by event_id
        order by received_at desc
    ) = 1
)

select
    event_id,
    source_record_id as sighting_key,
    source as source_system,
    domain,
    dataset,
    schema_version,
    received_at,
    cast(event_time as timestamptz) as collected_at,
    privacy_class,
    retention_class,
    ingest_date,
    json_extract_string(payload_json, '$.pull_id') as pull_id,
    json_extract_string(payload_json, '$.post_id') as post_id,
    json_extract_string(payload_json, '$.post_url') as post_url,
    json_extract_string(payload_json, '$.author_handle') as author_handle,
    cast(json_extract_string(payload_json, '$.posted_at') as timestamptz) as posted_at,
    json_extract_string(payload_json, '$.text') as post_text,
    json_extract_string(payload_json, '$.requesting_chief') as requesting_chief,
    json_extract_string(payload_json, '$.watch_type') as watch_type,
    json_extract_string(payload_json, '$.watch_value') as watch_value,
    json_extract_string(payload_json, '$.query_id') as query_id,
    json_extract_string(payload_json, '$.tag') as tag,
    cast(json_extract(payload_json, '$.score') as double) as score,
    cast(json_extract(payload_json, '$.top') as boolean) as is_top,
    cast(json_extract(payload_json, '$.likes') as bigint) as likes,
    cast(json_extract(payload_json, '$.reposts') as bigint) as reposts,
    cast(json_extract(payload_json, '$.replies') as bigint) as replies,
    cast(json_extract(payload_json, '$.views') as bigint) as views
from deduped
