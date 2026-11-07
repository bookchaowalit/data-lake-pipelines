-- Staging model: one row per ADR-0008 envelope from domain=commerce/dataset=events (the hourly
-- lake sink of commerce.events.v1). Only manifest-committed Bronze parts are eligible.
--
-- Kafka delivers at least once, so one envelope `id` can land in several batches. The envelope id
-- is the event's identity; the earliest delivery wins, so received_at is when it first arrived.
-- Envelopes carry reference ids and facts only (no personal data). Spec: docs/specs/sales-marts.md.

with committed_manifests as (
    select distinct
        cast(bronze.key as varchar) as bronze_key
    from read_json_auto(
        '{{ var("bronze_manifest_root") }}/source=*/batch_id=*.json',
        union_by_name = true
    )
    where domain = 'commerce'
      and dataset = 'events'
      and cast(schema_version as varchar) = '1'
),

source as (
    select parts.*
    from read_parquet(
        '{{ var("bronze_root") }}/domain=commerce/dataset=events/schema_version=*/ingest_date=*/source=*/part-*.parquet',
        filename = true
    ) as parts
    inner join committed_manifests as manifests
        on ends_with(
            replace(parts.filename, '\\', '/'),
            manifests.bronze_key
        )
),

envelopes as (
    select
        coalesce(json_extract_string(payload_json, '$.id'), source_record_id) as event_key,
        json_extract_string(payload_json, '$.type') as event_type,
        try_cast(json_extract_string(payload_json, '$.occurred_at') as timestamptz) as occurred_at,
        try_cast(received_at as timestamptz) as received_at,
        json_extract_string(payload_json, '$.producer') as producer,
        nullif(trim(json_extract_string(payload_json, '$.data.workspace_id')), '') as workspace_id,
        nullif(trim(json_extract_string(payload_json, '$.data.intent_id')), '') as intent_id,
        nullif(trim(json_extract_string(payload_json, '$.data.refund_id')), '') as refund_id,
        -- Minor units must be whole numbers: "12.5" or a string fails the cast and is rejected later.
        try_cast(json_extract(payload_json, '$.data.amount') as bigint) as amount_minor,
        upper(trim(json_extract_string(payload_json, '$.data.currency'))) as currency,
        json_extract_string(payload_json, '$.data.provider') as provider,
        privacy_class,
        ingest_run_id
    from source
)

select *
from envelopes
where event_key is not null
qualify row_number() over (
    partition by event_key
    order by received_at asc nulls last, ingest_run_id
) = 1
