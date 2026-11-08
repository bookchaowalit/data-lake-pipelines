-- Staging model: one row per ADR-0008 envelope from domain=commerce/dataset=events (the hourly
-- lake sink of commerce.events.v1). Only manifest-committed Bronze parts are eligible (stream
-- conversion manifests in production, batch manifests for other writers).
--
-- Kafka delivers at least once, so one envelope `id` can land in several batches. The envelope id
-- is the event's identity; the earliest delivery wins, so received_at is when it first arrived.
-- Envelopes carry reference ids and facts only (no personal data). Spec: docs/specs/sales-marts.md.

{#- DuckDB fails on a glob that matches no file, and a table function cannot take a subquery, so the
    patterns that have files are found when the model runs. A lake with only one manifest family
    (batch-only, or a fresh stream-only lake) or no commerce parts yet builds an empty model. -#}
{%- set manifest_patterns = [
    var("bronze_manifest_root") ~ "/stream-parquet/source=*/conversion_id=*.json",
    var("bronze_manifest_root") ~ "/source=*/batch_id=*.json",
] -%}
{%- set parts_pattern = var("bronze_root") ~ "/domain=commerce/dataset=events/schema_version=*/ingest_date=*/source=*/part-*.parquet" -%}
{%- set present = [] -%}
{%- set has_parts = [] -%}
{%- if execute -%}
    {%- for pattern in manifest_patterns -%}
        {%- if run_query("select count(*) from glob('" ~ pattern ~ "')").columns[0].values()[0] > 0 -%}
            {%- do present.append(pattern) -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if run_query("select count(*) from glob('" ~ parts_pattern ~ "')").columns[0].values()[0] > 0 -%}
        {%- do has_parts.append(true) -%}
    {%- endif -%}
{%- endif %}

with
{%- if present and has_parts %}
committed_manifests as (
    select distinct
        cast(bronze.key as varchar) as bronze_key
    -- The hourly lake sink (event-sink.yml) commits Bronze through stream_sink.convert_avro_file,
    -- which writes stream-parquet conversion manifests; batch ingests write source=*/batch_id=*.
    -- Both kinds carry domain, dataset, schema_version and bronze.key.
    from read_json_auto(
        [{% for pattern in present %}'{{ pattern }}'{% if not loop.last %}, {% endif %}{% endfor %}],
        union_by_name = true
    )
    where domain = 'commerce'
      and dataset = 'events'
      and cast(schema_version as varchar) = '1'
),

source as (
    select parts.*
    from read_parquet(
        '{{ parts_pattern }}',
        filename = true
    ) as parts
    inner join committed_manifests as manifests
        on ends_with(
            replace(parts.filename, '\\', '/'),
            manifests.bronze_key
        )
),
{%- else %}
-- No committed commerce manifests or no commerce parts in this lake yet: same columns, no rows.
source as (
    select
        cast(null as varchar) as payload_json,
        cast(null as varchar) as source_record_id,
        cast(null as varchar) as received_at,
        cast(null as varchar) as privacy_class,
        cast(null as varchar) as ingest_run_id
    where false
),
{%- endif %}

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
        -- Minor units must be a JSON integer. DuckDB's JSON cast would turn 12.5 into 12, true
        -- into 1 and "12" into 12, so check the JSON type first; anything else is null and rejected.
        case
            when json_type(payload_json, '$.data.amount') in ('BIGINT', 'UBIGINT')
                then try_cast(json_extract(payload_json, '$.data.amount') as bigint)
        end as amount_minor,
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
