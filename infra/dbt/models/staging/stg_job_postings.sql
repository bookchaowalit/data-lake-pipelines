-- Staging model: the jobs Silver snapshot, typed for the jobs marts.
--
-- Every Silver run writes a full snapshot part, and a later snapshot can drop
-- a record (e.g. quarantine it), so no mix of parts is ever correct. The
-- caller must name exactly one part as `jobs_silver_part`; the jobs_marts
-- runner passes the part of the run it just built.
-- Only valid rows are in this dataset (rejects live in job_postings_rejects).

{#- Only building the model needs the part; compile/docs render a placeholder. -#}
{% set silver_part = var("jobs_silver_part", "") %}
{% if not silver_part and execute and flags.WHICH in ("build", "run") %}
    {{ exceptions.raise_compiler_error(
        "stg_job_postings needs --vars '{jobs_silver_part: <one Silver part>}'; "
        "run infra/scripts/data_lake/jobs_marts.py instead of a bare dbt build"
    ) }}
{% endif %}

with source as (
    select *
    from read_parquet('{{ silver_part or "jobs_silver_part-unset.parquet" }}')
    where quality_status = 'valid'
),

deduped as (
    -- One part has one row per record_id already; this guards the contract.
    select *
    from source
    qualify row_number() over (
        partition by record_id
        order by received_at desc
    ) = 1
)

select
    record_id,
    board,
    provider,
    title,
    nullif(trim(company), '') as company,
    lower(nullif(trim(company), '')) as company_key,
    canonical_url,
    nullif(trim(location), '') as location,
    nullif(trim(salary_text), '') as salary_text,
    try_cast(posted_at as date) as posted_date,
    -- Instant in UTC, stored naive: offsets are honoured, and offset-less
    -- values are read as UTC because the profile pins TimeZone=UTC.
    try_cast(event_time as timestamptz) at time zone 'UTC' as seen_at,
    from_json(tags, '["VARCHAR"]') as tags,
    keyword,
    privacy_class,
    silver_run_id
from deduped
