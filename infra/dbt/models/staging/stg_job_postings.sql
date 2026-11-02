-- Staging model: the jobs Silver snapshot, typed for the jobs marts.
--
-- Every Silver run writes a full snapshot part, so a glob over all parts
-- would count a job once per run. The jobs_marts runner passes the exact part
-- of the run it just built as `jobs_silver_part`; the glob default is a
-- local-development convenience, made safe by keeping one row per record_id.
-- Only valid rows are in this dataset (rejects live in job_postings_rejects).

with source as (
    select *
    from read_parquet('{{ var("jobs_silver_part") }}')
    where quality_status = 'valid'
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by record_id
        order by received_at desc, silver_run_id desc
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
