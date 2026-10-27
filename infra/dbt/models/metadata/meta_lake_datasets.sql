-- One row per Bronze dataset and source that has at least one committed manifest:
-- what the lake actually holds, whatever the registry says.
select
    domain,
    dataset,
    source,
    count(*) as batches,
    sum(record_count) as records,
    min(received_at) as first_received_at,
    max(received_at) as last_received_at
from read_json_auto(
    '{{ var("bronze_manifest_root") }}/source=*/batch_id=*.json',
    union_by_name = true
)
group by domain, dataset, source
