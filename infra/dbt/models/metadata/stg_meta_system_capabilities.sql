-- One row per (system, capability domain) from business-capabilities.yaml; rank 1 = primary.
with latest as ({{ metadata_latest('system_capabilities') }})

select
    json_extract_string(payload_json, '$.id') as system_capability_id,
    json_extract_string(payload_json, '$.system_id') as system_id,
    json_extract_string(payload_json, '$.domain_id') as domain_id,
    cast(json_extract(payload_json, '$.rank') as integer) as domain_rank,
    cast(json_extract(payload_json, '$.is_primary') as boolean) as is_primary,
    exported_at
from latest
