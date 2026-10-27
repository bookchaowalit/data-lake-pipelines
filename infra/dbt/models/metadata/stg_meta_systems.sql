-- One row per system in solo-empire online-systems.yaml (latest registry export).
with latest as ({{ metadata_latest('systems') }})

select
    json_extract_string(payload_json, '$.id') as system_id,
    json_extract_string(payload_json, '$.name') as name,
    json_extract_string(payload_json, '$.kind') as kind,
    json_extract_string(payload_json, '$.status') as status,
    json_extract_string(payload_json, '$.phase') as phase,
    json_extract_string(payload_json, '$.repo') as repo,
    json_extract_string(payload_json, '$.host_provider') as host_provider,
    json_extract_string(payload_json, '$.host_mode') as host_mode,
    exported_at
from latest
