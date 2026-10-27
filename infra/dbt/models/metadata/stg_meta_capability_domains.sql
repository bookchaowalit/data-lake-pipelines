-- The 15 EOS capability domains from solo-empire business-capabilities.yaml (latest export).
with latest as ({{ metadata_latest('capability_domains') }})

select
    json_extract_string(payload_json, '$.id') as domain_id,
    cast(json_extract(payload_json, '$.number') as integer) as domain_number,
    json_extract_string(payload_json, '$.name') as name,
    exported_at
from latest
