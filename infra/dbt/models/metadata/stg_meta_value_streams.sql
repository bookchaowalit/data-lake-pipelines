-- The 8 EOS value streams from solo-empire business-capabilities.yaml (latest export).
with latest as ({{ metadata_latest('value_streams') }})

select
    json_extract_string(payload_json, '$.id') as value_stream_id,
    cast(json_extract(payload_json, '$.number') as integer) as value_stream_number,
    json_extract_string(payload_json, '$.name') as name,
    cast(json_extract(payload_json, '$.stage_count') as integer) as stage_count,
    exported_at
from latest
