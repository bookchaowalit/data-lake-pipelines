-- Ordered stages of each value stream; systems = who does the step today (empty = broken step).
with latest as ({{ metadata_latest('value_stream_stages') }})

select
    json_extract_string(payload_json, '$.id') as stage_id,
    json_extract_string(payload_json, '$.value_stream_id') as value_stream_id,
    cast(json_extract(payload_json, '$.position') as integer) as position,
    json_extract_string(payload_json, '$.name') as name,
    cast(json_extract(payload_json, '$.systems') as varchar[]) as systems,
    exported_at
from latest
