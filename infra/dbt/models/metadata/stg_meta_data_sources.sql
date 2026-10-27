-- One row per inbound flow in solo-empire data-sources.yaml (latest registry export).
with latest as ({{ metadata_latest('data_sources') }})

select
    json_extract_string(payload_json, '$.id') as source_id,
    json_extract_string(payload_json, '$.name') as name,
    json_extract_string(payload_json, '$.channel') as channel,
    json_extract_string(payload_json, '$.data') as data_description,
    json_extract_string(payload_json, '$.cadence') as cadence,
    json_extract_string(payload_json, '$.automation') as automation,
    json_extract_string(payload_json, '$.destination_system') as destination_system,
    cast(json_extract(payload_json, '$.destination_cells') as varchar[]) as destination_cells,
    json_extract_string(payload_json, '$.destination_store') as destination_store,
    json_extract_string(payload_json, '$.repo') as repo,
    cast(json_extract(payload_json, '$.catalog_assets') as varchar[]) as catalog_assets,
    cast(json_extract(payload_json, '$.probe_count') as integer) as probe_count,
    cast(json_extract(payload_json, '$.candidate') as boolean) as is_candidate,
    exported_at
from latest
