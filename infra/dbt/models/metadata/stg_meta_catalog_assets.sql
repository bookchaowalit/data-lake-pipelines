-- One row per asset in architecture-docs metadata-catalog/catalog.yaml.
with latest as ({{ metadata_latest('catalog_assets') }})

select
    json_extract_string(payload_json, '$.id') as asset_id,
    json_extract_string(payload_json, '$.kind') as kind,
    json_extract_string(payload_json, '$.name') as name,
    json_extract_string(payload_json, '$.owner') as owner,
    json_extract_string(payload_json, '$.status') as status,
    json_extract_string(payload_json, '$.data_classification') as data_classification,
    json_extract_string(payload_json, '$.visibility') as visibility,
    json_extract_string(payload_json, '$.source_path') as source_path,
    exported_at
from latest
