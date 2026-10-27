-- One row per edge in architecture-docs metadata-catalog/lineage.yaml.
with latest as ({{ metadata_latest('lineage_edges') }})

select
    json_extract_string(payload_json, '$.id') as edge_id,
    json_extract_string(payload_json, '$.from_asset') as from_asset,
    json_extract_string(payload_json, '$.to_asset') as to_asset,
    json_extract_string(payload_json, '$.relationship') as relationship,
    json_extract_string(payload_json, '$.evidence') as evidence,
    exported_at
from latest
