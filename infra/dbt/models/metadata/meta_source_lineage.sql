-- Every catalog asset an inbound flow reaches: its own catalog_assets (depth 0)
-- and everything downstream of them through lineage edges.
with recursive reach(source_id, asset_id, depth) as (
    select source_id, unnest(catalog_assets), 0
    from {{ ref('stg_meta_data_sources') }}
    union all
    select reach.source_id, edges.to_asset, reach.depth + 1
    from reach
    inner join {{ ref('stg_meta_lineage_edges') }} as edges
        on edges.from_asset = reach.asset_id
    where reach.depth < 10
)

select
    reach.source_id,
    reach.asset_id,
    assets.kind as asset_kind,
    assets.status as asset_status,
    min(reach.depth) as depth
from reach
left join {{ ref('stg_meta_catalog_assets') }} as assets
    on assets.asset_id = reach.asset_id
group by all
