-- One row per inbound flow: is it captured, by which system and host, and what
-- does it feed. The quick answer to "which sources are still outside the platform".
with reach as (
    select
        source_id,
        count(*) as reachable_assets,
        count(*) filter (where asset_kind = 'consumer') as consumers
    from {{ ref('meta_source_lineage') }}
    group by source_id
)

select
    sources.source_id,
    sources.name,
    sources.channel,
    sources.automation,
    sources.is_candidate,
    sources.destination_system,
    systems.system_id is not null as system_registered,
    systems.host_provider,
    systems.status as system_status,
    len(sources.catalog_assets) as catalog_assets,
    coalesce(reach.reachable_assets, 0) as reachable_assets,
    coalesce(reach.consumers, 0) as consumers,
    sources.probe_count > 0 as freshness_probed
from {{ ref('stg_meta_data_sources') }} as sources
left join {{ ref('stg_meta_systems') }} as systems
    on systems.system_id = sources.destination_system
left join reach
    on reach.source_id = sources.source_id
