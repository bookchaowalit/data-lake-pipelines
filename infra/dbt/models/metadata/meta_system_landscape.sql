-- One row per system in online-systems.yaml: where it runs, which EOS capability
-- domains it serves (primary first), which inbound flows land in it and which
-- catalog assets and consumers those flows reach. Generated from the registries
-- and the metadata catalog; nothing here is written by hand.
with domains as (
    select
        system_id,
        max(domain_id) filter (where is_primary) as primary_domain,
        list(domain_id order by domain_rank) as domains
    from {{ ref('stg_meta_system_capabilities') }}
    group by system_id
),

flows as (
    select
        destination_system as system_id,
        count(*) as inbound_flows,
        count(*) filter (where automation = 'automated') as automated_flows
    from {{ ref('stg_meta_data_sources') }}
    where destination_system is not null
    group by destination_system
),

reach as (
    select
        sources.destination_system as system_id,
        count(distinct lineage.asset_id) as catalog_assets,
        count(distinct lineage.asset_id) filter (where lineage.asset_kind = 'consumer') as consumers
    from {{ ref('meta_source_lineage') }} as lineage
    inner join {{ ref('stg_meta_data_sources') }} as sources
        on sources.source_id = lineage.source_id
    where sources.destination_system is not null
    group by sources.destination_system
)

select
    systems.system_id,
    systems.name,
    systems.kind,
    systems.status,
    systems.host_provider,
    systems.host_mode,
    systems.repo,
    domains.primary_domain,
    coalesce(domains.domains, []::varchar[]) as domains,
    domains.system_id is not null as capability_mapped,
    coalesce(flows.inbound_flows, 0) as inbound_flows,
    coalesce(flows.automated_flows, 0) as automated_flows,
    coalesce(reach.catalog_assets, 0) as catalog_assets,
    coalesce(reach.consumers, 0) as consumers
from {{ ref('stg_meta_systems') }} as systems
left join domains
    on domains.system_id = systems.system_id
left join flows
    on flows.system_id = systems.system_id
left join reach
    on reach.system_id = systems.system_id
