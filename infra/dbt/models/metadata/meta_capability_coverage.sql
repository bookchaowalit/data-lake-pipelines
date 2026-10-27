-- One row per EOS capability domain: how many systems serve it, how many run,
-- and whether automated data reaches it. platform_coverage is a mechanical
-- reading of the registries (missing = no running system; partial = systems run
-- but no automated inbound flow lands in them; served = both). The business
-- judgement stays in architecture-docs docs/architecture/EOS-GAP-MAP.md.
with serving as (
    select
        caps.domain_id,
        caps.is_primary,
        landscape.system_id,
        landscape.status,
        landscape.automated_flows,
        landscape.consumers
    from {{ ref('stg_meta_system_capabilities') }} as caps
    inner join {{ ref('meta_system_landscape') }} as landscape
        on landscape.system_id = caps.system_id
),

per_domain as (
    select
        domain_id,
        count(*) as systems,
        count(*) filter (where is_primary) as primary_systems,
        count(*) filter (where status not in ('planned', 'staged')) as running_systems,
        sum(automated_flows) as automated_flows,
        sum(consumers) as consumers,
        list(system_id order by system_id) as system_ids
    from serving
    group by domain_id
)

select
    domains.domain_id,
    domains.domain_number,
    domains.name,
    coalesce(per_domain.systems, 0) as systems,
    coalesce(per_domain.primary_systems, 0) as primary_systems,
    coalesce(per_domain.running_systems, 0) as running_systems,
    coalesce(per_domain.automated_flows, 0) as automated_flows,
    coalesce(per_domain.consumers, 0) as consumers,
    coalesce(per_domain.system_ids, []::varchar[]) as system_ids,
    case
        when coalesce(per_domain.running_systems, 0) = 0 then 'missing'
        when coalesce(per_domain.automated_flows, 0) = 0 then 'partial'
        else 'served'
    end as platform_coverage
from {{ ref('stg_meta_capability_domains') }} as domains
left join per_domain
    on per_domain.domain_id = domains.domain_id
