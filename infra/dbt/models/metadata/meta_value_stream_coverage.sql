-- One row per EOS value stream: how many stages have a running system and which
-- steps are broken (no system, or none of its systems runs). flow_status:
-- flowing = every stage served; partial = some; missing = none.
with stage_status as (
    select
        stages.value_stream_id,
        stages.position,
        stages.name,
        count(landscape.system_id) filter (where landscape.status not in ('planned', 'staged')) > 0 as served
    from {{ ref('stg_meta_value_stream_stages') }} as stages
    left join {{ ref('meta_value_stream_stage_systems') }} as links
        on links.stage_id = stages.stage_id
    left join {{ ref('meta_system_landscape') }} as landscape
        on landscape.system_id = links.system_id
    group by all
),

per_stream as (
    select
        value_stream_id,
        count(*) as stages,
        count(*) filter (where served) as served_stages,
        list(name order by position) filter (where not served) as broken_stages
    from stage_status
    group by value_stream_id
)

select
    streams.value_stream_id,
    streams.value_stream_number,
    streams.name,
    per_stream.stages,
    per_stream.served_stages,
    coalesce(per_stream.broken_stages, []::varchar[]) as broken_stages,
    case
        when per_stream.served_stages = per_stream.stages then 'flowing'
        when per_stream.served_stages = 0 then 'missing'
        else 'partial'
    end as flow_status
from {{ ref('stg_meta_value_streams') }} as streams
inner join per_stream
    on per_stream.value_stream_id = streams.value_stream_id
