-- One row per (value-stream stage, system) so the relationship to the system registry can be tested.
select
    stages.stage_id || ':' || system_id as stage_system_id,
    stages.stage_id,
    stages.value_stream_id,
    stages.position,
    system_id
from {{ ref('stg_meta_value_stream_stages') }} as stages,
    unnest(stages.systems) as t(system_id)
