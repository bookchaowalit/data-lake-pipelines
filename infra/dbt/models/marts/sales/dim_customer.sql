-- SCD Type 2 customer dimension. Customer = paying workspace. Tier is derived from facts only
-- (running net revenue per currency against var sales_vip_threshold_minor), so a rebuild from
-- Bronze reproduces every version. Spec: docs/specs/sales-marts.md (Behaviour 6).

{% set thresholds = var('sales_vip_threshold_minor') %}

with movements as (
    select workspace_id as customer_id, currency, occurred_at, event_key, amount_minor as delta_minor
    from {{ ref('int_sales_payments') }}
    union all
    select workspace_id, currency, occurred_at, event_key, -amount_minor
    from {{ ref('int_sales_refunds') }}
),

running as (
    select
        *,
        sum(delta_minor) over (
            partition by customer_id, currency
            order by occurred_at, event_key
            rows between unbounded preceding and current row
        ) as running_net_minor,
        case currency
            {%- for code, minor in thresholds.items() %}
            when '{{ code }}' then {{ minor | int }}
            {%- endfor %}
        end as vip_threshold_minor
    from movements
),

flagged as (
    select
        *,
        coalesce(running_net_minor >= vip_threshold_minor, false)::int as vip_in_currency
    from running
),

flag_changes as (
    -- Each movement can only change its own currency's VIP flag: +1 on entering, -1 on leaving.
    select
        *,
        vip_in_currency - coalesce(
            lag(vip_in_currency) over (partition by customer_id, currency order by occurred_at, event_key),
            0
        ) as vip_change
    from flagged
),

counted as (
    -- How many currencies the customer is VIP in after each movement.
    select
        *,
        sum(vip_change) over (
            partition by customer_id
            order by occurred_at, event_key
            rows between unbounded preceding and current row
        ) as vip_currencies
    from flag_changes
),

tiered as (
    select
        customer_id,
        occurred_at,
        event_key,
        case when vip_currencies > 0 then 'vip' else 'standard' end as tier
    from counted
),

changes as (
    select *
    from tiered
    qualify lag(tier) over (partition by customer_id order by occurred_at, event_key) is distinct from tier
),

first_sales as (
    select workspace_id as customer_id, min(occurred_at) as first_sale_at
    from {{ ref('int_sales_payments') }}
    group by 1
)

select
    -- epoch_us, not a rendered timestamp, so the key does not depend on the session time zone.
    md5(changes.customer_id || '|' || cast(epoch_us(changes.occurred_at) as varchar)) as customer_sk,
    changes.customer_id,
    row_number() over (partition by changes.customer_id order by changes.occurred_at, changes.event_key) as version,
    changes.tier,
    changes.occurred_at as valid_from,
    lead(changes.occurred_at) over (partition by changes.customer_id order by changes.occurred_at, changes.event_key) as valid_to,
    lead(changes.occurred_at) over (partition by changes.customer_id order by changes.occurred_at, changes.event_key) is null as is_current,
    first_sales.first_sale_at
from changes
left join first_sales using (customer_id)
