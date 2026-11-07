-- Sales fact. Grain: one row per payment intent. Refunds are attributed back to their sale,
-- whenever they happened. Amounts stay in minor units of their own currency.
-- Spec: docs/specs/sales-marts.md (Behaviour 3-5, 7).

with refunds as (
    select intent_id, sum(amount_minor) as refunded_minor, count(*) as refund_count
    from {{ ref('int_sales_refunds') }}
    group by 1
)

select
    p.intent_id,
    p.workspace_id as customer_id,
    d.customer_sk,
    d.tier as customer_tier_at_sale,
    p.provider,
    p.currency,
    p.occurred_at,
    p.received_at,
    cast(timezone('{{ var("sales_timezone") }}', p.occurred_at) as date) as sale_date,
    p.amount_minor as gross_minor,
    coalesce(r.refunded_minor, 0) as refunded_minor,
    p.amount_minor - coalesce(r.refunded_minor, 0) as net_minor,
    coalesce(r.refund_count, 0) as refund_count,
    coalesce(r.refunded_minor, 0) > p.amount_minor as over_refunded,
    coalesce(
        epoch(p.received_at) - epoch(p.occurred_at) > {{ var("sales_late_after_hours") | int }} * 3600,
        false
    ) as arrived_late
from {{ ref('int_sales_payments') }} as p
left join refunds as r using (intent_id)
-- Point in time: the customer version valid when the sale happened (it already counts this sale).
left join {{ ref('dim_customer') }} as d
    on d.customer_id = p.workspace_id
   and d.valid_from <= p.occurred_at
   and (d.valid_to is null or p.occurred_at < d.valid_to)
