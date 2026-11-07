-- Sales marts must be internally consistent (docs/specs/sales-marts.md).

select 'fact_net' as check_name, intent_id as key
from {{ ref('fact_sales') }}
where net_minor <> gross_minor - refunded_minor
   or gross_minor <= 0
   or (net_minor < 0 and not over_refunded)
   or customer_sk is null

union all

-- SCD2: versions of one customer never overlap, leave no gap, and exactly one is current.
select 'dim_chain', customer_id
from (
    select
        customer_id,
        valid_from,
        valid_to,
        is_current,
        lead(valid_from) over (partition by customer_id order by version) as next_from
    from {{ ref('dim_customer') }}
)
where (valid_to is null) <> is_current
   or (valid_to is not null and valid_to is distinct from next_from)
   or (valid_to is not null and valid_to <= valid_from)

union all

select 'dim_one_current', customer_id
from {{ ref('dim_customer') }}
group by customer_id
having count(*) filter (where is_current) <> 1

union all

-- Daily totals reconcile with the fact table.
select 'daily_reconcile', currency
from (
    select currency, sum(net_minor) as net from {{ ref('sales_daily_revenue') }} group by 1
) as daily
full outer join (
    select currency, sum(net_minor) as net from {{ ref('fact_sales') }} group by 1
) as fact using (currency)
where daily.net is distinct from fact.net
