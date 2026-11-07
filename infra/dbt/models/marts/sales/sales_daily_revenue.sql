-- Daily revenue per business date (var sales_timezone) and currency.
-- refunded_against_sales_minor restates the sale's day; refunds_booked_minor is the cash view
-- (the day the refund happened). Spec: docs/specs/sales-marts.md (Behaviour 8).

with sales as (
    select
        sale_date as business_date,
        currency,
        count(*) as orders,
        count(distinct customer_id) as customers,
        sum(gross_minor) as gross_minor,
        sum(refunded_minor) as refunded_against_sales_minor,
        sum(net_minor) as net_minor,
        count(*) filter (where arrived_late) as late_orders
    from {{ ref('fact_sales') }}
    group by 1, 2
),

booked as (
    select
        cast(timezone('{{ var("sales_timezone") }}', occurred_at) as date) as business_date,
        currency,
        sum(amount_minor) as refunds_booked_minor
    from {{ ref('int_sales_refunds') }}
    group by 1, 2
)

select
    business_date,
    currency,
    coalesce(sales.orders, 0) as orders,
    coalesce(sales.customers, 0) as customers,
    coalesce(sales.gross_minor, 0) as gross_minor,
    coalesce(sales.refunded_against_sales_minor, 0) as refunded_against_sales_minor,
    coalesce(sales.net_minor, 0) as net_minor,
    coalesce(booked.refunds_booked_minor, 0) as refunds_booked_minor,
    coalesce(sales.late_orders, 0) as late_orders
from sales
full outer join booked using (business_date, currency)
order by business_date, currency
