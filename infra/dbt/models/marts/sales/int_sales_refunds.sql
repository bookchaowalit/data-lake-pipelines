{{ config(materialized='view') }}
-- Valid refunds of a known payment, one row per refund_id.

select *
from {{ ref('stg_commerce_events') }}
where event_type = 'payment.refund.succeeded'
  and event_key not in (select event_key from {{ ref('sales_rejected_events') }})
qualify row_number() over (partition by refund_id order by occurred_at, received_at, event_key) = 1
