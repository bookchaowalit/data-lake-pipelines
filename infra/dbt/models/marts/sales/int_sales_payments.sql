{{ config(materialized='view') }}
-- Valid payments, one row per intent_id (a repeated success for the same intent counts once).

select *
from {{ ref('stg_commerce_events') }}
where event_type = 'payment.intent.succeeded'
  and event_key not in (select event_key from {{ ref('sales_rejected_events') }})
qualify row_number() over (partition by intent_id order by occurred_at, received_at, event_key) = 1
