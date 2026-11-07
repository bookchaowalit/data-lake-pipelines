-- Payment and refund events that failed validation, with the reason. Never silently dropped.
-- Spec: docs/specs/sales-marts.md (Behaviour 2).

with checked as (
    select
        *,
        case
            when workspace_id is null or intent_id is null
                or (event_type = 'payment.refund.succeeded' and refund_id is null) then 'missing_id'
            when amount_minor is null or amount_minor <= 0 then 'invalid_amount'
            when currency is null or not regexp_full_match(currency, '[A-Z]{3}') then 'invalid_currency'
            when occurred_at is null then 'invalid_time'
        end as field_reject_reason
    from {{ ref('stg_commerce_events') }}
    where event_type in ('payment.intent.succeeded', 'payment.refund.succeeded')
),

valid_payments as (
    -- The payment int_sales_payments keeps (same ordering), so a refund is checked against the
    -- payment it will be subtracted from.
    select intent_id, currency, workspace_id
    from checked
    where event_type = 'payment.intent.succeeded' and field_reject_reason is null
    qualify row_number() over (partition by intent_id order by occurred_at, received_at, event_key) = 1
),

reasons as (
    select
        checked.*,
        coalesce(
            field_reject_reason,
            case
                when event_type = 'payment.refund.succeeded' and valid_payments.intent_id is null
                    then 'orphan_refund'
                when event_type = 'payment.refund.succeeded' and valid_payments.currency <> checked.currency
                    then 'currency_mismatch'
                when event_type = 'payment.refund.succeeded' and valid_payments.workspace_id <> checked.workspace_id
                    then 'workspace_mismatch'
            end
        ) as reject_reason
    from checked
    left join valid_payments on valid_payments.intent_id = checked.intent_id
)

select
    event_key,
    event_type,
    reject_reason,
    occurred_at,
    received_at,
    workspace_id,
    intent_id,
    refund_id,
    amount_minor,
    currency
from reasons
where reject_reason is not null
