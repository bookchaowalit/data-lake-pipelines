# Spec: sales marts (fact_sales, dim_customer SCD2, sales_daily_revenue)

Status: proposed (2026-11-07; review fixes same day; Gold, schedule and revenue page added 2026-11-09). Standard: ADR-0004 (spec, then failing test, then code).
Code: `infra/dbt/models/staging/stg_commerce_events.sql`, `infra/dbt/models/marts/sales/`.
Tests: `infra/dbt/tests/sales_marts_quality.sql`, `infra/tests/data_lake/dbt_lake_test.py`
(`check_sales_outputs`). Platform rules: ADR-0013 (one R2 lake, DuckDB + dbt, no warehouse).

## Problem

The commerce event plane (ADR-0008) already lands `payment.intent.succeeded` envelopes from the
platform-payment outbox in Bronze `domain=commerce/dataset=events` every hour. Nothing turns
them into sales numbers. Jobs is the only domain with marts, and it has no dimensional model
(no fact/dimension split, no history of changing attributes).

The marts must also hold up against what an at-least-once event stream does in production:
the same event delivered twice, events that arrive days late, refunds that land after the sale
day was reported, and malformed events.

## Source contract

Bronze rows from `domain=commerce/dataset=events`, manifest-committed parts only. In production
the lake sink commits them through `stream_sink.convert_avro_file`, whose manifests live under
`control/manifests/stream-parquet/source=*/conversion_id=*.json`; batch manifests
(`control/manifests/source=*/batch_id=*.json`) are read too. The test fixture lands its events
through the same sink and converter. `payload_json`
is an ADR-0008 envelope: `id`, `type`, `version`, `occurred_at`, `producer`, `data`.
`received_at` is when the lake sink landed it.

| type | data fields used | producer status |
|---|---|---|
| `payment.intent.succeeded` | `workspace_id`, `intent_id`, `amount` (minor units), `currency`, `provider` | live (platform-payment 9eb7c86) |
| `payment.refund.succeeded` | `workspace_id`, `intent_id`, `refund_id`, `amount` (minor units), `currency` | live (platform-payment fb1034d, when a refund settles as succeeded) |

Other `type`s are ignored. The customer is the paying workspace (`workspace_id`). No personal
data is read; envelopes carry reference ids only.

## Behaviour

1. **Dedupe (idempotency).** One envelope `id` is one event. A redelivered envelope (same `id`,
   later `received_at`) counts once; the earliest delivery wins, so `received_at` is when the
   event first arrived. A second success for the same `intent_id`, or a second refund with the
   same `refund_id`, also counts once.
2. **Validation.** A payment or refund is rejected when `intent_id` (or `refund_id` for
   refunds) or `workspace_id` is missing, `amount` is not a positive integer, `currency` is not
   three letters, or `occurred_at` does not parse. `amount` must be a JSON integer: `12.5`, `"1200"`
   and `true` are rejected, not rounded or coerced. A refund is also rejected as `orphan_refund`
   when no valid payment has its `intent_id`, as `currency_mismatch` when its currency differs
   from the payment's, or as `workspace_mismatch` when its workspace differs. It is checked
   against the payment that is kept (the earliest valid success of that intent), never another
   duplicate. Rejects go to `sales_rejected_events` with a `reject_reason`. They are
   never silently dropped.
3. **Business date.** `sale_date` / `refund_date` is the `occurred_at` date in
   `var('sales_timezone')` (default `Asia/Bangkok`), not UTC and not the arrival date.
4. **Late arrival.** An event is late when `received_at - occurred_at` is more than
   `var('sales_late_after_hours')` (default 24). Late events still land on their real business
   date. Every run rebuilds from full Bronze history, so a late event restates the day it
   belongs to. `fact_sales.arrived_late` and `sales_daily_revenue.late_orders` show how much of
   a day came in late.
5. **fact_sales**: grain is one row per `intent_id`. Columns: `intent_id`, `customer_id`,
   `customer_sk`, `customer_tier_at_sale`, `provider`, `currency`, `occurred_at`, `received_at`,
   `sale_date`, `gross_minor`, `refunded_minor` (all valid refunds of that intent, whenever they
   happened), `net_minor = gross_minor - refunded_minor`, `refund_count`, `over_refunded`
   (refunds > gross), `arrived_late`. Amounts are never converted between currencies.
6. **dim_customer (SCD Type 2)**: `tier` is `vip` while the customer's running net revenue in a
   currency is at or above that currency's threshold in `var('sales_vip_threshold_minor')`
   (default THB 10,000,000 = 100,000 baht; USD 300,000 = 3,000 dollars), otherwise `standard`.
   A currency without a threshold never makes a customer VIP, and an empty map (`{}`) turns VIP
   off for everyone. Payments raise the running net and
   refunds lower it, so tier can go up and back down. Each tier change opens a new version:
   `customer_sk` (md5 of `customer_id` and `valid_from`), `customer_id`, `version`, `tier`,
   `valid_from`, `valid_to` (null on the current row), `is_current`, `first_sale_at`. Movements at
   the same instant count as one change (the state after all of them), so a version never opens
   and closes at the same timestamp. A
   customer's versions never overlap and leave no gaps, and exactly one is current. The
   dimension is derived from facts only, so a rebuild from Bronze reproduces it.
7. **Point-in-time join.** `fact_sales.customer_tier_at_sale` is the version valid at the sale's
   `occurred_at` (`valid_from <= occurred_at < valid_to`), which already counts the sale itself.
   A later refund never rewrites the tier a past sale was made at.
8. **sales_daily_revenue**: grain is (`business_date`, `currency`). `orders`, `customers`,
   `gross_minor`, `refunded_against_sales_minor` (refunds attributed back to the sale's day; the
   restated view), `net_minor` = `gross_minor - refunded_against_sales_minor`,
   `refunds_booked_minor` (refunds by the day they happened; the cash view), `late_orders`. A day
   with only refunds has a row with zero orders.

## Metrics (semantic definitions)

Defined once in `marts/sales/schema.yml` under `meta.metrics`, so every consumer (BI, API,
agents) uses the same meaning:

- **gross revenue** = sum of `gross_minor`, by sale date.
- **net revenue** = sum of `net_minor`, by sale date (restated when refunds arrive).
- **refunds booked** = sum of `refunds_booked_minor`, by refund date.
- **VIP customers** = customers whose current `dim_customer` row has `tier = 'vip'`.

## Gold, schedule and revenue page (added 2026-11-09)

Trigger met: platform-payment is ready in Stripe test mode and the hosted `payment-relay` cron
(2026-11-09) sends `payment.intent.succeeded` to Kafka every hour at :50.

9. **Runner** `infra/scripts/data_lake/sales_marts.py`: copies Bronze
   `domain=commerce/dataset=events/`, the commerce stream conversion manifests
   (`control/manifests/stream-parquet/`) and the commerce batch manifests of every source that has
   commerce parts (`control/manifests/source=<src>/batch_id=*.json`) from the lake into a scratch directory (one recursive
   copy per prefix), runs `dbt build --select +path:models/marts/sales` against it, and publishes
   `fact_sales`, `dim_customer`, `sales_daily_revenue` and `sales_rejected_events` as write-once
   Gold parts `gold/domain=sales/dataset=<mart>/schema_version=1/part-<gold_run_id>.parquet`, a
   manifest under `control/manifests/gold/source=commerce-events/domain=sales/` and a publish
   pointer under `control/pointers/gold/source=commerce-events/domain=sales/`.
   `gold_run_id` = hash(sha256 of every copied input object, model files incl. `dbt_project.yml`,
   duckdb/pyarrow versions), so unchanged inputs write nothing new (the newest pointer is reused
   when it names the same Gold run). An empty lake publishes empty marts; that is a success.
10. **Schedule and alerting.** `.github/workflows/sales-marts.yml`, started by the jobs queue
   (`sales-marts`, `20 * * * *`, 15 min after the lake sink). Public log: counts and run ids only,
   never intent or workspace ids. Heartbeat `sales-marts` (3 h); `failed` = `sales-marts`,
   `sales-bi`, or `rejects` when `sales_rejected_events` is not empty (a contract break upstream).
11. **Revenue page** (`sales_bi_snapshot.py`, `sales_bi_site.py`): static, script-free, strict CSP,
   Cloudflare Pages `revenue-insights` behind Access (owner policy). Per currency: orders, gross,
   refunds booked, net (last 30 business days and all time), VIP customers; a daily revenue table
   for the last 30 business days; the newest 20 sales (date, amount, tier, provider; customer and
   intent ids shortened to 8 characters). Amounts are shown in major units with the currency code;
   currencies are never added together. Major units follow the ISO 4217 exponent (0 for KRW, JPY …,
   3 for KWD, BHD …, otherwise 2). Deployed only when `<gold_run_id>@<commit>` is not live.

## Not in scope (named triggers)

- Currency conversion: only when two currencies must be summed for a report.
- Iceberg MERGE / incremental models: only when a full rebuild passes 5 minutes (ADR-0013).
- Reverse ETL of VIP tier to the CRM: when the CRM has a field to receive it.
