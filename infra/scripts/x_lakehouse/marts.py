"""Gold layer for the X digest: plain views over the curated tables (spec: docs/specs/x-digest.md).

The curated tables are small, so views are enough (ADR-0013: one lake, no new engine). ``digest_log``
records each digest by id (window, status, counts, tokens, cost), never message text.
"""

from __future__ import annotations

from typing import Any

from .schemas import PG_SCHEMA, SHEET_TZ

S = PG_SCHEMA
TZ = SHEET_TZ

# Higher = more important. Urgent alerts first, then digest alerts, then opportunity score, source
# tier and engagement. Kept in SQL so the digest and any BI tool rank the same way.
# The tier weight (1000s) is larger than the most the secondary terms can add (100 + 15 + 10 + 10 =
# 135), so an urgent candidate always ranks above any digest alert, and a digest alert above the rest.
PRIORITY = """
      case alert_tier when 'urgent_candidate' then 2000 when 'digest' then 1000 else 0 end
    + least(5, greatest(0, coalesce(opportunity_score, 0))) * 20
    + case when cred_tier in ('T1', 'T2') then 15 when cred_tier in ('T3', 'T3b') then 5 else 0 end
    + case when signal_type in ('Opportunity', 'Warning', 'Launch') then 10 else 0 end
    + least(10, ln(1 + coalesce(likes, 0) + coalesce(reposts, 0) * 2) * 1.5)
"""


def marts_ddl() -> str:
    return f"""
create or replace view {S}.mart_x_signal_priority as
select post_id, collected_at, (collected_at at time zone '{TZ}') as collected_ict,
       domain, topic, category, signal_type, alert_tier, route, cred_tier, content_label,
       opportunity_score, verification_status, suggested_action, author_handle, post_url,
       post_text, likes, reposts, views,  -- full text: the digest screens it before any cut
       round(({PRIORITY})::numeric, 2) as priority
from {S}.x_signals;

create or replace view {S}.mart_x_domain_hourly as
select date_trunc('hour', collected_at at time zone '{TZ}') as hour_ict,
       coalesce(domain, '(none)') as domain,
       count(*) as signals,
       count(*) filter (where alert_tier is not null and alert_tier <> 'none') as alerts,
       count(*) filter (where alert_tier = 'urgent_candidate') as urgent,
       count(*) filter (where signal_type = 'Opportunity' or coalesce(opportunity_score, 0) >= 3) as opportunities,
       count(*) filter (where cred_tier in ('T1', 'T2')) as strong_sources,
       round(avg(coalesce(opportunity_score, 0))::numeric, 2) as avg_score
from {S}.x_signals
group by 1, 2;

create or replace view {S}.mart_claims_queue as
select claim_id, claim, verification_status, queue_state, priority, next_check,
       last_checked, first_seen_link, checked_by
from {S}.claims
where coalesce(lower(queue_state), '') not in ('closed', 'done', 'resolved', 'archived', 'dismissed')
order by priority nulls last, next_check nulls last, claim_id;

create table if not exists {S}.digest_log (
    digest_id text primary key,
    kind text not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    status text not null,
    signals int,
    sent_messages int,
    tokens_in int,
    tokens_out int,
    cost_usd double precision,
    error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

do $$ begin
  if exists (select 1 from pg_roles where rolname = 'anon') then
    revoke all on {S}.mart_x_signal_priority, {S}.mart_x_domain_hourly, {S}.mart_claims_queue, {S}.digest_log from anon;
  end if;
  if exists (select 1 from pg_roles where rolname = 'authenticated') then
    revoke all on {S}.mart_x_signal_priority, {S}.mart_x_domain_hourly, {S}.mart_claims_queue, {S}.digest_log
      from authenticated;
  end if;
end $$;
"""


def ensure_marts(conn: Any) -> None:
    from . import curated as cur

    cur.ensure_schema(conn)
    with conn.cursor() as c:
        c.execute(marts_ddl())
    conn.commit()
