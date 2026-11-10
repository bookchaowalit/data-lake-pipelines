#!/usr/bin/env python3
"""Bot inbox: the SQL door the bots use instead of appending rows to the Sheet.

The bots cannot run this package and hold no R2 or database credentials, but they can run SQL in
the Supabase project through the Supabase connector. So the bot-facing interface is one function:

    select x_intel.ingest('<dataset>', '[{...}, ...]'::jsonb, '<run_id>');

``ingest()`` (SECURITY DEFINER, owned by the schema owner, EXECUTE for ``postgres`` only):

* checks the dataset name and every row (an object whose keys are the dataset's Sheet header names;
  an unknown column, a nested value or a blank key column rejects that row, and only that row);
* stores each accepted row in ``x_intel.ingest_inbox`` keyed by ``(dataset, row_key)``, where
  ``row_key`` is computed exactly like ``coerce.row_key`` (tested on fixtures of every dataset);
  a raw dataset ignores a row it already holds; a config dataset (no raw history) keeps the newest
  version of the row so the drain applies the latest edit;
* for the curated tables also upserts Postgres at once, with the same rules as ``curated.upsert``
  (change-only updates, no update from an older version, claims Book checked stay as they are), so
  the next bot run can read its query-plan state before the cron drains;
* returns ``{"inserted", "skipped", "upserted", "errors"}``.

The only R2 writer stays the Railway cron (``scheduled.py``): with ``X_LAKE_SOURCE=inbox|both`` it
drains undrained inbox rows through ``append_rows`` (R2 raw + curated), marks them drained, and
after a validated run deletes rows drained more than three days ago.

Read helpers for the bots, in the Sheet's column order and cell format (text, ICT wall time):
``x_intel.v_query_plan_sheet``, ``x_intel.v_watchlist_sheet``, ``x_intel.v_claims_open``.

    python -m x_lakehouse.inbox --print-ddl      # review the migration
    python -m x_lakehouse.inbox --apply          # apply it (X_LAKE_DB_URL; owner approval)
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

from .schemas import CURATED, DATASETS, PG_SCHEMA, POSTGRES, SHEET_TZ, Curated, Dataset, Kind  # noqa: E402
from .coerce import TZ as TZ_ICT  # noqa: E402

S = PG_SCHEMA
INBOX = f"{S}.ingest_inbox"
DRAIN_LOCK = 0x786C616B65  # "xlake": pg_try_advisory_lock key held by the one drainer
CONFIG_LOCK = 0x786C616B6563  # "xlakec": transaction lock serialising config ingest() with the replay
# Raw ingest() holds it shared for its transaction, prune() exclusively: a key's move from the inbox to
# ingest_seen is never seen half-done (no inbox row yet no ingest_seen row) by a resend in flight.
PRUNE_LOCK = 0x786C616B6570  # "xlakep"
KEEP_DRAINED = timedelta(days=3)
KEEP_DELIVERIES = timedelta(days=30)  # longer than any bot retry
# Datasets whose stored inbox row is replaced by a newer version of the same key (no raw history:
# the drain must apply the latest edit, not the first one).
REPLACEABLE = tuple(n for n, d in DATASETS.items() if not d.raw)
# Raw datasets with a curated table: prune keeps each pruned key's payload hash in ingest_seen, so a
# key resent with other values is still refused once its inbox row is gone (raw keeps the first
# version and would never take the change; curated alone would diverge from raw).
SEEN_GUARDED = tuple(n for n, d in DATASETS.items() if d.raw and n in CURATED)
CLOSED_STATES = ("closed", "done", "resolved", "archived", "dismissed")  # same list as serve_sheet

# Python's str.strip() whitespace set, so SQL trims exactly what coerce.raw_text trims.
_WS_CHARS = ("\t\n\x0b\x0c\r\x1c\x1d\x1e\x1f \x85\xa0 " + "".join(chr(c) for c in range(0x2000, 0x200B))
             + "    　")
_WS = "".join(f"\\u{ord(c):04x}" for c in _WS_CHARS)  # PG regex escapes (standard_conforming_strings)


def _lit(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _arr(items: list[str] | tuple[str, ...]) -> str:
    return "array[" + ", ".join(_lit(i) for i in items) + "]::text[]"


def _helpers() -> str:
    """Pure SQL helpers mirroring coerce.py. ``_raw_text`` must match ``coerce.raw_text`` byte for byte
    (row keys); the typed casts may be slightly looser because the drain re-applies the Python rules."""
    return f"""
-- Payload hash for ingest_seen, equal exactly when jsonb equality holds (the in-inbox check uses
-- "is distinct from", where 3 = 3.0 = 3.00): numbers are hashed as their trimmed numeric value.
create or replace function {S}.payload_md5(r jsonb) returns text language sql immutable parallel safe
set search_path = pg_catalog as $f$
  select md5(coalesce(jsonb_object_agg(k, case when jsonb_typeof(v) = 'number'
                                               then to_jsonb(trim_scale((v #>> '{{}}')::numeric)) else v end),
                      '{{}}'::jsonb)::text)
  from jsonb_each(r) e(k, v)
$f$;
create or replace function {S}._strip(t text) returns text language sql immutable parallel safe
set search_path = pg_catalog as $f$
  select regexp_replace(t, '^[{_WS}]+|[{_WS}]+$', '', 'g')
$f$;

-- json.loads of the jsonb text gives int (no '.') or float; coerce.raw_text prints floats that are
-- whole and below 2^53 as integers and others like Python's repr.
create or replace function {S}._num_text(n numeric) returns text language plpgsql immutable parallel safe
set search_path = pg_catalog set extra_float_digits = 1 as $f$
declare t text := n::text; f float8;
begin
  if position('.' in t) = 0 then return t; end if;
  f := t::float8;
  if f = trunc(f) and abs(f) < 9007199254740992 then return (f::bigint)::text; end if;
  if f = trunc(f) and abs(f) < 1e16 then return (f::bigint)::text || '.0'; end if;
  return f::text;
end $f$;

create or replace function {S}._raw_text(v jsonb) returns text language sql immutable parallel safe
set search_path = pg_catalog, {S} as $f$
  select coalesce(case jsonb_typeof(v)
    when 'string' then {S}._strip(v #>> '{{}}')
    when 'boolean' then case when (v #>> '{{}}')::boolean then 'TRUE' else 'FALSE' end
    when 'number' then {S}._num_text((v #>> '{{}}')::numeric)
    when 'null' then ''
    else v::text end, '')
$f$;

create or replace function {S}.row_key(p_dataset text, p_keys text[], r jsonb) returns text
language sql immutable parallel safe set search_path = pg_catalog, {S} as $f$
  select encode(sha256(convert_to(p_dataset || chr(30)
           || coalesce((select string_agg({S}._raw_text(r -> k), chr(31) order by i)
                        from unnest(p_keys) with ordinality u(k, i)), ''), 'UTF8')), 'hex')
$f$;

create or replace function {S}._c_text(v jsonb) returns text language sql immutable parallel safe
set search_path = pg_catalog, {S} as $f$
  select case when jsonb_typeof(v) = 'number' and abs((v #>> '{{}}')::numeric) >= 9007199254740992
              then null else nullif({S}._raw_text(v), '') end
$f$;

create or replace function {S}._c_int(v jsonb) returns bigint language plpgsql immutable parallel safe
set search_path = pg_catalog, {S} as $f$
declare t text;
begin
  if v is null or jsonb_typeof(v) in ('null', 'boolean', 'object', 'array') then return null; end if;
  if jsonb_typeof(v) = 'number' then
    if (v #>> '{{}}')::numeric <> trunc((v #>> '{{}}')::numeric) then return null; end if;
    return (v #>> '{{}}')::numeric::bigint;
  end if;
  t := replace({S}._strip(v #>> '{{}}'), ',', '');
  if t ~ '^[+-]?[0-9]+$' then return t::bigint; end if;
  return null;
exception when others then return null;
end $f$;

create or replace function {S}._c_float(v jsonb) returns float8 language plpgsql immutable parallel safe
set search_path = pg_catalog, {S} as $f$
declare t text;
begin
  if v is null or jsonb_typeof(v) in ('null', 'boolean', 'object', 'array') then return null; end if;
  t := replace({S}._strip(v #>> '{{}}'), ',', '');
  if t = '' then return null; end if;
  return t::float8;
exception when others then return null;
end $f$;

create or replace function {S}._c_bool(v jsonb) returns boolean language sql immutable parallel safe
set search_path = pg_catalog, {S} as $f$
  select case when jsonb_typeof(v) = 'boolean' then (v #>> '{{}}')::boolean
              when jsonb_typeof(v) = 'string' and upper({S}._strip(v #>> '{{}}')) = 'TRUE' then true
              when jsonb_typeof(v) = 'string' and upper({S}._strip(v #>> '{{}}')) = 'FALSE' then false end
$f$;

create or replace function {S}._c_ts(v jsonb) returns timestamptz language plpgsql immutable parallel safe
set search_path = pg_catalog, {S} as $f$
declare t text; m text[];
begin
  if v is null or jsonb_typeof(v) <> 'string' then return null; end if;
  t := {S}._strip(v #>> '{{}}');
  m := regexp_match(t, '^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})[ T](\\d{{2}}):(\\d{{2}})(?::(\\d{{2}}))?$');
  if m is not null then
    return make_timestamptz(m[1]::int, m[2]::int, m[3]::int, m[4]::int, m[5]::int,
                            coalesce(m[6], '0')::int, {_lit(SHEET_TZ)});
  end if;
  m := regexp_match(t, '^(\\d{{4}}-\\d{{2}}-\\d{{2}}[T ]\\d{{2}}:\\d{{2}}(?::\\d{{2}}(?:\\.\\d+)?)?)(Z|[+-]\\d{{2}}(?::?\\d{{2}})?)?$');
  if m is null then return null; end if;
  if m[2] is null then return (m[1]::timestamp) at time zone {_lit(SHEET_TZ)}; end if;
  return (m[1] || replace(m[2], 'Z', '+00:00'))::timestamptz;
exception when others then return null;
end $f$;

create or replace function {S}._c_date(v jsonb) returns date language plpgsql immutable parallel safe
set search_path = pg_catalog, {S} as $f$
begin
  if v is null or jsonb_typeof(v) <> 'string' then return null; end if;
  return left({S}._strip(v #>> '{{}}'), 10)::date;
exception when others then return null;
end $f$;
"""


def _cast(expr: str, kind: Kind) -> str:
    return f"{S}._c_{'text' if kind == 'str' else kind}({expr})"


def _inbox_table() -> str:
    return f"""
create table if not exists {INBOX} (
    id bigserial primary key,
    dataset text not null,
    run_id text not null,
    row jsonb not null,
    row_key text not null,
    received_at timestamptz not null default now(),
    drained_batch_id text,
    drained_at timestamptz,
    unique (dataset, row_key)
);
-- Config deliveries already applied: a retried file from an earlier run must not replace a later edit.
create table if not exists {S}.ingest_deliveries (
    dataset text not null,
    row_key text not null,
    run_id text not null,
    payload_md5 text not null,
    received_at timestamptz not null default now(),
    primary key (dataset, row_key, run_id, payload_md5)
);
-- Keys pruned from the inbox (curated raw datasets only), kept so a changed resend is still refused.
create table if not exists {S}.ingest_seen (
    dataset text not null,
    row_key text not null,
    payload_md5 text not null,
    primary key (dataset, row_key)
);
create index if not exists ingest_inbox_undrained_idx on {INBOX} (id) where drained_at is null;
create index if not exists ingest_inbox_drained_at_idx on {INBOX} (drained_at) where drained_at is not null;
create index if not exists ingest_inbox_run_idx on {INBOX} (run_id, dataset);
create index if not exists ingest_inbox_feed_post_idx on {INBOX} ((row ->> 'post_id'), received_at)
    where dataset = 'x_feed';
"""


def _upsert_fn(c: Curated) -> str:
    """SQL twin of curated.upsert for one table, fed with the accepted rows of one ingest() call."""
    ds = c.source
    cols = [n for n, _ in c.data_columns]
    joined = {n for n, _, _ in c.joined}
    src = [f"{_cast(f'r.j -> {_lit(n)}', k)} as {n}" for n, k in ds.columns]
    for n, k, from_col in c.joined:
        src.append(f"{_cast(f'f.row -> {_lit(from_col)}', k)} as {n}")
    feed = ""
    if c.joined:
        # Post facts from the newest X Feed row the bots sent for this post (send x_feed first).
        feed = (f" left join lateral (select i.row from {INBOX} i where i.dataset = 'x_feed' "
                f"and i.row ->> 'post_id' = {S}._raw_text(r.j -> 'post_id') "
                f"order by i.received_at desc, i.id desc limit 1) f on true")
    order = ", ".join(c.pk) + (f", {c.version} desc nulls last" if c.version else "") + ", ord desc"
    sets = [f"{n} = coalesce(excluded.{n}, t.{n})" if n in joined else f"{n} = excluded.{n}"
            for n in cols if n not in c.pk]
    changed = " or ".join(
        (f"(excluded.{n} is not null and t.{n} is distinct from excluded.{n})" if n in joined
         else f"t.{n} is distinct from excluded.{n}") for n in cols if n not in c.pk)
    guard = f" where ({changed})" if changed else " where true"
    if c.version:
        guard += f" and (t.{c.version} is null or excluded.{c.version} >= t.{c.version})"
    if c.protect:
        col, val = c.protect
        guard += (f" and not (lower(coalesce(t.{col}, '')) = {_lit(val)}"
                  f" and lower(coalesce(excluded.{col}, '')) <> {_lit(val)})")
    col_list = ", ".join(cols)
    elig = ""
    if c.protect:
        # Mirrors curated.eligible: once Book checked the claim (stored or in this call), only Book's
        # rows are candidates, so a later bot row cannot hide an earlier Book row of the same call.
        col, val = c.protect
        on = " and ".join(f"s.{k} = src.{k}" for k in c.pk)
        same = " and ".join(f"o.{k} = src.{k}" for k in c.pk)
        mine = f"lower(coalesce(src.{col}, '')) = {_lit(val)}"
        elig = (f", elig as (\n    select src.* from src left join {S}.{c.table} s on {on}\n"
                f"    where {mine} or not (lower(coalesce(s.{col}, '')) = {_lit(val)}"
                f" or exists (select 1 from src o where {same} and lower(coalesce(o.{col}, '')) = {_lit(val)}))\n  )")
    return f"""
create or replace function {S}._ingest_upsert_{c.table}(p_rows jsonb, p_batch text) returns jsonb
language plpgsql security definer set search_path = pg_catalog, {S} as $f$
declare n_ins int; n_upd int; n_rows int;
begin
  with src as (
    select r.ord, {', '.join(src)}
    from jsonb_array_elements(p_rows) with ordinality r(j, ord){feed}
  ){elig}, latest as (
    select distinct on ({', '.join(c.pk)}) * from {'elig' if c.protect else 'src'}
    where {' and '.join(f'{k} is not null' for k in c.pk)}
    order by {order}
  ), up as (
    insert into {S}.{c.table} as t ({col_list}, _batch_id)
    select {col_list}, p_batch from latest
    on conflict ({', '.join(c.pk)}) do update set {', '.join(sets + ['_batch_id = excluded._batch_id', 'updated_at = now()'])}
    {guard}
    returning (xmax = 0) as ins
  )
  select count(*) filter (where ins), count(*) filter (where not ins), (select count(*) from latest)
  into n_ins, n_upd, n_rows from up;
  return jsonb_build_object('rows', n_rows, 'inserted', n_ins, 'updated', n_upd);
end $f$;
revoke all on function {S}._ingest_upsert_{c.table}(jsonb, text) from public;
"""


def _ingest_fn() -> str:
    meta = []
    for n, d in DATASETS.items():
        meta.append(f"    when {_lit(n)} then cols := {_arr(d.names)}; keys := {_arr(d.key)}; "
                    f"req := {_arr(d.required_columns)};")
    upserts = "\n".join(f"    when {_lit(t)} then up := {S}._ingest_upsert_{t}(good, 'ingest-' || p_run_id);"
                        for t in CURATED)
    return f"""
create or replace function {S}.ingest(p_dataset text, p_rows jsonb, p_run_id text) returns jsonb
language plpgsql security definer set search_path = pg_catalog, {S} as $f$
declare
  cols text[]; keys text[]; req text[];
  r jsonb; i int := 0; n int; bad text[]; nested text[]; blank text[]; rk text; seen text;
  ins int := 0; skipped int := 0; errors jsonb := '[]'::jsonb; good jsonb := '[]'::jsonb; up jsonb;
begin
  case p_dataset
{chr(10).join(meta)}
    else
      return jsonb_build_object('inserted', 0, 'skipped', 0, 'upserted', null, 'errors',
        jsonb_build_array(jsonb_build_object('row', null, 'error', 'unknown dataset', 'dataset', p_dataset)));
  end case;
  if p_run_id is null or p_run_id !~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{{0,63}}$' then
    return jsonb_build_object('inserted', 0, 'skipped', 0, 'upserted', null, 'errors',
      jsonb_build_array(jsonb_build_object('row', null, 'error', 'run_id must match [A-Za-z0-9_.:-], 1-64 chars')));
  end if;
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then
    return jsonb_build_object('inserted', 0, 'skipped', 0, 'upserted', null, 'errors',
      jsonb_build_array(jsonb_build_object('row', null, 'error', 'rows must be a JSON array of objects')));
  end if;
  if p_dataset = any({_arr(REPLACEABLE)}) then
    perform pg_advisory_xact_lock({CONFIG_LOCK});  -- one config writer at a time (see reapply_config)
  elsif p_dataset = any({_arr(SEEN_GUARDED)}) then
    perform pg_advisory_xact_lock_shared({PRUNE_LOCK});  -- wait out a prune in progress (see prune)
  end if;
  for r in select value from jsonb_array_elements(p_rows) loop
    i := i + 1;
    if jsonb_typeof(r) <> 'object' then
      errors := errors || jsonb_build_object('row', i, 'error', 'row is not an object');
      continue;
    end if;
    bad := array(select k from jsonb_object_keys(r) k where not (k = any(cols)));
    if cardinality(bad) > 0 then
      errors := errors || jsonb_build_object('row', i, 'error', 'unknown columns', 'columns', to_jsonb(bad));
      continue;
    end if;
    nested := array(select k from jsonb_each(r) e(k, v) where jsonb_typeof(v) in ('object', 'array'));
    if cardinality(nested) > 0 then
      errors := errors || jsonb_build_object('row', i, 'error', 'nested values', 'columns', to_jsonb(nested));
      continue;
    end if;
    blank := array(select k from unnest(req) k where _raw_text(r -> k) = '');
    if cardinality(blank) > 0 then
      errors := errors || jsonb_build_object('row', i, 'error', 'key column is blank', 'columns', to_jsonb(blank));
      continue;
    end if;
    rk := row_key(p_dataset, keys, r);
    if p_dataset = any({_arr(REPLACEABLE)}) then
      -- The same delivery again (a retried file): skip it, even if a later run changed the row since.
      insert into ingest_deliveries (dataset, row_key, run_id, payload_md5)
      values (p_dataset, rk, p_run_id, payload_md5(r)) on conflict do nothing;
      get diagnostics n = row_count;
      if n = 0 then skipped := skipped + 1; continue; end if;
      insert into ingest_inbox as t (dataset, run_id, row, row_key) values (p_dataset, p_run_id, r, rk)
      on conflict (dataset, row_key) do update
        set row = excluded.row, run_id = excluded.run_id, received_at = clock_timestamp(),
            drained_batch_id = null, drained_at = null
        where t.row is distinct from excluded.row;
      get diagnostics n = row_count;
      if n > 0 then ins := ins + 1; else skipped := skipped + 1; end if;
      good := good || jsonb_build_array(r);
    else
      select s.payload_md5 into seen from ingest_seen s where s.dataset = p_dataset and s.row_key = rk;
      if seen is not null then  -- drained and pruned earlier: same rules as a key still in the inbox
        if seen <> payload_md5(r) then
          errors := errors || jsonb_build_object('row', i, 'error',
            'key already stored with different values; send a new version');
        else
          skipped := skipped + 1;
        end if;
        continue;
      end if;
      insert into ingest_inbox (dataset, run_id, row, row_key) values (p_dataset, p_run_id, r, rk)
      on conflict (dataset, row_key) do nothing;
      get diagnostics n = row_count;
      if n > 0 then
        ins := ins + 1;
        good := good || jsonb_build_array(r);  -- only new rows reach the curated upsert
      elsif exists (select 1 from ingest_inbox x where x.dataset = p_dataset and x.row_key = rk
                    and x.row is distinct from r) then
        -- Raw keeps the first version of a key; applying the changed values to curated only would be
        -- rolled back by the drain. A correction must come as a new version (e.g. a new collected_at).
        errors := errors || jsonb_build_object('row', i, 'error',
          'key already stored with different values; send a new version');
      else
        skipped := skipped + 1;
      end if;
    end if;
  end loop;
  case p_dataset
{upserts}
    else up := null;
  end case;
  return jsonb_build_object('inserted', ins, 'skipped', skipped, 'upserted', up, 'errors', errors);
end $f$;
revoke all on function {S}.ingest(text, jsonb, text) from public;
"""


def _sheet_cell(col: str, kind: Kind) -> str:
    if kind == "ts":
        return f"coalesce(to_char({col} at time zone {_lit(SHEET_TZ)}, 'YYYY-MM-DD HH24:MI'), '')"
    if kind == "date":
        return f"coalesce(to_char({col}, 'YYYY-MM-DD'), '')"
    if kind == "bool":
        return f"coalesce(case when {col} then 'TRUE' else 'FALSE' end, '')"
    if kind == "float":
        return f"coalesce({S}._num_text({col}::numeric), '')"
    return f"coalesce({col}::text, '')"


def _sheet_view(name: str, ds: Dataset, table: str, where: str = "", order: str = "") -> str:
    cols = ",\n    ".join(f"{_sheet_cell(n, k)} as {n}" for n, k in ds.columns)
    return (f"create or replace view {S}.{name} as select\n    {cols}\nfrom {S}.{table}"
            f"{' where ' + where if where else ''}{' order by ' + order if order else ''};\n")


def _views() -> str:
    claims = CURATED["claims"]
    closed = ", ".join(_lit(s) for s in CLOSED_STATES)
    return (_sheet_view("v_query_plan_sheet", DATASETS["x_query_plan"], "x_query_plan", order="query_id")
            + _sheet_view("v_watchlist_sheet", DATASETS["x_watchlist"], "x_watchlist",
                          order="requesting_chief, type, value")
            + f"create or replace view {S}.v_claims_open as select {', '.join(n for n, _ in claims.data_columns)} "
              f"from {S}.claims where coalesce(lower(queue_state), '') not in ({closed}) "
              "order by priority nulls last, next_check nulls last, claim_id;\n")


def _grants() -> str:
    """EXECUTE / SELECT for the connector role ``postgres`` only; nothing for anon or authenticated."""
    fns = [f"{S}.ingest(text, jsonb, text)"]
    rels = [INBOX, f"{S}.ingest_deliveries", f"{S}.ingest_seen", f"{S}.v_query_plan_sheet", f"{S}.v_watchlist_sheet", f"{S}.v_claims_open"]
    revoke = "\n".join(
        f"  if exists (select 1 from pg_roles where rolname = {_lit(role)}) then\n"
        + "".join(f"    revoke all on function {f} from {role};\n" for f in fns)
        + "".join(f"    revoke all on {r} from {role};\n" for r in rels)
        + "  end if;" for role in ("anon", "authenticated"))
    grant = ("  if exists (select 1 from pg_roles where rolname = 'postgres') and current_user <> 'postgres' then\n"
             f"    grant usage on schema {S} to postgres;\n"  # do not rely on postgres being a member of x_lake
             + "".join(f"    grant execute on function {f} to postgres;\n" for f in fns)
             + "".join(f"    grant select on {r} to postgres;\n" for r in rels[3:])  # the views only
             + "  end if;")
    return f"do $$ begin\n{revoke}\n{grant}\nend $$;\n"


def inbox_ddl() -> str:
    """Idempotent: safe to run on every inbox-mode cron run. Needs the curated tables (postgres_ddl)."""
    parts = [_helpers(), _inbox_table()]
    parts += [_upsert_fn(c) for c in CURATED.values()]
    parts += [_ingest_fn(), _views(), _grants()]
    return "\n".join(parts)


def ensure_inbox(conn: Any) -> None:
    from . import curated as cur

    cur.ensure_schema(conn)
    with conn.cursor() as c:
        c.execute(inbox_ddl())
    conn.commit()


# ------------------------------------------------------------------------------------- drain (cron)

def try_lock(conn: Any) -> bool:
    """Session-level advisory lock: one drainer at a time. Released when ``conn`` closes."""
    with conn.cursor() as c:
        c.execute("select pg_try_advisory_lock(%s)", (DRAIN_LOCK,))
        ok = bool(c.fetchone()[0])
    conn.commit()
    return ok


def undrained(conn: Any) -> list[tuple[int, str, dict[str, Any], datetime]]:
    with conn.cursor() as c:
        c.execute(f"select id, dataset, row, received_at from {INBOX} where drained_at is null order by id")
        rows = [(int(i), str(d), dict(r), ra) for i, d, r, ra in c.fetchall()]
    conn.commit()
    return rows


def drain(conn: Any, *, store: Any, prefix: str, cfg: Any = None, now: datetime | None = None) -> dict[str, Any]:
    """Undrained inbox rows -> append_rows (R2 raw + curated) -> mark drained. Counts only in the result.

    Config datasets (no raw) are NOT upserted again: ingest() already applied them, and re-applying a
    snapshot read here could roll back an edit a bot made while the drain ran. They are only typed;
    a config row with a coercion error stays undrained (never pruned) and is reported until a
    corrected row replaces it.
    """
    from .append import append_rows
    from .coerce import type_row

    now = now or datetime.now(timezone.utc)
    pending = undrained(conn)
    if not pending:
        return {"rows": 0, "marked": 0, "superseded": 0, "unmarked": 0, "datasets": {}, "batch_id": None,
                "config_coerce_errors": {}}
    by_ds: dict[str, list[dict[str, Any]]] = defaultdict(list)
    done: list[tuple[int, datetime]] = []
    config_errors: dict[str, int] = defaultdict(int)
    for rid, ds, row, ra in pending:
        if DATASETS[ds].raw:
            by_ds[ds].append(row)
            done.append((rid, ra))
        elif type_row(DATASETS[ds], row).errors:
            config_errors[ds] += 1
        else:
            done.append((rid, ra))
    report: dict[str, Any] = {"batch_id": f"drain-{now:%Y%m%dT%H%M%SZ}", "raw": {}, "curated": {}, "coerce_errors": {}}
    if by_ds:
        report = append_rows(dict(by_ds), cfg=cfg, store=store, prefix=prefix, conn=conn, now=now)
    with conn.cursor() as c:
        # Matching received_at too: a config row replaced after it was read stays undrained.
        c.execute(f"update {INBOX} i set drained_batch_id = %s, drained_at = %s "
                  "from unnest(%s::bigint[], %s::timestamptz[]) u(id, ra) "
                  "where i.id = u.id and i.received_at = u.ra and i.drained_at is null",
                  (report["batch_id"], now, [d[0] for d in done], [d[1] for d in done]))
        marked = c.rowcount
        # A config row ingest() replaced while the drain ran is superseded, not lost: its newer
        # version stays undrained for the next run (the received_at check above skipped it).
        c.execute(f"select count(*) from {INBOX} i join unnest(%s::bigint[], %s::timestamptz[]) u(id, ra) "
                  "on i.id = u.id where i.drained_at is null and i.received_at <> u.ra",
                  ([d[0] for d in done], [d[1] for d in done]))
        superseded = int(c.fetchone()[0])
    conn.commit()
    counts: dict[str, int] = defaultdict(int)
    for _, ds, _, _ in pending:
        counts[ds] += 1
    return {"rows": len(pending), "expected": len(done), "marked": marked, "superseded": superseded,
            "unmarked": len(done) - marked - superseded, "batch_id": report["batch_id"],
            "datasets": dict(counts), "raw": report["raw"], "curated": report["curated"],
            "coerce_errors": report["coerce_errors"], "config_coerce_errors": dict(config_errors)}


def reapply_config(conn: Any, since: datetime) -> dict[str, Any]:
    """Re-apply config rows the bots sent since ``since`` (X_LAKE_SOURCE=both, right after the Sheet
    catch-up). The catch-up upserts the Sheet as it read it; a bot edit that reached the Sheet and the
    inbox after that read would otherwise be rolled back. Newest received row per key wins."""
    out: dict[str, Any] = {}
    with conn.cursor() as c:
        # Same lock as ingest() for config rows: no bot edit can land between the read and the upsert.
        c.execute("select pg_advisory_xact_lock(%s)", (CONFIG_LOCK,))
        for n in REPLACEABLE:
            c.execute(f"select coalesce(jsonb_agg(row order by received_at, id), '[]'::jsonb) from {INBOX} "
                      "where dataset = %s and received_at >= %s", (n, since))
            rows = c.fetchone()[0]
            if rows:
                c.execute(f"select {S}._ingest_upsert_{n}(%s::jsonb, %s)",
                          (json.dumps(rows), f"reapply-{since:%Y%m%dT%H%M%SZ}"))
                out[n] = c.fetchone()[0]
    conn.commit()
    return out


def landed(conn: Any, store: Any, prefix: str, since: datetime) -> dict[str, int]:
    """Raw datasets: inbox rows drained since ``since`` whose row_key is missing from R2 (must be 0)."""
    import io

    import pyarrow.parquet as pq

    from .lake import PART, _join

    with conn.cursor() as c:
        c.execute(f"select dataset, row_key from {INBOX} where drained_at >= %s", (since,))
        keys: dict[str, set[str]] = defaultdict(set)
        for ds, rk in c.fetchall():
            if DATASETS[ds].raw:
                keys[ds].add(rk)
    conn.commit()
    out = {}
    for ds, want in keys.items():
        have: set[str] = set()
        for key in store.list(_join(prefix, "raw", ds) + "/"):
            if PART.search(key):
                have.update(pq.read_table(io.BytesIO(store.read(key)), columns=["_row_key"])
                            .column("_row_key").to_pylist())
        out[ds] = len(want - have)
    return out


def prune(conn: Any, now: datetime | None = None) -> int:
    now = now or datetime.now(timezone.utc)
    with conn.cursor() as c:
        c.execute("select pg_advisory_xact_lock(%s)", (PRUNE_LOCK,))  # no raw ingest() mid-flight
        c.execute(f"insert into {S}.ingest_seen (dataset, row_key, payload_md5) "
                  f"select dataset, row_key, {S}.payload_md5(row) from {INBOX} "
                  f"where drained_at < %s and dataset = any(%s) on conflict do nothing",
                  (now - KEEP_DRAINED, list(SEEN_GUARDED)))
        c.execute(f"delete from {INBOX} where drained_at < %s", (now - KEEP_DRAINED,))
        n = c.rowcount
        c.execute(f"delete from {S}.ingest_deliveries where received_at < %s", (now - KEEP_DELIVERIES,))
    conn.commit()
    return n


def stored_dates(store: Any, prefix: str) -> dict[str, dict[str, Any]]:
    """row_key -> raw partition date, for raw datasets without a timestamp column of their own
    (x_analyses): the date a row first landed is its only usable date."""
    from .lake import read_dataset

    out: dict[str, dict[str, Any]] = {}
    for n, ds in DATASETS.items():
        if ds.raw and ds.ingest_ts is None and n != "x_post_context":  # context is dated by its post
            tbl = read_dataset(store, prefix, ds)
            out[n] = ({} if tbl is None else
                      dict(zip(tbl.column("_row_key").to_pylist(), tbl.column("_partition_date").to_pylist())))
    return out


def compare(conn: Any, sheet_typed: dict[str, list[Any]], window_start: datetime, now: datetime,
            grace: timedelta = timedelta(minutes=60), *, snapshot_at: datetime | None = None,
            stored_on: dict[str, dict[str, Any]] | None = None) -> dict[str, dict[str, int]]:
    """Dual-write check (X_LAKE_SOURCE=both): what reached the inbox must match what reached the Sheet.

    missing_in_sheet: inbox rows received from ``window_start`` until the Sheet was read
    (``snapshot_at``, default ``now``; the bots write the Sheet first, so a row received after the
    read may legitimately be missing from it) whose key the Sheet does not hold (claims compare
    claim_id, since the Sheet keeps only a claim's latest version).
    missing_in_inbox: raw-dataset Sheet rows dated from ``window_start`` to ``now - grace`` (a bot
    run still writing is not a miss) that never reached the inbox. Datasets without a timestamp
    column use the date the row first landed in raw (``stored_on``; a row not in raw yet is due).
    """
    from .pipeline import feed_index, partition_values

    upper_received = (snapshot_at or now) - timedelta(minutes=1)  # database vs cron clock skew
    with conn.cursor() as c:
        c.execute(f"select dataset, row_key, row, received_at from {INBOX}")
        inbox = c.fetchall()
    conn.commit()
    feed_ts = {k: v.get("collected_at") for k, v in feed_index(sheet_typed.get("x_feed", [])).items()}
    out: dict[str, dict[str, int]] = {}
    for n, ds in DATASETS.items():
        rows = sheet_typed.get(n, [])
        sheet_keys = {r.row_key for r in rows}
        mine = [(rk, row, ra) for d, rk, row, ra in inbox if d == n]
        all_inbox = {rk for rk, _, _ in mine}
        recent = [(rk, row) for rk, row, ra in mine if window_start <= ra <= upper_received]
        c = CURATED.get(n)
        # Only Claims are rewritten in place in the Sheet (one row per claim); every other dataset,
        # x_signals included, keeps each version as its own Sheet row, so it is compared by row key.
        if c is not None and n == "claims":
            from .coerce import type_row

            sheet_pks = {tuple(r.values.get(k) for k in c.pk) for r in rows}
            miss_sheet = sum(1 for _, row in recent
                             if tuple(type_row(ds, row).values.get(k) for k in c.pk) not in sheet_pks)
        else:
            miss_sheet = sum(1 for rk, _ in recent if rk not in sheet_keys)
        rep = {"inbox_rows": len(recent), "missing_in_sheet": miss_sheet}
        if ds.raw:
            if ds.ingest_ts is None and n != "x_post_context":
                landed_on = (stored_on or {}).get(n, {})
                first_day = window_start.astimezone(TZ_ICT).date()
                due = [r for r in rows if r.row_key not in landed_on or landed_on[r.row_key] >= first_day]
            else:
                upper = now - grace
                ts = partition_values(ds, rows, feed_ts)
                due = [r for r, t in zip(rows, ts) if isinstance(t, datetime) and window_start <= t <= upper]
            rep["sheet_rows"] = len(due)
            rep["missing_in_inbox"] = sum(1 for r in due if r.row_key not in all_inbox)
        out[n] = rep
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--print-ddl", action="store_true")
    g.add_argument("--apply", action="store_true", help="apply curated + inbox DDL to X_LAKE_DB_URL")
    args = p.parse_args(argv)
    if args.print_ddl:
        print(inbox_ddl())
        return 0
    from . import curated as cur
    from .config import Config

    conn = cur.connect(Config().require("X_LAKE_DB_URL"))
    try:
        ensure_inbox(conn)
    finally:
        conn.close()
    print("applied: x_intel.ingest_inbox, x_intel.ingest(), read views, grants")
    return 0


__all__ = ["POSTGRES", "compare", "reapply_config", "stored_dates", "drain", "ensure_inbox", "inbox_ddl", "landed", "prune", "try_lock"]

if __name__ == "__main__":
    sys.exit(main())
