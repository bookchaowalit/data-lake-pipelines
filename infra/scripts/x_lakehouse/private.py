#!/usr/bin/env python3
"""Private datasets: the owner's own X account data, kept apart from everything the other bots and the
Sheet read. Spec: docs/specs/x-lake-private-datasets.md.

    x_own_account_daily  x_own_mentions  x_bookmarks  x_home_timeline

Layers, all separate from the public ones:

* bot door ``x_intel_private.ingest(dataset, rows jsonb, run_id)`` (SECURITY DEFINER, owned by the
  schema owner, EXECUTE for ``postgres`` only): the same row checks, row keys and retry rules as
  ``x_intel.ingest()``, phone numbers masked to "[phone]", rows stored in
  ``x_intel_private.ingest_inbox``. ``x_intel.ingest()`` refuses these four names and stores nothing.
* curated tables in schema ``x_intel_private`` (RLS on, no policy; nothing granted to any other role);
* raw Parquet under ``<prefix>/private/raw/<dataset>/`` (lake.dataset_root), never under ``raw/``;
* never served: not in serve_sheet, sheet_summaries, the marts, the digest or the BI site.

The cron (``scheduled.py``, inbox/both modes) drains the private inbox after the public one, checks that
every drained row reached R2, and after an ok run prunes the private inbox and applies retention.
Logs carry dataset names and counts only.

The schema itself is created by the owner (the lake role has no CREATE on the database):
``create schema x_intel_private authorization x_lake;``. Until it exists every private step is skipped
and logged as ``schema_missing``; nothing fails.

Limit: Supabase's ``postgres`` role usually bypasses RLS. If other bots connect as ``postgres`` they
may still be able to read this schema; see the spec for the owner's follow-up (a separate role).

    python -m x_lakehouse.private --print-ddl
    python -m x_lakehouse.private --apply          # X_LAKE_DB_URL; owner approval
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

from . import curated as cur  # noqa: E402
from . import inbox as ib  # noqa: E402
from .schemas import PG_SCHEMA, PRIVATE_CURATED, PRIVATE_DATASETS, PRIVATE_NAMES, PRIVATE_SCHEMA, table_ddl  # noqa: E402

P = PRIVATE_SCHEMA
S = PG_SCHEMA  # the shared, pure SQL helpers (row_key, _raw_text, _mask_row, payload_md5) live in x_intel
P_INBOX = f"{P}.ingest_inbox"
P_SEEN = f"{P}.ingest_seen"
# Roles that must never reach this schema. The lake role owns it; serve_sheet, the digest and the BI
# site connect with the lake role too, so for them the guard is code (deny lists + tests), not grants.
DENIED_ROLES = ("anon", "authenticated")


def _lit(t: str) -> str:
    return "'" + t.replace("'", "''") + "'"


def _arr(items: tuple[str, ...] | list[str]) -> str:
    return "array[" + ", ".join(_lit(i) for i in items) + "]::text[]"


def _tables() -> str:
    out = [f"""
create table if not exists {P_INBOX} (
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
create table if not exists {P_SEEN} (
    dataset text not null,
    row_key text not null,
    payload_md5 text not null,
    primary key (dataset, row_key)
);
create index if not exists ingest_inbox_undrained_idx on {P_INBOX} (id) where drained_at is null;
create index if not exists ingest_inbox_drained_at_idx on {P_INBOX} (drained_at) where drained_at is not null;
"""]
    for c in PRIVATE_CURATED.values():
        out += table_ddl(c)
    return "\n".join(out) + "\n"


def _ingest_fn() -> str:
    meta = [f"    when {_lit(n)} then cols := {_arr(d.names)}; keys := {_arr(d.key)}; "
            f"req := {_arr(d.required_columns)}; mask := {_arr(d.mask_phone)};"
            for n, d in PRIVATE_DATASETS.items()]
    case = ("  case p_dataset\n" + "\n".join(meta) + "\n    else\n" if meta else "  case\n    when false then null;\n    else\n")
    return f"""
create or replace function {P}.ingest(p_dataset text, p_rows jsonb, p_run_id text) returns jsonb
language plpgsql security definer set search_path = pg_catalog, {P} as $f$
declare
  cols text[]; keys text[]; req text[]; mask text[];
  r jsonb; i int := 0; n int; bad text[]; nested text[]; blank text[]; rk text; seen text;
  ins int := 0; skipped int := 0; errors jsonb := '[]'::jsonb;
begin
{case}      return jsonb_build_object('inserted', 0, 'skipped', 0, 'errors',
        jsonb_build_array(jsonb_build_object('row', null, 'error', 'unknown private dataset', 'dataset', p_dataset)));
  end case;
  if p_run_id is null or p_run_id !~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{{0,63}}$' then
    return jsonb_build_object('inserted', 0, 'skipped', 0, 'errors',
      jsonb_build_array(jsonb_build_object('row', null, 'error', 'run_id must match [A-Za-z0-9_.:-], 1-64 chars')));
  end if;
  if p_rows is null or jsonb_typeof(p_rows) <> 'array' then
    return jsonb_build_object('inserted', 0, 'skipped', 0, 'errors',
      jsonb_build_array(jsonb_build_object('row', null, 'error', 'rows must be a JSON array of objects')));
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
    blank := array(select k from unnest(req) k where {S}._raw_text(r -> k) = '');
    if cardinality(blank) > 0 then
      errors := errors || jsonb_build_object('row', i, 'error', 'key column is blank', 'columns', to_jsonb(blank));
      continue;
    end if;
    if cardinality(mask) > 0 then
      r := {S}._mask_row(r, mask);
    end if;
    rk := {S}.row_key(p_dataset, keys, r);
    select s.payload_md5 into seen from {P_SEEN} s where s.dataset = p_dataset and s.row_key = rk;
    if seen is not null then  -- drained and pruned earlier
      if seen <> {S}.payload_md5(r) then
        errors := errors || jsonb_build_object('row', i, 'error', 'key already stored with different values; send a new version');
      else
        skipped := skipped + 1;
      end if;
      continue;
    end if;
    insert into {P_INBOX} (dataset, run_id, row, row_key) values (p_dataset, p_run_id, r, rk)
    on conflict (dataset, row_key) do nothing;
    get diagnostics n = row_count;
    if n > 0 then
      ins := ins + 1;
    elsif exists (select 1 from {P_INBOX} x where x.dataset = p_dataset and x.row_key = rk and x.row is distinct from r) then
      errors := errors || jsonb_build_object('row', i, 'error', 'key already stored with different values; send a new version');
    else
      skipped := skipped + 1;
    end if;
  end loop;
  return jsonb_build_object('inserted', ins, 'skipped', skipped, 'errors', errors);
end $f$;
revoke all on function {P}.ingest(text, jsonb, text) from public;
"""


def _grants() -> str:
    tables = [P_INBOX, P_SEEN] + [c.qualified for c in PRIVATE_CURATED.values()]
    rls = "".join(f"alter table {t} enable row level security;\n" for t in tables)
    deny = "\n".join(
        f"  if exists (select 1 from pg_roles where rolname = {_lit(role)}) then\n"
        f"    revoke all on schema {P} from {role};\n"
        f"    revoke all on all tables in schema {P} from {role};\n"
        f"    revoke all on all sequences in schema {P} from {role};\n"
        f"    revoke all on all functions in schema {P} from {role};\n"
        f"  end if;" for role in DENIED_ROLES)
    # The bots' connector role may call the door, nothing else: no SELECT on any private table.
    bot = ("  if exists (select 1 from pg_roles where rolname = 'postgres') and current_user <> 'postgres' then\n"
           f"    grant usage on schema {P} to postgres;\n"
           f"    grant execute on function {P}.ingest(text, jsonb, text) to postgres;\n"
           "  end if;")
    return (f"revoke all on schema {P} from public;\n"
            f"revoke all on all tables in schema {P} from public;\n"
            f"revoke all on all functions in schema {P} from public;\n"
            + rls + f"do $$ begin\n{deny}\n{bot}\nend $$;\n")


def private_ddl() -> str:
    """Idempotent. Needs schema x_intel_private (owner-created) and the x_intel helpers (inbox_ddl)."""
    return _tables() + _ingest_fn() + _grants()


def schema_ready(conn: Any) -> bool:
    """True when schema x_intel_private exists and this role may create in it. If it is missing, try to
    create it (works for a role with CREATE on the database, e.g. the test superuser); the lake role
    cannot, so production waits for the owner and every private step is skipped."""
    with conn.cursor() as c:
        c.execute("select exists (select 1 from pg_namespace where nspname = %s)", (P,))
        exists = bool(c.fetchone()[0])
        if not exists:
            c.execute("select has_database_privilege(current_user, current_database(), 'CREATE')")
            if c.fetchone()[0]:
                c.execute(f"create schema {P}")
                exists = True
        ok = exists
        if exists:
            c.execute("select has_schema_privilege(current_user, %s, 'CREATE')", (P,))
            ok = bool(c.fetchone()[0])
    conn.commit()
    return ok


def ensure_private(conn: Any) -> bool:
    """Apply the private DDL when the schema is usable. True when the private datasets are in use.

    With X_LAKE_NEW_DATASETS=off the DDL is still applied, so x_intel_private.ingest() is regenerated
    to refuse every dataset (stored rows stay), and False is returned (no private drain)."""
    if not schema_ready(conn):
        return False
    with conn.cursor() as c:
        c.execute(private_ddl())
    conn.commit()
    return bool(PRIVATE_DATASETS)


def drain(conn: Any, *, store: Any, prefix: str, cfg: Any = None, now: datetime | None = None) -> dict[str, Any]:
    """Private inbox -> append_rows (private raw root + x_intel_private tables) -> mark drained."""
    return ib.drain(conn, store=store, prefix=prefix, cfg=cfg, now=now, inbox=P_INBOX, names=list(PRIVATE_DATASETS))


def landed(conn: Any, store: Any, prefix: str, since: datetime) -> dict[str, int]:
    return ib.landed(conn, store, prefix, since, inbox=P_INBOX)


def prune(conn: Any, now: datetime | None = None) -> dict[str, int]:
    """After an ok run: inbox rows drained more than 3 days ago (their payload hash kept in ingest_seen)
    and curated rows past retention. Idempotent; raw files are never deleted here."""
    now = now or datetime.now(timezone.utc)
    with conn.cursor() as c:
        c.execute(f"insert into {P_SEEN} (dataset, row_key, payload_md5) "
                  f"select dataset, row_key, {S}.payload_md5(row) from {P_INBOX} "
                  "where drained_at < %s on conflict do nothing", (now - ib.KEEP_DRAINED,))
        c.execute(f"delete from {P_INBOX} where drained_at < %s", (now - ib.KEEP_DRAINED,))
        inbox_rows = c.rowcount
    conn.commit()
    return {"inbox": inbox_rows, **cur.prune_retention(conn, list(PRIVATE_CURATED.values()), now)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--print-ddl", action="store_true")
    g.add_argument("--apply", action="store_true", help="apply the private DDL to X_LAKE_DB_URL")
    args = p.parse_args(argv)
    if args.print_ddl:
        print(private_ddl())
        return 0
    from .config import Config

    conn = cur.connect(Config().require("X_LAKE_DB_URL"))
    try:
        ib.ensure_inbox(conn)
        if not ensure_private(conn):
            print(f"schema {P} is missing or not writable: create it as the owner first "
                  f"(create schema {P} authorization x_lake;)", file=sys.stderr)
            return 1
    finally:
        conn.close()
    print(f"applied: {P_INBOX}, {P}.ingest(), {len(PRIVATE_CURATED)} private tables, RLS, grants")
    return 0


__all__ = ["P_INBOX", "PRIVATE_NAMES", "drain", "ensure_private", "landed", "private_ddl", "prune", "schema_ready"]

if __name__ == "__main__":
    sys.exit(main())
