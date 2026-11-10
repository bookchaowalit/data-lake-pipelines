"""Stable X Watchlist row numbers for the Sheet cutover (plan §11 Phase 2, open item "watchlist_rows").

``x_query_plan.watchlist_rows`` (102 of 218 rows on 2026-11-10) and ``x_account_snapshots.watchlist_row`` point at
X Watchlist entries by their **Sheet row number**. Once Chief Data reads the watchlist from
``x_intel.v_watchlist_sheet`` instead of the tab, those numbers must keep naming the same entries, so:

- ``x_intel.watchlist_row_map`` freezes ``row_no`` per (requesting_chief, type, value), seeded once from the live tab
  (``--seed --execute``; a dry run by default prints counts only);
- an AFTER INSERT trigger on ``x_watchlist`` gives every new entry the next number, as an append to the tab would
  (only once the map is seeded, so it cannot take a number the tab already uses);
- ``v_watchlist_sheet`` lists rows in that order and adds the column ``sheet_row``.

The dry run is also the check while the Sheet is still written: ``number_conflicts`` and ``moved`` must be 0.

    python -m x_lakehouse.watchlist_rows --seed            # dry run against the Sheet + Postgres (read-only)
    python -m x_lakehouse.watchlist_rows --seed --execute  # owner OK: writes the map (idempotent)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

from .schemas import PG_SCHEMA  # noqa: E402

S = PG_SCHEMA
MAP = f"{S}.watchlist_row_map"
KEY = ("requesting_chief", "type", "value")
FIRST_DATA_ROW = 2  # row 1 is the header


def ddl() -> str:
    """Idempotent; part of inbox_ddl (runs on every inbox-mode cron run)."""
    return f"""
create table if not exists {MAP} (
    row_no integer primary key,
    requesting_chief text not null,
    type text not null,
    value text not null,
    origin text not null default 'trigger',   -- 'sheet' (seeded from the tab) or 'trigger' (new entry)
    assigned_at timestamptz not null default now(),
    unique (requesting_chief, type, value)
);

create or replace function {S}.assign_watchlist_row() returns trigger language plpgsql as $fn$
begin
    if not exists (select 1 from {MAP}) then
        return null;  -- not seeded yet: numbering starts from the tab, never from here
    end if;
    perform pg_advisory_xact_lock(hashtext('{MAP}'));
    insert into {MAP} (row_no, requesting_chief, type, value)
    select coalesce(max(row_no), {FIRST_DATA_ROW - 1}) + 1, new.requesting_chief, new.type, new.value from {MAP}
    on conflict (requesting_chief, type, value) do nothing;
    return null;
end
$fn$;

create or replace trigger trg_assign_watchlist_row after insert on {S}.x_watchlist
    for each row execute function {S}.assign_watchlist_row();
"""


def plan_seed(sheet: Iterable[tuple[int, tuple[str, str, str]]], mapped: dict[tuple[str, str, str], int]) -> dict[str, Any]:
    """Compare the tab (sheet row, key) with the map. Never renumbers: a key whose number differs is ``moved``,
    a number already held by another key is a ``number_conflict``; both block the seed."""
    by_no = {n: k for k, n in mapped.items()}
    to_insert: list[tuple[int, tuple[str, str, str]]] = []
    moved, conflicts, dup_keys, seen = [], [], [], set()
    for row_no, key in sheet:
        if key in seen:
            dup_keys.append(row_no)
            continue
        seen.add(key)
        if key in mapped:
            if mapped[key] != row_no:
                moved.append({"key_row": row_no, "mapped": mapped[key]})
        elif row_no in by_no:
            conflicts.append(row_no)
        else:
            to_insert.append((row_no, key))
    return {"sheet_rows": len(seen) + len(dup_keys), "mapped_before": len(mapped), "to_insert": to_insert,
            "moved": moved, "number_conflicts": conflicts, "duplicate_keys_in_sheet": dup_keys}


def _key(values: dict[str, Any]) -> tuple[str, str, str] | None:
    k = tuple(values.get(c) for c in KEY)
    return None if any(v in (None, "") for v in k) else tuple(str(v) for v in k)  # type: ignore[return-value]


def sheet_keys(typed_rows: list[Any]) -> list[tuple[int, tuple[str, str, str]]]:
    out = []
    for r in typed_rows:
        key = _key(r.values)
        if key is not None and r.sheet_row is not None:
            out.append((int(r.sheet_row), key))
    return sorted(out)


def read_map(conn: Any) -> dict[tuple[str, str, str], int]:
    with conn.cursor() as c:
        c.execute(f"select requesting_chief, type, value, row_no from {MAP}")
        rows = {(a, b, v): n for a, b, v, n in c.fetchall()}
    conn.commit()
    return rows


def seed(conn: Any, sheet: list[tuple[int, tuple[str, str, str]]], *, execute: bool) -> dict[str, Any]:
    """Dry run unless ``execute``. Inserts the tab's numbers, then numbers any curated entry the tab lacks after the
    highest one (in key order), all in one transaction. Refuses (nothing written) on any moved or conflicting row."""
    plan = plan_seed(sheet, read_map(conn))
    report = {k: (len(v) if isinstance(v, list) else v) for k, v in plan.items()}
    blocked = bool(plan["moved"] or plan["number_conflicts"] or plan["duplicate_keys_in_sheet"])
    report["blocked"] = blocked
    with conn.cursor() as c:
        c.execute(f"select count(*) from {S}.x_watchlist w where not exists (select 1 from {MAP} m "
                  "where m.requesting_chief = w.requesting_chief and m.type = w.type and m.value = w.value)")
        report["curated_unmapped_before"] = int(c.fetchone()[0])
    conn.commit()
    if not execute or blocked:
        report["written"] = 0
        return report
    with conn.cursor() as c:
        c.execute("select pg_advisory_xact_lock(hashtext(%s))", (MAP,))
        for row_no, (chief, typ, value) in plan["to_insert"]:
            c.execute(f"insert into {MAP} (row_no, requesting_chief, type, value, origin) values (%s, %s, %s, %s, 'sheet')"
                      " on conflict do nothing", (row_no, chief, typ, value))
        c.execute(f"""insert into {MAP} (row_no, requesting_chief, type, value, origin)
            select (select coalesce(max(row_no), {FIRST_DATA_ROW - 1}) from {MAP})
                   + row_number() over (order by w.requesting_chief, w.type, w.value), w.requesting_chief, w.type, w.value,
                   'trigger'
            from {S}.x_watchlist w where not exists (select 1 from {MAP} m where m.requesting_chief = w.requesting_chief
                and m.type = w.type and m.value = w.value)""")
        report["curated_numbered_after_tab"] = c.rowcount
    conn.commit()
    report["written"] = len(plan["to_insert"]) + report["curated_numbered_after_tab"]
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--seed", action="store_true", required=True)
    ap.add_argument("--execute", action="store_true", help="write the map (owner OK); default is a dry run")
    args = ap.parse_args(argv)
    from . import backfill
    from . import curated as cur
    from .config import Config
    from .inbox import ensure_inbox

    cfg = Config()
    typed, checks, missing = backfill.load(cfg, ["x_watchlist"])
    if missing or "x_watchlist" not in typed:
        print(json.dumps({"status": "failed", "error": "X Watchlist tab missing"}))
        return 2
    conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
    try:
        if args.execute:
            ensure_inbox(conn)  # creates the map, trigger and view (same DDL the cron applies)
        else:
            with conn.cursor() as c:
                c.execute("select to_regclass(%s)", (MAP,))
                exists = c.fetchone()[0] is not None
            conn.commit()
            if not exists:  # dry run before the DDL exists: compare against an empty map
                sheet = sheet_keys(typed["x_watchlist"])
                plan = plan_seed(sheet, {})
                print(json.dumps({"status": "dry_run", "map_exists": False,
                                  **{k: (len(v) if isinstance(v, list) else v) for k, v in plan.items()}}))
                return 0
        report = seed(conn, sheet_keys(typed["x_watchlist"]), execute=args.execute)
    finally:
        conn.close()
    print(json.dumps({"status": "executed" if args.execute else "dry_run", **report}))
    return 1 if report["blocked"] else 0


if __name__ == "__main__":
    sys.exit(main())
