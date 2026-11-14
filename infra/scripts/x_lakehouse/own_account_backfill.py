#!/usr/bin/env python3
"""One-time backfill of ``x_own_account_daily`` from an external history CSV (never committed).

The CSV has fewer columns than the contract: ``date, handle, user_id, followers, following, posts,
views_24h, likes_24h, replies_24h``. Mapping: ``date`` -> ``snapshot_date``, ``source = 'backfill_csv'``,
every other contract column stays NULL (``snapshot_at``, ``listed``, ``followers_delta``, ``posts_24h``,
``reposts_24h``, ``mentions_24h``): they are not derived or invented here. Dates missing from the
file (e.g. 2026-11-02..09) stay missing: no row is synthesised.

The rows go through ``x_intel_private.ingest()`` like any bot write, so the Railway cron stays the only
R2 writer and a rerun is a no-op (same run_id, same keys). Output carries counts and dates only.

DRY RUN BY DEFAULT:
    python -m x_lakehouse.own_account_backfill /path/history.csv               # counts, nothing written
    python -m x_lakehouse.own_account_backfill /path/history.csv --sql-out DIR # SQL files for the bot box
    python -m x_lakehouse.own_account_backfill /path/history.csv --execute     # ingest via X_LAKE_DB_URL
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

from .schemas import X_OWN_ACCOUNT_DAILY  # noqa: E402

CSV_COLUMNS = ("date", "handle", "user_id", "followers", "following", "posts", "views_24h", "likes_24h",
               "replies_24h")
RENAME = {"date": "snapshot_date"}
SOURCE = "backfill_csv"
DEFAULT_RUN_ID = "backfill-csv-own-account"


class BackfillError(ValueError):
    pass


def read_rows(path: Path) -> list[dict[str, Any]]:
    """CSV -> contract rows (blank cells left out, so they land as NULL)."""
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        header = [h.strip() for h in (reader.fieldnames or [])]
        unknown = [h for h in header if h and h not in CSV_COLUMNS]
        missing = [h for h in ("date", "handle", "user_id") if h not in header]
        if unknown or missing:
            raise BackfillError(f"CSV header: unknown columns {unknown}, missing {missing}")
        rows = []
        for raw in reader:
            rec: dict[str, Any] = {"source": SOURCE}
            for k, v in raw.items():
                k = (k or "").strip()
                v = (v or "").strip()
                if k and v:
                    rec[RENAME.get(k, k)] = v
            rows.append(rec)
    contract = set(X_OWN_ACCOUNT_DAILY.names)
    for rec in rows:
        assert set(rec) <= contract, sorted(set(rec) - contract)
    return rows


def summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    days = sorted({date.fromisoformat(str(r["snapshot_date"])[:10]) for r in rows if r.get("snapshot_date")})
    gaps: list[str] = []
    for a, b in zip(days, days[1:]):
        d = a + timedelta(days=1)
        while d < b:
            gaps.append(d.isoformat())
            d += timedelta(days=1)
    return {"rows": len(rows), "handles": len({r.get("handle") for r in rows}),
            "first_date": days[0].isoformat() if days else None, "last_date": days[-1].isoformat() if days else None,
            "dates": len(days), "missing_dates": gaps,
            "null_columns": [c for c in X_OWN_ACCOUNT_DAILY.names if all(c not in r for r in rows)]}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("csv", type=Path)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--execute", action="store_true", help="call x_intel_private.ingest() via X_LAKE_DB_URL")
    g.add_argument("--sql-out", type=Path, help="write bot_sql files for the Supabase connector instead")
    g.add_argument("--dry-run", action="store_true", help="the default; accepted for clarity")
    p.add_argument("--run-id", default=DEFAULT_RUN_ID)
    args = p.parse_args(argv)
    rows = read_rows(args.csv)
    out: dict[str, Any] = {"mode": "execute" if args.execute else "sql" if args.sql_out else "dry-run", **summary(rows)}
    if args.sql_out:
        from . import bot_sql

        out["files"] = len(bot_sql.write_files(X_OWN_ACCOUNT_DAILY.name, rows, args.run_id, args.sql_out))
    elif args.execute:
        from . import curated as cur
        from .config import Config

        conn = cur.connect(Config().require("X_LAKE_DB_URL"))
        try:
            with conn.cursor() as c:
                c.execute("select x_intel_private.ingest(%s, %s::jsonb, %s)",
                          (X_OWN_ACCOUNT_DAILY.name, json.dumps(rows, ensure_ascii=False), args.run_id))
                res = c.fetchone()[0]
            conn.commit()
        finally:
            conn.close()
        out |= {"inserted": res["inserted"], "skipped": res["skipped"], "errors": len(res["errors"])}
    print(json.dumps(out, ensure_ascii=False))
    return 1 if out.get("errors") else 0


if __name__ == "__main__":
    sys.exit(main())
