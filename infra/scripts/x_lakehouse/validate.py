#!/usr/bin/env python3
"""Reconcile Sheet vs raw Parquet vs Postgres. Read-only everywhere.

Per dataset: Sheet rows, raw rows and distinct ``_row_key``s, Postgres rows; Sheet keys missing
from raw; duplicate post_id in Sheet / raw / curated; null rates; rows carrying coercion errors.
Exit 1 on any failure (missing tab/column, coercion errors, duplicate keys, Sheet rows missing
from raw, raw or Sheet keys missing from Postgres), with the reasons — the daily parallel-run check.

    python -m x_lakehouse.validate                 # uses X_LAKE_URI and X_LAKE_DB_URL
    python -m x_lakehouse.validate --since 2026-11-05   # only Sheet rows ingested on/after a date
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

import pyarrow.compute as pc  # noqa: E402

from . import curated as cur  # noqa: E402
from .backfill import load  # noqa: E402
from .config import Config  # noqa: E402
from .lake import ingest_date, open_store, read_dataset  # noqa: E402
from .pipeline import feed_index, partition_values, validate_rows  # noqa: E402
from .schemas import CURATED, DATASETS, PG_SCHEMA, X_FEED  # noqa: E402


def reconcile(cfg: Config, names: list[str], since: date | None, lake_uri: str | None) -> dict[str, Any]:
    typed, checks, missing_tabs = load(cfg, names)
    store, prefix = open_store(cfg, lake_uri)
    conn = cur.connect(cfg.db_url) if cfg.db_url else None
    feed_ts = {k: v.get("collected_at") for k, v in feed_index(typed.get(X_FEED.name, [])).items()}
    out: dict[str, Any] = {"missing_tabs": missing_tabs, "datasets": {}}
    try:
        for n in names:
            ds = DATASETS[n]
            rows = typed.get(n, [])
            if since:
                parts = partition_values(ds, rows, feed_ts)
                rows = [r for r, ts in zip(rows, parts) if ingest_date(ts, date.max) >= since]
            validate_rows(ds, rows, checks[n], sample=0)
            rep: dict[str, Any] = {"sheet_rows": len(rows), "coerce_error_rows": checks[n].rows_with_errors,
                                   "duplicate_post_ids_sheet": checks[n].duplicate_post_ids,
                                   "null_rate": checks[n].null_rate}
            tbl = None
            if ds.raw:
                tbl = read_dataset(store, prefix, ds, since)
                raw_keys = set(tbl.column("_row_key").to_pylist()) if tbl is not None else set()
                rep["raw_rows"] = 0 if tbl is None else tbl.num_rows
                rep["raw_distinct_keys"] = len(raw_keys)
                rep["raw_duplicate_keys"] = rep["raw_rows"] - len(raw_keys)
                missing = [r.sheet_row for r in rows if r.row_key not in raw_keys]
                rep["sheet_rows_missing_from_raw"] = len(missing)
                rep["missing_sample_sheet_rows"] = missing[:10]
                if tbl is not None and "post_id" in ds.kinds and n != "x_post_context":
                    ids = tbl.column("post_id")
                    rep["raw_duplicate_post_ids"] = len(ids) - ids.null_count - len(pc.unique(ids.drop_null()))
                if tbl is not None:
                    rep["raw_rows_with_coerce_errors"] = tbl.num_rows - tbl.column("_coerce_errors").null_count
            if conn is not None and n in CURATED:
                c = CURATED[n]
                # Expected keys: the scoped Sheet rows plus, for raw-backed tables, the scoped raw rows
                # (after cutover the lake is the only writer, so raw is what Postgres must match).
                expected = {tuple(r.values.get(k) for k in c.pk) for r in rows}
                if ds.raw and tbl is not None:
                    expected |= set(zip(*(tbl.column(k).to_pylist() for k in c.pk)))
                expected = {k for k in expected if all(v is not None for v in k)}
                with conn.cursor() as q:
                    q.execute(f"select {', '.join(c.pk)} from {PG_SCHEMA}.{n}")
                    stored = {tuple(row) for row in q.fetchall()}
                pg_missing = sorted(expected - stored)
                rep["postgres_rows"] = len(stored)
                rep["postgres_missing_keys"] = len(pg_missing)
                rep["postgres_missing_sample"] = [list(k) for k in pg_missing[:5]]
                if "post_id" in c.pk:
                    with conn.cursor() as q:
                        q.execute(f"select count(*) - count(distinct post_id) from {PG_SCHEMA}.{n}")
                        rep["postgres_duplicate_post_ids"] = int(q.fetchone()[0])
            chk = checks[n]
            reasons = []
            if n in missing_tabs or DATASETS[n].tab in missing_tabs:
                reasons.append("tab not found")
            if chk.missing_columns:
                reasons.append(f"missing columns {chk.missing_columns}")
            if chk.rows_with_errors:
                reasons.append(f"{chk.rows_with_errors} rows with coercion errors {chk.coerce_errors}")
            if chk.duplicate_keys:
                reasons.append(f"{chk.duplicate_keys} duplicate natural keys in the Sheet")
            if chk.duplicate_post_ids:
                reasons.append(f"{chk.duplicate_post_ids} duplicate post_id in the Sheet")
            if rep.get("sheet_rows_missing_from_raw"):
                reasons.append(f"{rep['sheet_rows_missing_from_raw']} Sheet rows missing from raw")
            if rep.get("raw_duplicate_keys"):
                reasons.append(f"{rep['raw_duplicate_keys']} duplicate _row_key in raw")
            if rep.get("raw_duplicate_post_ids") and n == "x_feed":
                reasons.append(f"{rep['raw_duplicate_post_ids']} duplicate post_id in raw")
            if rep.get("postgres_missing_keys"):
                reasons.append(f"{rep['postgres_missing_keys']} keys missing from Postgres")
            if rep.get("postgres_duplicate_post_ids"):
                reasons.append(f"{rep['postgres_duplicate_post_ids']} duplicate post_id in Postgres")
            rep["reasons"] = reasons
            rep["ok"] = not reasons
            out["datasets"][n] = rep
    finally:
        if conn is not None:
            conn.close()
    out["ok"] = all(r["ok"] for r in out["datasets"].values()) and not missing_tabs
    return out


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--datasets", default=",".join(DATASETS))
    p.add_argument("--since", type=date.fromisoformat)
    p.add_argument("--lake-uri")
    args = p.parse_args(argv)
    report = reconcile(Config(), [n for n in args.datasets.split(",") if n], args.since, args.lake_uri)
    print(f"{'dataset':22} {'sheet':>7} {'raw':>7} {'missing':>7} {'pg':>7} {'pgMiss':>7} {'dupPost':>7} {'coerceErr':>9}  ok")
    for n, r in report["datasets"].items():
        dup = max(r.get("duplicate_post_ids_sheet") or 0, r.get("raw_duplicate_post_ids") or 0,
                  r.get("postgres_duplicate_post_ids") or 0)
        print(f"{n:22} {r['sheet_rows']:>7} {r.get('raw_rows', '-'):>7} {r.get('sheet_rows_missing_from_raw', '-'):>7} "
              f"{r.get('postgres_rows', '-'):>7} {r.get('postgres_missing_keys', '-'):>7} {dup:>7} "
              f"{r['coerce_error_rows']:>9}  {'ok' if r['ok'] else 'FAIL: ' + '; '.join(r['reasons'])}")
    print(json.dumps({"ok": report["ok"], "missing_tabs": report["missing_tabs"]}))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
