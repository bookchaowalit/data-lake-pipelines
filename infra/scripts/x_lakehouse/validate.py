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
from .pipeline import DatasetCheck, feed_index, partition_values, validate_rows  # noqa: E402
from .schemas import DATASETS, X_FEED, curated_for  # noqa: E402


def reconcile(cfg: Config, names: list[str], since: date | None, lake_uri: str | None, *,
              sheet: bool = True, preloaded: tuple[Any, Any, list[str]] | None = None) -> dict[str, Any]:
    """``sheet=False``: raw vs Postgres only (after cutover the Sheet no longer grows).
    ``preloaded``: the ``backfill.load`` result, to reuse one Sheet read.
    Datasets without a Sheet tab are always checked raw vs Postgres only. Postgres rows past a table's
    retention are not expected (curated.prune_retention deleted them; raw keeps them)."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    if not sheet:
        typed, checks, missing_tabs = {}, {n: DatasetCheck(n, DATASETS[n].tab) for n in names}, []
    elif preloaded is not None:
        typed, checks, missing_tabs = preloaded
        checks = {n: DatasetCheck(n, DATASETS[n].tab, missing_columns=list(checks[n].missing_columns))
                  for n in names}  # fresh counters: validate_rows below fills them
    else:
        typed, checks, missing_tabs = load(cfg, names)
    store, prefix = open_store(cfg, lake_uri)
    conn = cur.connect(cfg.db_url) if cfg.db_url else None
    feed_ts = {k: v.get("collected_at") for k, v in feed_index(typed.get(X_FEED.name, [])).items()}
    out: dict[str, Any] = {"missing_tabs": missing_tabs, "datasets": {}}
    try:
        for n in names:
            ds = DATASETS[n]
            rows = typed.get(n, [])
            stored_on: dict[str, date] = {}
            if since and ds.raw and ds.ingest_ts is None:
                # No timestamp column: scope by the partition each row was actually stored in.
                # Rows not in raw at all stay in scope, so they are reported as missing.
                full = read_dataset(store, prefix, ds)
                if full is not None:
                    stored_on = dict(zip(full.column("_row_key").to_pylist(),
                                         full.column("_partition_date").to_pylist()))
                rows = [r for r in rows if r.row_key not in stored_on or stored_on[r.row_key] >= since]
            elif since:
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
                if tbl is not None and "post_id" in ds.kinds and n not in ("x_post_context", "x_analyses"):
                    ids = tbl.column("post_id")
                    rep["raw_duplicate_post_ids"] = len(ids) - ids.null_count - len(pc.unique(ids.drop_null()))
                if tbl is not None:
                    rep["raw_rows_with_coerce_errors"] = tbl.num_rows - tbl.column("_coerce_errors").null_count
            c = curated_for(n)
            if conn is not None and c is not None:
                # Expected keys: the scoped Sheet rows plus, for raw-backed tables, the scoped raw rows
                # (after cutover the lake is the only writer, so raw is what Postgres must match).
                cut = cur.retention_cut(c, now)
                vcols = [x for x in (c.version, c.version_also, c.protect[0] if c.protect else None, c.retain_col) if x]
                cand = [(tuple(r.values.get(k) for k in c.pk), {x: r.values.get(x) for x in vcols}) for r in rows]
                if ds.raw and tbl is not None:
                    cols = {x: tbl.column(x).to_pylist() for x in vcols}
                    cand += [(pk, {x: cols[x][i] for x in vcols})
                             for i, pk in enumerate(zip(*(tbl.column(k).to_pylist() for k in c.pk)))]
                cand = [(pk, v) for pk, v in cand if all(x is not None for x in pk)]
                if c.version:
                    # Retention is judged on each key's newest version: a bookmark pruned 180 days after its
                    # removal still has older active versions in raw, and those must not be expected.
                    latest: dict[tuple[Any, ...], dict[str, Any]] = {}
                    for pk, v in cand:
                        k_new, k_old = cur.version_key(c, v), cur.version_key(c, latest.get(pk, {}))
                        if pk not in latest or (k_new is not None and (k_old is None or k_new >= k_old)):
                            latest[pk] = v
                    judged = list(latest.items())
                else:
                    judged = cand
                expected = {pk for pk, v in judged
                            if cut is None or v.get(c.retain_col) is None or v[c.retain_col] >= cut}
                with conn.cursor() as q:
                    q.execute(f"select {', '.join(c.pk)} from {c.qualified}")
                    stored = {tuple(row) for row in q.fetchall()}
                pg_missing = sorted(expected - stored)
                rep["postgres_rows"] = len(stored)
                rep["postgres_missing_keys"] = len(pg_missing)
                rep["postgres_missing_sample"] = [list(k) for k in pg_missing[:5]]
                if c.version:
                    # A newer version can reach raw while its upsert fails: the key exists, the row is stale.
                    gcol = c.protect[0] if c.protect else c.version
                    pairs = [(pk, cur.version_key(c, v), v.get(gcol)) for pk, v in cand]
                    pairs = [(pk, k, g) for pk, k, g in pairs if k is not None]
                    sel = [c.version] + ([c.version_also] if c.version_also else [])
                    with conn.cursor() as q:
                        q.execute(f"select {', '.join(c.pk)}, {gcol}, {', '.join(sel)} from {c.qualified}")
                        stored_rows = [(tuple(row[:len(c.pk)]), row[len(c.pk)],
                                        cur.version_key(c, dict(zip(sel, row[len(c.pk) + 1:]))))
                                       for row in q.fetchall()]
                    stored_v = {pk: v for pk, _, v in stored_rows}
                    # Protected claims: once Book checked a claim (stored or any version), only Book's
                    # versions are eligible; the newest eligible one is what Postgres must hold.
                    prot = cur.protected_pks(c, [(pk, g) for pk, g, _ in stored_rows] + [(pk, g) for pk, _, g in pairs])
                    newest: dict[tuple[Any, ...], Any] = {}
                    held: set[tuple[Any, ...]] = set()
                    for pk, v, g in pairs:
                        if pk in prot and not cur.is_protected_value(c, g):
                            held.add(pk)
                            continue
                        if pk not in newest or v > newest[pk]:
                            newest[pk] = v
                    stale = sorted(pk for pk, v in newest.items()
                                   if pk in stored_v and (stored_v[pk] is None or stored_v[pk] < v))
                    rep["postgres_protected_held"] = len(held)
                    rep["postgres_stale_versions"] = len(stale)
                    rep["postgres_stale_sample"] = [list(k) for k in stale[:5]]
                if c.pk == ("post_id",):
                    with conn.cursor() as q:
                        q.execute(f"select count(*) - count(distinct post_id) from {c.qualified}")
                        rep["postgres_duplicate_post_ids"] = int(q.fetchone()[0])
            chk = checks[n]
            reasons = []
            if n in missing_tabs or DATASETS[n].tab in missing_tabs:
                reasons.append("tab not found")
            if chk.missing_columns:
                reasons.append(f"missing columns {chk.missing_columns}")
            if chk.rows_with_errors:
                reasons.append(f"{chk.rows_with_errors} Sheet rows with coercion errors {chk.coerce_errors}")
            if rep.get("raw_rows_with_coerce_errors"):  # after cutover raw may hold rows the Sheet never had
                reasons.append(f"{rep['raw_rows_with_coerce_errors']} raw rows with coercion errors")
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
            if rep.get("postgres_stale_versions"):
                reasons.append(f"{rep['postgres_stale_versions']} Postgres rows older than raw/Sheet "
                               f"({c.version})")
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
    p.add_argument("--raw-only", action="store_true",
                   help="check Sheet vs raw only; Postgres is reported as not checked")
    args = p.parse_args(argv)
    cfg = Config()
    names = [n for n in args.datasets.split(",") if n]
    if not args.raw_only and not cfg.db_url and any(curated_for(n) for n in names):
        p.error("set X_LAKE_DB_URL to check the curated layer, or pass --raw-only")
    if args.raw_only:
        cfg.env = {k: v for k, v in cfg.env.items() if k != "X_LAKE_DB_URL"}
    report = reconcile(cfg, names, args.since, args.lake_uri)
    if args.raw_only:
        print("raw-only: Postgres NOT checked")
    print(f"{'dataset':22} {'sheet':>7} {'raw':>7} {'missing':>7} {'pg':>7} {'pgMiss':>7} {'dupPost':>7} {'coerceErr':>9}  ok")
    for n, r in report["datasets"].items():
        dup = max(r.get("duplicate_post_ids_sheet") or 0, r.get("raw_duplicate_post_ids") or 0,
                  r.get("postgres_duplicate_post_ids") or 0)
        print(f"{n:22} {r['sheet_rows']:>7} {r.get('raw_rows', '-'):>7} {r.get('sheet_rows_missing_from_raw', '-'):>7} "
              f"{r.get('postgres_rows', '-'):>7} {r.get('postgres_missing_keys', '-'):>7} {dup:>7} "
              f"{max(r['coerce_error_rows'], r.get('raw_rows_with_coerce_errors') or 0):>9}  {'ok' if r['ok'] else 'FAIL: ' + '; '.join(r['reasons'])}")
    print(json.dumps({"ok": report["ok"], "missing_tabs": report["missing_tabs"]}))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
