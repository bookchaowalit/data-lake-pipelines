#!/usr/bin/env python3
"""One-time backfill: read every in-scope Sheet tab (read-only), validate, write raw Parquet to R2 and
upsert the curated Postgres tables.

DRY RUN BY DEFAULT. Without ``--execute`` nothing is written anywhere except, if you ask for it,
``--local-out DIR`` (the exact Parquet files the run would upload, for inspection with DuckDB).

Idempotent and resumable: rows already in a raw partition (by ``_row_key``) are skipped, and the
curated upserts change only rows whose values differ. Rerun the same command after a failure.

Examples:
    python -m x_lakehouse.backfill                                  # dry run, Sheet only
    python -m x_lakehouse.backfill --local-out /tmp/x-lake          # dry run + local Parquet
    python -m x_lakehouse.backfill --check-targets                  # dry run, also reads R2/Postgres counts
    python -m x_lakehouse.backfill --execute                        # the real migration (after approval)
"""

from __future__ import annotations

import argparse
import json
import uuid
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

import httpx  # noqa: E402

from . import curated as cur  # noqa: E402
from .coerce import TZ  # noqa: E402
from .config import Config  # noqa: E402
from .lake import LocalStore, open_store  # noqa: E402
from .pipeline import DatasetCheck, RunResult, check_header, feed_index, run, type_tab, validate_rows  # noqa: E402
from .schemas import DATASETS, X_FEED, X_POST_CONTEXT, X_SIGNALS, is_protected  # noqa: E402
from .sheet import SheetReader  # noqa: E402


def load(cfg: Config, names: list[str]) -> tuple[dict[str, Any], dict[str, DatasetCheck], list[str]]:
    tabs = [DATASETS[n].tab for n in names if DATASETS[n].sheet]  # bot-only datasets have no tab
    for t in tabs:
        assert not is_protected(t), t
    missing_tabs: list[str] = []
    data: dict[str, Any] = {}
    if tabs:  # only bot-only datasets: no Sheet read, so no Google credential is needed
        with httpx.Client(timeout=120) as http:
            reader = SheetReader(cfg, http)
            titles = reader.titles()
            missing_tabs = [t for t in tabs if t not in titles]
            data = reader.read([t for t in tabs if t in titles])
    typed: dict[str, Any] = {}
    checks: dict[str, DatasetCheck] = {}
    for n in names:
        ds = DATASETS[n]
        chk = DatasetCheck(n, ds.tab)
        if ds.tab not in data:
            checks[n] = chk
            continue
        td = data[ds.tab]
        check_header(ds, td.header, chk)
        typed[n] = type_tab(ds, td.rows)  # type: ignore[arg-type]
        checks[n] = chk
    return typed, checks, missing_tabs


def print_report(result: RunResult, missing_tabs: list[str], *, execute: bool, targets: dict[str, Any]) -> None:
    mode = "EXECUTE" if execute else "DRY RUN (nothing written to R2 or Postgres)"
    print(f"== x_lakehouse backfill — {mode}")
    if missing_tabs:
        print(f"!! tabs not found in the Sheet: {missing_tabs}")
    print(f"\n{'dataset':22} {'sheet':>7} {'raw new':>8} {'raw skip':>8} {'parts':>5} {'curated':>8} "
          f"{'coerce err':>10} {'dup key':>7} {'dup post':>8} {'blank key':>9}")
    for n, chk in result.checks.items():
        raw = result.raw.get(n)
        c = result.curated.get(n, {})
        print(f"{n:22} {chk.rows:>7} {raw.written if raw else '-':>8} {raw.skipped if raw else '-':>8} "
              f"{len([p for p in raw.partitions if p.to_write]) if raw else '-':>5} {c.get('rows', '-'):>8} "
              f"{sum(chk.coerce_errors.values()):>10} {chk.duplicate_keys:>7} "
              f"{'-' if chk.duplicate_post_ids is None else chk.duplicate_post_ids:>8} {chk.blank_key_rows:>9}")
    for n, chk in result.checks.items():
        print(f"\n-- {n}  (tab {chk.tab!r}, {chk.rows} rows)")
        if chk.missing_columns or chk.unexpected_columns or chk.order_differs:
            print(f"   SCHEMA MISMATCH missing={chk.missing_columns} unexpected={chk.unexpected_columns} "
                  f"order_differs={chk.order_differs}")
        else:
            print("   schema: header matches the contract")
        if chk.coerce_errors:
            print(f"   coercion errors (kept as null + logged in _coerce_errors): {chk.coerce_errors}")
            for s in chk.coerce_samples[:5]:
                print(f"     row {s['sheet_row']} {s['column']}: {s['raw']!r} — {s['reason']}")
        high = {k: v for k, v in chk.null_rate.items() if v > 0}
        if high:
            print("   null rate: " + ", ".join(f"{k}={v:.0%}" for k, v in sorted(high.items(), key=lambda x: -x[1])))
        raw = result.raw.get(n)
        if raw and raw.partitions:
            days = [p.date for p in raw.partitions]
            print(f"   raw partitions: {len(days)} ({days[0]} … {days[-1]}), e.g. {raw.partitions[0].key or '(none new)'}")
        if n in result.curated:
            print(f"   curated upsert: {result.curated[n]}")
        for s in chk.samples:
            print(f"   sample: {json.dumps(s, ensure_ascii=False, default=str)[:400]}")
    if targets:
        print("\n-- current targets (read-only)")
        for k, v in targets.items():
            print(f"   {k}: {v}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--execute", action="store_true", help="really write R2 + Postgres (default: dry run)")
    p.add_argument("--dry-run", action="store_true", help="the default; accepted for clarity")
    p.add_argument("--datasets", default=",".join(n for n, d in DATASETS.items() if d.sheet),
                   help="comma list of Sheet datasets (default: all with a Sheet tab)")
    p.add_argument("--local-out", help="dry run: also write the Parquet parts to this directory")
    p.add_argument("--lake-uri", help="override X_LAKE_URI")
    p.add_argument("--check-targets", action="store_true", help="dry run: read existing R2/Postgres state")
    p.add_argument("--sample", type=int, default=2)
    p.add_argument("--json", action="store_true", dest="as_json")
    args = p.parse_args(argv)
    if args.execute and args.dry_run:
        p.error("--execute and --dry-run together")
    if args.execute and args.local_out:
        p.error("--local-out is for dry runs")
    cfg = Config()
    names = [n.strip() for n in args.datasets.split(",") if n.strip()]
    unknown = [n for n in names if n not in DATASETS]
    if unknown:
        p.error(f"unknown datasets {unknown}")
    no_tab = [n for n in names if not DATASETS[n].sheet]
    if no_tab:
        p.error(f"datasets without a Sheet tab (bots send them through ingest()): {no_tab}")

    typed, checks, missing_tabs = load(cfg, names)
    for n, rows in typed.items():
        validate_rows(DATASETS[n], rows, checks[n], args.sample)
    feed_lookup: dict[str, dict[str, Any]] = {}
    if X_FEED.name not in names and ({X_POST_CONTEXT.name, X_SIGNALS.name} & set(names)):
        # Context is partitioned by its post's capture date and signals take author/text from X Feed:
        # read X Feed too (lookup only, not written) so a dataset-scoped backfill matches a full one.
        feed_typed, feed_checks, feed_missing = load(cfg, [X_FEED.name])
        if feed_missing or feed_checks[X_FEED.name].missing_columns:
            missing_tabs += feed_missing or [f"{X_FEED.tab} (columns {feed_checks[X_FEED.name].missing_columns})"]
        feed_lookup = feed_index(feed_typed.get(X_FEED.name, []))
    blocking = [f"{n}: missing columns {c.missing_columns}" for n, c in checks.items() if c.missing_columns]
    blocking += [f"tab not found: {t}" for t in missing_tabs]
    if blocking and (args.execute or args.local_out):
        # A lost column would land as nulls under unchanged row keys, which a rerun cannot correct.
        print("refusing to write: " + "; ".join(blocking), file=sys.stderr)
        return 2
    if args.execute and not cfg.db_url:
        p.error("--execute needs X_LAKE_DB_URL (raw and curated are written together)")

    now = datetime.now(timezone.utc)
    batch_id = f"backfill-{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    store = None
    prefix = ""
    conn = None
    targets: dict[str, Any] = {}
    write_files = args.execute
    if args.execute or args.check_targets:
        store, prefix = open_store(cfg, args.lake_uri)
        if cfg.db_url:
            conn = cur.connect(cfg.db_url)
            if args.execute:
                cur.ensure_schema(conn)
    if args.local_out:
        store, prefix, write_files = LocalStore(Path(args.local_out)), "", True
    try:
        if args.check_targets and conn is not None:
            for t in cur.CURATED:  # x_intel only: the private schema is never counted from here
                try:
                    targets[f"postgres x_intel.{t}"] = cur.count(conn, t)
                except Exception as exc:  # table not created yet
                    conn.rollback()
                    targets[f"postgres x_intel.{t}"] = f"n/a ({type(exc).__name__})"
        result = run(typed, store=store, prefix=prefix, conn=conn if args.execute else None,
                     source="sheet-backfill", execute=write_files, batch_id=batch_id,
                     run_date=now.astimezone(TZ).date(), now=now, feed_lookup=feed_lookup)
    finally:
        if conn is not None:
            conn.close()
    result.checks = checks
    if args.as_json:
        print(json.dumps({"checks": {k: asdict(v) for k, v in checks.items()},
                          "raw": {k: asdict(v) for k, v in result.raw.items()},
                          "curated": result.curated, "missing_tabs": missing_tabs, "targets": targets},
                         ensure_ascii=False, default=str, indent=1))
    else:
        print_report(result, missing_tabs, execute=args.execute, targets=targets)
    return 2 if blocking else 0


if __name__ == "__main__":
    sys.exit(main())
