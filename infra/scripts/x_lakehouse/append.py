"""Daily writer: what the bot runs call instead of appending rows to the Sheet.

    from x_lakehouse.append import append_rows
    report = append_rows({
        "x_feed": feed_rows,            # list of dicts keyed by the Sheet header names
        "x_signals": signal_rows,
        "claims": claim_rows,
    })

Writes each raw dataset's rows to R2 (new rows only, as the next part of each ingest-date
partition) and upserts x_signals / claims / config tables in Postgres. Safe to call again with
the same rows: raw skips known ``_row_key``s and the upserts are no-ops for unchanged rows.

``execute=False`` validates and plans without touching R2 or Postgres.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from . import curated as cur
from .coerce import TZ, type_row
from .config import Config
from .lake import Store, ingest_date, lookup_latest, open_store
from .pipeline import DatasetCheck, run, validate_rows
from .schemas import DATASETS, X_FEED, X_POST_CONTEXT, X_SIGNALS


class AppendError(RuntimeError):
    pass


def append_rows(rows_by_dataset: dict[str, list[dict[str, Any]]], *, execute: bool = True,
                cfg: Config | None = None, store: Store | None = None, prefix: str | None = None,
                conn: Any = None, now: datetime | None = None) -> dict[str, Any]:
    cfg = cfg or Config()
    now = now or datetime.now(timezone.utc)
    unknown = [n for n in rows_by_dataset if n not in DATASETS]
    if unknown:
        raise AppendError(f"unknown datasets {unknown}")
    typed = {n: [type_row(DATASETS[n], cells) for cells in rows] for n, rows in rows_by_dataset.items()}
    checks = {}
    for n, rows in typed.items():
        chk = DatasetCheck(n, DATASETS[n].tab)
        validate_rows(DATASETS[n], rows, chk, sample=0)
        checks[n] = chk
    batch_id = f"append-{now:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
    own_conn = False
    if execute:
        if store is None:
            store, default_prefix = open_store(cfg)
            prefix = default_prefix if prefix is None else prefix
        if conn is None:
            conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
            own_conn = True
        cur.ensure_schema(conn)
    run_date = now.astimezone(TZ).date()
    feed_lookup: dict[str, dict[str, Any]] = {}
    if store is not None and (typed.get(X_SIGNALS.name) or typed.get(X_POST_CONTEXT.name)):
        # Signals and post context often arrive in a later call than their posts: fetch those posts'
        # feed rows (facts for x_signals, the capture date that partitions x_post_context).
        in_batch = {str(r.values.get("post_id")) for r in typed.get(X_FEED.name, [])}
        sigs = [r for r in typed.get(X_SIGNALS.name, []) if r.values.get("post_id")]
        ctx = [r for r in typed.get(X_POST_CONTEXT.name, []) if r.values.get("post_id")]
        need = {str(r.values["post_id"]) for r in sigs + ctx} - in_batch
        days = {ingest_date(r.values.get("collected_at"), run_date) for r in sigs
                if str(r.values["post_id"]) in need}
        if any(str(r.values["post_id"]) in need for r in ctx):
            days.add(run_date)  # context has no timestamp: search the week before the run
        feed_lookup = lookup_latest(store, prefix or "", X_FEED, "post_id", need, days)
    try:
        result = run(typed, store=store, prefix=prefix or "", conn=conn, source="append", execute=execute,
                     batch_id=batch_id, run_date=run_date, now=now, feed_lookup=feed_lookup)
    finally:
        if own_conn:
            conn.close()
    return {
        "batch_id": batch_id,
        "execute": execute,
        "raw": {n: {"written": r.written, "skipped": r.skipped,
                    "parts": [p.key for p in r.partitions if p.to_write]} for n, r in result.raw.items()},
        "curated": result.curated,
        "coerce_errors": {n: c.coerce_errors for n, c in checks.items() if c.coerce_errors},
        "checks": {n: asdict(c) for n, c in checks.items()},
    }
