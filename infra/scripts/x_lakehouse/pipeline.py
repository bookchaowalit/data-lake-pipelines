"""Shared steps for backfill and the daily append: type rows, validate, write raw, upsert curated."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

from . import curated as cur
from .coerce import TypedRow, type_row
from .lake import Store, WriteReport, write_raw
from .schemas import CURATED, DATASETS, X_FEED, X_POST_CONTEXT, Dataset


@dataclass
class DatasetCheck:
    dataset: str
    tab: str
    rows: int = 0
    missing_columns: list[str] = field(default_factory=list)
    unexpected_columns: list[str] = field(default_factory=list)
    order_differs: bool = False
    coerce_errors: dict[str, int] = field(default_factory=dict)
    coerce_samples: list[dict[str, Any]] = field(default_factory=list)
    rows_with_errors: int = 0
    blank_key_rows: int = 0
    duplicate_keys: int = 0
    duplicate_post_ids: int | None = None
    null_rate: dict[str, float] = field(default_factory=dict)
    samples: list[dict[str, Any]] = field(default_factory=list)


def check_header(ds: Dataset, header: list[str], chk: DatasetCheck) -> None:
    hdr = [h for h in header if h]
    chk.missing_columns = [c for c in ds.names if c not in hdr]
    chk.unexpected_columns = [h for h in hdr if h not in ds.kinds]
    chk.order_differs = [h for h in hdr if h in ds.kinds] != [c for c in ds.names if c in hdr]


def _short(v: Any, n: int = 60) -> Any:
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, str) and len(v) > n:
        return v[:n] + "…"
    return v


def validate_rows(ds: Dataset, rows: list[TypedRow], chk: DatasetCheck, sample: int = 2) -> None:
    chk.rows = len(rows)
    errs: Counter[str] = Counter()
    for r in rows:
        if r.errors:
            chk.rows_with_errors += 1
        for col, info in r.errors.items():
            if info["reason"] == "key column is blank":
                continue
            errs[col] += 1
            if len(chk.coerce_samples) < 10:
                chk.coerce_samples.append({"sheet_row": r.sheet_row, "column": col,
                                           "raw": _short(info["raw"], 200), "reason": info["reason"]})
        if any(info["reason"] == "key column is blank" for info in r.errors.values()):
            chk.blank_key_rows += 1
    chk.coerce_errors = dict(errs)
    keys = Counter(r.row_key for r in rows)
    chk.duplicate_keys = sum(n - 1 for n in keys.values() if n > 1)
    # Context rows and the weekly analyses legitimately repeat a post_id. X Feed repeats are counted
    # but validate does not fail on them (one post, several watches); signals must not repeat.
    # The bot-only datasets key a post with another column (handle, folder, collection time).
    if "post_id" in ds.kinds and ds.sheet and ds.name not in (X_POST_CONTEXT.name, "x_analyses"):
        ids = Counter(r.values.get("post_id") for r in rows if r.values.get("post_id"))
        chk.duplicate_post_ids = sum(n - 1 for n in ids.values() if n > 1)
    if rows:
        chk.null_rate = {c: round(sum(1 for r in rows if r.values.get(c) is None) / len(rows), 4)
                         for c in ds.names}
    chk.samples = [{"sheet_row": r.sheet_row, **{k: _short(v) for k, v in r.values.items()}}
                   for r in rows[:sample]]


def type_tab(ds: Dataset, cells: list[tuple[int | None, dict[str, Any]]]) -> list[TypedRow]:
    return [type_row(ds, c, sheet_row=i) for i, c in cells]


def partition_values(ds: Dataset, rows: list[TypedRow], feed_ts: dict[str, Any]) -> list[Any]:
    if ds.ingest_ts:
        return [r.values.get(ds.ingest_ts) for r in rows]
    if ds.name == X_POST_CONTEXT.name:  # no timestamp of its own: the post's X Feed collected_at
        return [feed_ts.get(str(r.values.get("post_id"))) for r in rows]
    return [None] * len(rows)


def feed_index(feed_rows: list[TypedRow]) -> dict[str, dict[str, Any]]:
    """post_id -> latest X Feed values (later rows win)."""
    out: dict[str, dict[str, Any]] = {}
    for r in feed_rows:
        pid = r.values.get("post_id")
        if pid:
            out[str(pid)] = r.values
    return out


@dataclass
class RunResult:
    checks: dict[str, DatasetCheck] = field(default_factory=dict)
    raw: dict[str, WriteReport] = field(default_factory=dict)
    curated: dict[str, dict[str, int]] = field(default_factory=dict)


def run(typed: dict[str, list[TypedRow]], *, store: Store | None, prefix: str, conn: Any, source: str,
        execute: bool, batch_id: str, run_date: date, feed_lookup: dict[str, dict[str, Any]] | None = None,
        now: datetime | None = None) -> RunResult:
    """Write raw for every raw dataset in ``typed``, then upsert the curated tables.

    ``feed_lookup`` adds X Feed facts that are not in this batch (append runs pass the day's feed).
    """
    now = now or datetime.now(timezone.utc)
    result = RunResult()
    feed = feed_index(typed.get(X_FEED.name, []))
    if feed_lookup:
        feed = {**feed_lookup, **feed}
    feed_ts = {pid: v.get("collected_at") for pid, v in feed.items()}
    for name, rows in typed.items():
        ds = DATASETS[name]
        if ds.raw:
            result.raw[name] = write_raw(ds, rows, partition_values(ds, rows, feed_ts), store=store,
                                         prefix=prefix, source=source, execute=execute, run_date=run_date,
                                         batch_id=batch_id, now=now)
    for name, rows in typed.items():
        if name not in CURATED:
            continue
        c = CURATED[name]
        stored = cur.stored_protected(conn, c) if execute and conn is not None else set()
        latest = cur.dedupe_latest(c, cur.eligible(c, rows, stored))
        records = cur.join_feed(latest, feed, c) if c.joined else [dict(r.values) for r in latest]
        if execute and conn is not None:
            result.curated[name] = cur.upsert(conn, c, records, batch_id)
        else:
            joined_missing = (sum(1 for r in latest if str(r.values.get("post_id")) not in feed)
                              if c.joined else 0)
            result.curated[name] = {"rows": len(records), "join_missing": joined_missing}
    return result
