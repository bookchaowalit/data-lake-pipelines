#!/usr/bin/env python3
"""Scheduled parallel-run job (Railway cron ``data-schedules/x-lakehouse``, 3x a day).

One run = catch-up ``backfill --execute`` (Sheet -> R2 raw + Supabase curated; idempotent, writes only
rows not stored yet), then ``validate --since <yesterday ICT>`` against all three stores, then the
serving view as a DRY RUN (row counts only; the Sheet is never written by this job).

Logs carry counts only, never row content. Exit 0 = written and validated; 1 = validate failed or a
write was refused. With ``HEARTBEAT_TOKEN`` set it posts the result to the uptime monitor; with
``SENTRY_DSN`` set a failed run is reported (scrubbed, see data_lake/observability.py).

    python -m x_lakehouse.scheduled            # what the cron runs

Source (``X_LAKE_SOURCE``, default ``sheet``; see inbox.py for the bot side):
    sheet   today's parallel run, unchanged.
    both    dual write: drain the bot inbox, then the Sheet catch-up, validate Sheet vs raw vs Postgres
            and compare inbox keys with Sheet keys (any miss fails the run). ``X_LAKE_COMPARE_SINCE``
            (ISO time) starts the comparison window at the first dual-write run.
    inbox   lake only: drain the inbox, validate raw vs Postgres, check every drained row reached R2;
            after an ok run delete inbox rows drained more than 3 days ago.
inbox/both take a Postgres advisory lock: a second drainer exits 0 without writing.
``X_LAKE_SERVE=execute`` refreshes the serving tabs after an ok run (default: dry run, counts only).
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

import httpx  # noqa: E402

from . import backfill, inbox, serve_sheet  # noqa: E402
from . import curated as cur  # noqa: E402
from .lake import open_store  # noqa: E402
from .coerce import TZ  # noqa: E402
from .config import Config  # noqa: E402
from .schemas import DATASETS  # noqa: E402
from .validate import reconcile  # noqa: E402

HEARTBEAT_URL = "https://monitor.bookchaowalit.com/heartbeat/x-lakehouse"


def _log(**fields: object) -> None:
    print(json.dumps(fields, ensure_ascii=False, default=str), flush=True)


def _backfill() -> tuple[int, dict[str, object]]:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):  # the report holds row samples; keep them out of the logs
        code = backfill.main(["--execute", "--json", "--sample", "0"])
    try:
        out = json.loads(buf.getvalue())
    except ValueError:
        return code or 1, {}
    raw = {n: {"new": sum(p["to_write"] for p in r["partitions"]),
               "skipped": sum(p["already_present"] for p in r["partitions"])} for n, r in out["raw"].items()}
    return code, {"raw": raw, "curated": out["curated"], "missing_tabs": out["missing_tabs"],
                  "schema_drift": {n: c["missing_columns"] for n, c in out["checks"].items() if c["missing_columns"]}}


def _serve_counts(cfg: Config) -> dict[str, int]:
    conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
    try:
        return {tab: len(rows) for tab, (_, rows) in serve_sheet.fetch(conn).items()}
    finally:
        conn.close()


def _serve_execute() -> int:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):  # serve_sheet prints tab names and row counts only
        code = serve_sheet.main(["--execute"])
    _log(event="serve", exit_code=code, lines=buf.getvalue().splitlines()[-5:])
    return code


def _source(cfg: Config) -> str:
    src = cfg.get("X_LAKE_SOURCE", "sheet").lower()
    if src not in ("sheet", "both", "inbox"):
        raise ValueError("X_LAKE_SOURCE must be sheet, both or inbox")
    return src


def _heartbeat(ok: bool) -> None:
    token = os.environ.get("HEARTBEAT_TOKEN", "").strip()
    if not token:
        return
    try:
        httpx.post(HEARTBEAT_URL, timeout=15, json={"ok": ok, "failed": [] if ok else ["x-lakehouse"]},
                   headers={"Authorization": f"Bearer {token}", "User-Agent": "x-lakehouse/1"})
    except httpx.HTTPError:
        _log(event="heartbeat", status="error")


def main(argv: list[str] | None = None) -> int:
    try:
        return _run()
    except BaseException:
        _heartbeat(False)  # a crash alerts now, not when the heartbeat window runs out
        raise


def _run() -> int:
    cfg = Config()
    if _source(cfg) == "sheet":
        return _run_sheet(cfg)
    return _run_inbox(cfg)


def _finish(ok: bool, started: datetime, cfg: Config) -> int:
    if ok and cfg.get("X_LAKE_SERVE").lower() == "execute":
        ok = _serve_execute() == 0
    else:
        _log(event="serve_dry_run", rows=_serve_counts(cfg))
    _log(event="done", ok=ok, seconds=round((datetime.now(timezone.utc) - started).total_seconds()))
    _heartbeat(ok)
    return 0 if ok else 1


def _run_sheet(cfg: Config) -> int:
    started = datetime.now(timezone.utc)
    code, summary = _backfill()
    _log(event="backfill", exit_code=code, **summary)
    since = (started.astimezone(TZ) - timedelta(days=1)).date()
    report = reconcile(cfg, list(DATASETS), since, None)
    failed = {n: r["reasons"] for n, r in report["datasets"].items() if not r["ok"]}
    _log(event="validate", since=since, ok=report["ok"], failed=failed,
         sheet_rows={n: r["sheet_rows"] for n, r in report["datasets"].items()})
    return _finish(code == 0 and report["ok"], started, cfg)


def _run_inbox(cfg: Config) -> int:
    source = _source(cfg)
    started = datetime.now(timezone.utc)
    lock = cur.connect(cfg.require("X_LAKE_DB_URL"))
    try:
        if not inbox.try_lock(lock):
            _log(event="drain", status="locked", note="another drainer holds the lock; nothing written")
            return 0
        inbox.ensure_inbox(lock)
        store, prefix = open_store(cfg)
        drained = inbox.drain(lock, store=store, prefix=prefix, cfg=cfg, now=started)
        _log(event="drain", source=source, rows=drained["rows"], marked=drained.get("marked", 0),
             datasets=drained["datasets"], coerce_errors=drained.get("coerce_errors", {}),
             raw={n: r["written"] for n, r in drained.get("raw", {}).items()}, curated=drained.get("curated", {}),
             config_coerce_errors=drained.get("config_coerce_errors", {}))
        # Every accepted row marked, and no config row held back for a coercion error (those stay in
        # the inbox and fail each run until a corrected row replaces them).
        ok = drained.get("marked", 0) == drained.get("expected", 0) and not drained.get("config_coerce_errors")
        since = (started.astimezone(TZ) - timedelta(days=1)).date()
        if source == "both":
            code, summary = _backfill()
            _log(event="backfill", exit_code=code, **summary)
            ok = ok and code == 0
            snapshot_at = datetime.now(timezone.utc)  # rows the bots send after this may miss the read
            sheet = backfill.load(cfg, list(DATASETS))
            report = reconcile(cfg, list(DATASETS), since, None, preloaded=sheet)
            window = datetime.combine(since, datetime.min.time(), TZ)
            if cfg.get("X_LAKE_COMPARE_SINCE"):
                window = max(window, datetime.fromisoformat(cfg.get("X_LAKE_COMPARE_SINCE")).astimezone(timezone.utc))
            cmp = inbox.compare(lock, sheet[0], window, started, snapshot_at=snapshot_at,
                                stored_on=inbox.stored_dates(store, prefix))
            mismatch = {n: r for n, r in cmp.items() if r["missing_in_sheet"] or r.get("missing_in_inbox")}
            _log(event="compare", since=window, ok=not mismatch, mismatch=mismatch,
                 inbox_rows={n: r["inbox_rows"] for n, r in cmp.items()})
            ok = ok and not mismatch
        else:
            report = reconcile(cfg, list(DATASETS), since, None, sheet=False)
        failed = {n: r["reasons"] for n, r in report["datasets"].items() if not r["ok"]}
        lost = {n: k for n, k in inbox.landed(lock, store, prefix, started - timedelta(days=1)).items() if k}
        _log(event="validate", since=since, source=source, ok=report["ok"] and not lost, failed=failed,
             drained_missing_from_raw=lost)
        # A drained row with a coercion error fails the run even when its ingest date is older than the
        # validate window (a delayed or replayed row); the run then also skips the prune.
        ok = ok and report["ok"] and not lost and not drained.get("coerce_errors")
        if ok:
            _log(event="prune", deleted=inbox.prune(lock, started))
    finally:
        lock.close()  # releases the advisory lock
    return _finish(ok, started, cfg)


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data_lake.observability import run_with_reporting

    sys.exit(run_with_reporting("x-lakehouse", main))
