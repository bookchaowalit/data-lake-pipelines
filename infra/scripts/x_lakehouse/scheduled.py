#!/usr/bin/env python3
"""Scheduled parallel-run job (Railway cron ``data-schedules/x-lakehouse``, 3x a day).

One run = catch-up ``backfill --execute`` (Sheet -> R2 raw + Supabase curated; idempotent, writes only
rows not stored yet), then ``validate --since <yesterday ICT>`` against all three stores, then the
serving view as a DRY RUN (row counts only; the Sheet is never written by this job).

Logs carry counts only, never row content. Exit 0 = written and validated; 1 = validate failed or a
write was refused. With ``HEARTBEAT_TOKEN`` set it posts the result to the uptime monitor; with
``SENTRY_DSN`` set a failed run is reported (scrubbed, see data_lake/observability.py).

    python -m x_lakehouse.scheduled            # what the cron runs
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

from . import backfill, serve_sheet  # noqa: E402
from . import curated as cur  # noqa: E402
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
    started = datetime.now(timezone.utc)
    code, summary = _backfill()
    _log(event="backfill", exit_code=code, **summary)
    since = (started.astimezone(TZ) - timedelta(days=1)).date()
    report = reconcile(Config(), list(DATASETS), since, None)
    failed = {n: r["reasons"] for n, r in report["datasets"].items() if not r["ok"]}
    _log(event="validate", since=since, ok=report["ok"], failed=failed,
         sheet_rows={n: r["sheet_rows"] for n, r in report["datasets"].items()})
    _log(event="serve_dry_run", rows=_serve_counts(Config()))
    ok = code == 0 and report["ok"]
    _log(event="done", ok=ok, seconds=round((datetime.now(timezone.utc) - started).total_seconds()))
    _heartbeat(ok)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data_lake.observability import run_with_reporting

    sys.exit(run_with_reporting("x-lakehouse", main))
