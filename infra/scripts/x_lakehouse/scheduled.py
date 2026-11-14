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
inbox/both take a Postgres advisory lock: a second drainer exits 0 without writing. They also drain the
private inbox (private.py; skipped while schema x_intel_private does not exist) and write one
``x_intel.drain_log`` row per run (counts only).
``X_LAKE_SERVE=execute`` refreshes the serving tabs after an ok run (default: dry run, counts only).
``X_LAKE_META=dry|write`` lands owner-shared Meta posts (meta.py, spec docs/specs/meta-intake.md) before the digest.
``X_LAKE_DIGEST=dry|send`` builds (and sends) the Telegram digest at the end of every run (digest.py).
``X_LAKE_WEEKLY=dry|write`` builds the weekly analyses and triage audit into lh_* tables (weekly.py).
``X_LAKE_OPS_ALERT`` (default ``on``) sends one Telegram note when a hook above had a problem.
``X_LAKE_GOLD=write`` lands the queries/ results as Gold Parquet on the lake (gold.py).
``X_LAKE_BI=build|deploy`` then renders the x-insights site (bi_site.py) and, with ``deploy``, uploads it to
Cloudflare Pages (pages_deploy.py; spec docs/specs/x-insights.md).
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    __package__ = "x_lakehouse"

import httpx  # noqa: E402

from . import backfill, digest, feed, inbox, private, serve_sheet  # noqa: E402
from . import curated as cur  # noqa: E402
from .lake import open_store  # noqa: E402
from .coerce import TZ  # noqa: E402
from .config import Config  # noqa: E402
from .schemas import DATASETS  # noqa: E402
from .validate import reconcile  # noqa: E402

HEARTBEAT_URL = "https://monitor.bookchaowalit.com/heartbeat/x-lakehouse"


_EVENTS: list[dict[str, object]] = []  # this run's log lines, for _ops_alert
DIGEST_BAD = ("error", "partial", "unknown", "blocked", "superseded")


def _log(**fields: object) -> None:
    _EVENTS.append(fields)
    print(json.dumps(fields, ensure_ascii=False, default=str), flush=True)


def _issues(events: list[dict[str, object]]) -> list[str]:
    """Short, fixed-vocabulary lines for every hook problem of this run (never row text or error text from
    a library: exception types, our own statuses and fallback reasons only)."""
    out: list[str] = []
    for e in events:
        ev = e.get("event")
        if ev not in ("meta", "digest", "weekly", "gold", "bi"):
            continue
        if e.get("status") == "error":
            out.append(f"{ev}: error {str(e.get('error', '')).split(':', 1)[0][:60]}")
            continue
        if ev == "meta":  # step-level errors inside an ok run (counts and exception types only)
            if e.get("ocr_errors"):
                out.append(f"meta ocr: {e.get('ocr_errors')} failed")
            for part in ("landing", "ocr", "labelling"):
                if str(e.get(part, "")).startswith("error"):
                    out.append(f"meta {part}: {str(e.get(part))[:60]}")
            if e.get("labelling") in ("gateway_error", "invalid_answers"):
                out.append(f"meta labelling: {e.get('labelling')}")
            ret = e.get("retention")
            if isinstance(ret, dict) and ret.get("stale_landing"):
                out.append("meta retention: stale landing left")
        if ev == "digest":
            for r in e.get("reports") or []:
                if not isinstance(r, dict):
                    continue
                if r.get("status") in DIGEST_BAD:
                    out.append(f"digest {r.get('digest_id')}: {r.get('status')}")
                elif r.get("status") == "sent" and r.get("ai") is False and r.get("fallback_reason"):
                    out.append(f"digest {r.get('digest_id')}: facts-only ({str(r['fallback_reason'])[:80]})")
        elif ev == "weekly" and isinstance(e.get("triage"), str) and str(e["triage"]).startswith("error"):
            out.append(f"weekly {e.get('week')}: triage {str(e['triage'])[:80]}")
        elif ev == "gold" and e.get("ok") is False:
            out.append(f"gold: failed {sorted((e.get('errors') or {}).keys())[:5]}")
    return out


def _ops_alert(cfg: Config) -> None:
    """One Telegram note when a hook had a problem (the lake run itself is watched by the heartbeat).
    X_LAKE_OPS_ALERT=off disables it; without the Telegram settings it only logs."""
    issues = _issues(_EVENTS)
    if not issues or (cfg.get("X_LAKE_OPS_ALERT", "on").lower() == "off"):
        return
    token, chat = cfg.get("TELEGRAM_NOTI_BOT_TOKEN"), cfg.get("TELEGRAM_NOTI_CHAT_ID")
    if not token or not chat:
        _log(event="ops_alert", status="skipped", issues=len(issues))
        return
    text = "⚠️ x-lakehouse\n" + "\n".join(f"• {i}" for i in issues[:15])
    try:
        r = httpx.post(f"https://api.telegram.org/bot{token}/sendMessage", timeout=30,
                       json={"chat_id": chat, "text": text, "disable_web_page_preview": True})
        _log(event="ops_alert", status="sent" if r.status_code == 200 else f"http {r.status_code}", issues=len(issues))
    except httpx.HTTPError as exc:
        _log(event="ops_alert", status="error", error=type(exc).__name__, issues=len(issues))  # no URL: it has the token


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
        try:
            feed_win = serve_sheet.feed_window(cfg)
        except Exception:  # noqa: BLE001 - counts only; the execute path reports a lake error itself
            feed_win = None
        return {tab: len(rows) for tab, (_, rows) in serve_sheet.fetch(conn, feed_win).items()}
    finally:
        conn.close()


def _serve_execute() -> int:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):  # serve_sheet prints tab names and row counts only
        code = serve_sheet.main(["--execute"])
    _log(event="serve", exit_code=code, lines=buf.getvalue().splitlines()[-8:])
    return code


def _digest(cfg: Config) -> None:
    """X_LAKE_DIGEST=dry|send: the Telegram digest (digest.py). A failure is logged, never fatal to the
    lake run; the reports hold counts and ids only."""
    mode = cfg.get("X_LAKE_DIGEST", "off").lower() or "off"
    if mode == "off":
        return
    try:
        conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
        try:
            _log(event="digest", mode=mode, reports=digest.run(conn, cfg, mode=mode, feed_loader=feed.loader(cfg)))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - reported, the lake run's result stands
        _log(event="digest", mode=mode, status="error", error=f"{type(exc).__name__}: {str(exc)[:200]}")


def _meta(cfg: Config) -> None:
    """X_LAKE_META=dry|write: Meta captures -> raw + meta_posts (meta.py). Counts only in the log; never
    fatal to the lake run."""
    mode = cfg.get("X_LAKE_META", "off").lower() or "off"
    if mode == "off":
        return
    try:
        from . import meta

        store, prefix = open_store(cfg)
        conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
        try:
            _log(event="meta", **meta.run(conn, cfg, mode=mode, store=store, prefix=prefix))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - reported, the lake run's result stands
        _log(event="meta", mode=mode, status="error", error=type(exc).__name__)


def _hour() -> datetime:
    """The current hour: the site's feed window moves once an hour, so a rebuild within it is byte-stable."""
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _bi(cfg: Config) -> None:
    """X_LAKE_BI=build|deploy: the x-insights site. Like the digest, a failure is logged (exception type
    only) and never changes the lake run's result."""
    mode = cfg.get("X_LAKE_BI", "off").lower() or "off"
    if mode == "off":
        return
    try:
        if mode not in ("build", "deploy"):
            raise ValueError("X_LAKE_BI must be off, build or deploy")
        from . import bi_site, pages_deploy

        denylist = digest.load_denylist(cfg)
        from .marts import ensure_marts

        conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
        try:
            ensure_marts(conn)  # the site reads the marts; do not rely on X_LAKE_DIGEST having created them
            feed_win, feed_days, feed_error = None, None, None
            try:
                from .lake import open_store

                end = _hour()
                store, prefix = open_store(cfg)
                rows = feed.load_rows(store, prefix, end - timedelta(days=bi_site.CHART_DAYS))
                feed_win = feed.summarise(rows, end - timedelta(days=serve_sheet.DAYS), end)
                feed_days = feed.daily(rows, end - timedelta(days=bi_site.CHART_DAYS), end)
            except Exception as exc:  # noqa: BLE001 - the site still builds from the marts
                feed_win, feed_days, feed_error = None, None, type(exc).__name__
            snap = bi_site.load_snapshot(conn, denylist, feed_win, feed_days)
        finally:
            conn.close()
        with tempfile.TemporaryDirectory(prefix="x-insights-") as tmp:
            built = bi_site.build_site(snap, Path(tmp) / "site")
            result: dict[str, object] = {"status": "built"}
            if mode == "deploy":
                result = pages_deploy.deploy(Path(tmp) / "site", account=cfg.require("CLOUDFLARE_ACCOUNT_ID"),
                                             project=cfg.get("X_LAKE_BI_PROJECT", "x-insights"),
                                             token=cfg.require("CLOUDFLARE_PAGES_TOKEN"), mark=str(built["mark"]))
        _log(event="bi", mode=mode, **{**built, **result}, **({"feed_error": feed_error} if feed_error else {}))
    except Exception as exc:  # noqa: BLE001 - reported, the lake run's result stands
        # PagesError text is the API step, HTTP status and error codes only; other errors log their type.
        detail = str(exc)[:200] if type(exc).__name__ == "PagesError" else None
        _log(event="bi", mode=mode, status="error", error=type(exc).__name__, detail=detail)


def _weekly(cfg: Config) -> None:
    """X_LAKE_WEEKLY=dry|write: weekly analyses + triage audit into lh_* tables (weekly.py). Logged
    (counts, the week, exception types), never fatal to the lake run."""
    mode = cfg.get("X_LAKE_WEEKLY", "off").lower() or "off"
    if mode == "off":
        return
    try:
        from . import weekly

        store, prefix = open_store(cfg)
        conn = cur.connect(cfg.require("X_LAKE_DB_URL"))
        try:
            _log(event="weekly", **weekly.run(conn, cfg, mode=mode, store=store, prefix=prefix))
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - reported, the lake run's result stands
        _log(event="weekly", mode=mode, status="error", error=type(exc).__name__)


def _gold(cfg: Config) -> None:
    """X_LAKE_GOLD=write: queries/ over raw -> gold/ on the lake. Logged (counts, query names, exception types),
    never fatal to the lake run."""
    mode = cfg.get("X_LAKE_GOLD", "off").lower() or "off"
    if mode == "off":
        return
    try:
        if mode != "write":
            raise ValueError("X_LAKE_GOLD must be off or write")
        from . import gold

        report = gold.run(cfg, write=True)
        _log(event="gold", run_id=report["run_id"], ok=report["ok"],
             rows={q: r.get("rows") for q, r in report["queries"].items()},
             errors={q: r["error"] for q, r in report["queries"].items() if r["status"] != "ok"})
    except Exception as exc:  # noqa: BLE001 - reported, the lake run's result stands
        _log(event="gold", status="error", error=type(exc).__name__)


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
    _meta(cfg)
    _digest(cfg)
    _weekly(cfg)
    _gold(cfg)
    _bi(cfg)
    _ops_alert(cfg)
    _log(event="done", ok=ok, seconds=round((datetime.now(timezone.utc) - started).total_seconds()))
    _heartbeat(ok)
    return 0 if ok else 1


def _names(private_on: bool) -> list[str]:
    """Datasets to validate: every public one, the private ones only when their schema is in use."""
    return [n for n, d in DATASETS.items() if not d.private or private_on]


def _run_sheet(cfg: Config) -> int:
    started = datetime.now(timezone.utc)
    code, summary = _backfill()
    _log(event="backfill", exit_code=code, **summary)
    since = (started.astimezone(TZ) - timedelta(days=1)).date()
    report = reconcile(cfg, _names(False), since, None)
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
        private_on = private.ensure_private(lock)
        store, prefix = open_store(cfg)
        drained = inbox.drain(lock, store=store, prefix=prefix, cfg=cfg, now=started)
        _log(event="drain", source=source, rows=drained["rows"], marked=drained.get("marked", 0),
             superseded=drained.get("superseded", 0),
             datasets=drained["datasets"], coerce_errors=drained.get("coerce_errors", {}),
             raw={n: r["written"] for n, r in drained.get("raw", {}).items()}, curated=drained.get("curated", {}),
             config_coerce_errors=drained.get("config_coerce_errors", {}))
        # Every accepted row marked, and no config row held back for a coercion error (those stay in
        # the inbox and fail each run until a corrected row replaces them).
        ok = drained.get("unmarked", 0) == 0 and not drained.get("config_coerce_errors")
        pdrained: dict[str, object] = {}
        if private_on:
            pdrained = private.drain(lock, store=store, prefix=prefix, cfg=cfg, now=started)
            _log(event="drain_private", rows=pdrained["rows"], marked=pdrained.get("marked", 0),
                 datasets=pdrained["datasets"], coerce_errors=pdrained.get("coerce_errors", {}),
                 raw={n: r["written"] for n, r in pdrained.get("raw", {}).items()},  # type: ignore[union-attr]
                 curated=pdrained.get("curated", {}))
            ok = ok and pdrained.get("unmarked", 0) == 0
        else:
            _log(event="drain_private", status="schema_missing", note="x_intel_private not created yet; skipped")
        since = (started.astimezone(TZ) - timedelta(days=1)).date()
        if source == "both":
            catchup_at = datetime.now(timezone.utc) - timedelta(minutes=1)  # before the Sheet read
            code, summary = _backfill()
            _log(event="backfill", exit_code=code, **summary)
            ok = ok and code == 0
            # The catch-up wrote config as the Sheet looked when it was read; put back bot edits sent since.
            _log(event="reapply_config", upserted=inbox.reapply_config(lock, catchup_at))
            snapshot_at = datetime.now(timezone.utc)  # rows the bots send after this may miss the read
            sheet = backfill.load(cfg, _names(private_on))
            report = reconcile(cfg, _names(private_on), since, None, preloaded=sheet)
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
            report = reconcile(cfg, _names(private_on), since, None, sheet=False)
        failed = {n: r["reasons"] for n, r in report["datasets"].items() if not r["ok"]}
        landed = inbox.landed(lock, store, prefix, started - timedelta(days=1))
        if private_on:
            landed |= private.landed(lock, store, prefix, started - timedelta(days=1))
        lost = {n: k for n, k in landed.items() if k}
        _log(event="validate", since=since, source=source, ok=report["ok"] and not lost, failed=failed,
             drained_missing_from_raw=lost)
        # A drained row with a coercion error fails the run even when its ingest date is older than the
        # validate window (a delayed or replayed row); the run then also skips the prune.
        ok = (ok and report["ok"] and not lost and not drained.get("coerce_errors")
              and not pdrained.get("coerce_errors"))
        both = [drained, pdrained]
        inbox.record_drain(
            lock, batch_id=str(drained.get("batch_id") or pdrained.get("batch_id") or f"drain-{started:%Y%m%dT%H%M%SZ}"),
            started_at=started, source=source,
            rows_by_dataset={n: k for d in both for n, k in dict(d.get("datasets") or {}).items()},  # type: ignore[arg-type]
            raw_written={n: r["written"] for d in both for n, r in dict(d.get("raw") or {}).items()},  # type: ignore[arg-type]
            curated={n: r for d in both for n, r in dict(d.get("curated") or {}).items()},  # type: ignore[arg-type]
            coerce_errors={n: r for d in both for n, r in dict(d.get("coerce_errors") or {}).items()},  # type: ignore[arg-type]
            missing_from_raw=landed, ok=ok)
        if ok:
            _log(event="prune", deleted=inbox.prune(lock, started))
            if private_on:
                _log(event="prune_private", deleted=private.prune(lock, started))
        # Still holding the lock: the serving refresh rewrites rows and clears the tail in separate
        # requests, so two runs refreshing at once could leave a tab short.
        return _finish(ok, started, cfg)
    finally:
        lock.close()  # releases the advisory lock


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from data_lake.observability import run_with_reporting

    sys.exit(run_with_reporting("x-lakehouse", main))
