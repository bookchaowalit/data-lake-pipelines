#!/usr/bin/env python3
"""Run one bounded News capture, replay it into Bronze, and check freshness.

The collector remains owned by ``book-news-scraping``. This control-plane
runner only coordinates the approved RSS capture, lake replay, and read-only
health gate. It never sends a request from the API and never enables external
writes. Cumulative history files are committed as append deltas, while the
validated capture stays in memory for an ingest or health retry. A source lock,
bounded timeout, and finite retry count make the command safe to attach to a
scheduler after the local cadence evidence is complete.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence


ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
DEFAULT_LAKE_URI = str(ROOT / "data" / "lake-local" / "scraping-captures-v2")

if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.ingest_scraping_captures import (  # noqa: E402
    CAPTURES,
    CaptureSpec,
    _map_records,
    _records,
    ingest_capture,
)
from data_lake.capture_lock import acquire_lock as _acquire_lock  # noqa: E402
from data_lake.workspace import OWNER_ROOT, product_path  # noqa: E402

NEWS_REPO = OWNER_ROOT / "public/book-apps/tools/content-news-scraper"
NEWS_COLLECTOR = NEWS_REPO / "scripts" / "run_feeds.py"
NEWS_CAPTURE_DIR = NEWS_REPO / "data" / "exported"
# Share the lock with the domain-owned legacy cron entry so an eventual
# scheduler migration cannot run the collector twice at the same time.
DEFAULT_LOCK_PATH = NEWS_REPO / "data" / "news-feeds.lock"


NEWS_CAPTURES: tuple[CaptureSpec, ...] = tuple(
    spec for spec in CAPTURES if spec.product == "news"
)
# The four feed adapters publish these stable source labels into the News
# contract.  Health checks use the labels instead of the aggregate newest row
# so one healthy feed cannot hide a silent or stale sibling.
NEWS_FEEDS: tuple[str, ...] = (
    "Matichon",
    "Bangkok Post Business",
    "Blognone",
    "Notebookspec",
)
MAX_CAPTURE_BYTES = 32 * 1024 * 1024


@dataclass(frozen=True)
class PreparedCapture:
    """Validated bytes and mapped records for one immutable ingest batch."""

    spec: CaptureSpec
    raw: bytes
    records: tuple[dict[str, Any], ...]
    capture_mode: str


class NewsCycleError(RuntimeError):
    """A bounded News cycle failed a safe operational gate."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class LockBusyError(NewsCycleError):
    """Another News cycle owns the source lock."""


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _latest_observed_at(items: Sequence[dict[str, Any]]) -> datetime | None:
    latest: datetime | None = None
    for item in items:
        for field in ("updated_at", "observed_at", "event_time", "captured_at"):
            parsed = _parse_timestamp(item.get(field))
            if parsed is not None and (latest is None or parsed > latest):
                latest = parsed
            if parsed is not None:
                break
    return latest


@contextmanager
def acquire_lock(path: Path, *, timeout_seconds: float = 0.0) -> Iterator[None]:
    """Acquire an advisory source lock, failing closed after a short wait."""
    with _acquire_lock(
        path,
        timeout_seconds=timeout_seconds,
        busy_error=LockBusyError,
    ):
        yield


def _run_collector(*, limit: int, timeout_seconds: float) -> None:
    if limit < 1:
        raise ValueError("collector limit must be positive")
    if timeout_seconds <= 0:
        raise ValueError("collector timeout must be positive")
    if not NEWS_COLLECTOR.is_file() or not NEWS_REPO.is_dir():
        raise NewsCycleError("collector_missing")
    command = [
        sys.executable,
        str(NEWS_COLLECTOR),
        "--limit",
        str(limit),
        "--output-dir",
        str(NEWS_CAPTURE_DIR),
    ]
    try:
        result = subprocess.run(
            command,
            cwd=NEWS_REPO,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
            env={
                **os.environ,
                "FREE_ONLY": "true",
                "ALLOW_PAID_PROVIDERS": "false",
                "ALLOW_EXTERNAL_WRITES": "false",
            },
        )
    except subprocess.TimeoutExpired as exc:
        raise NewsCycleError("collector_timeout") from exc
    except OSError as exc:
        raise NewsCycleError(f"collector_{type(exc).__name__}") from exc
    if result.returncode != 0:
        # Collector stdout/stderr can contain source content. Keep the
        # operator report to a stable error class and do not echo payloads.
        raise NewsCycleError("collector_failed")


def _read_capture_bytes(spec: CaptureSpec) -> bytes:
    path = product_path(spec.path)
    if not path.is_file():
        raise NewsCycleError("capture_missing")
    try:
        with path.open("rb") as capture:
            raw = capture.read(MAX_CAPTURE_BYTES + 1)
    except OSError as exc:
        raise NewsCycleError(f"capture_{type(exc).__name__}") from exc
    if len(raw) > MAX_CAPTURE_BYTES:
        raise NewsCycleError("capture_ValueError")
    return raw


def _history_delta(before: bytes, after: bytes) -> tuple[bytes | None, str]:
    """Return a header plus only newly appended history rows.

    A history CSV is a cumulative projection.  Prefix checking prevents a
    truncation, rewrite, or partial append from being mistaken for a safe
    delta and duplicated in Bronze.  ``None`` means the source did not append
    anything during this cycle.
    """
    if not before:
        return after, "full"
    if not after.startswith(before):
        raise NewsCycleError("capture_history_rewrite")
    if not before.endswith((b"\n", b"\r")):
        raise NewsCycleError("capture_history_boundary")
    appended = after[len(before) :]
    if not appended.strip():
        return None, "unchanged"
    header_end = before.find(b"\n")
    if header_end < 0:
        raise NewsCycleError("capture_history_header")
    return before[: header_end + 1] + appended, "append_delta"


def _history_baselines() -> dict[str, bytes]:
    baselines: dict[str, bytes] = {}
    for spec in NEWS_CAPTURES:
        if not spec.history:
            continue
        path = product_path(spec.path)
        if not path.is_file():
            baselines[spec.path] = b""
            continue
        try:
            with path.open("rb") as capture:
                raw = capture.read(MAX_CAPTURE_BYTES + 1)
        except OSError as exc:
            raise NewsCycleError(f"capture_{type(exc).__name__}") from exc
        if len(raw) > MAX_CAPTURE_BYTES:
            raise NewsCycleError("capture_ValueError")
        baselines[spec.path] = raw
    return baselines


def _prepare_captures(
    history_before: dict[str, bytes],
) -> tuple[tuple[PreparedCapture, ...], list[dict[str, Any]]]:
    prepared: list[PreparedCapture] = []
    validated: list[dict[str, Any]] = []
    for spec in NEWS_CAPTURES:
        full_raw = _read_capture_bytes(spec)
        records_raw = full_raw
        capture_mode = "snapshot"
        if spec.history:
            delta, capture_mode = _history_delta(
                history_before.get(spec.path, b""), full_raw
            )
            if delta is None:
                validated.append(
                    {
                        "dataset": spec.dataset,
                        "history": True,
                        "record_count": 0,
                        "unchanged": True,
                    }
                )
                continue
            records_raw = delta
        try:
            parsed = _records(records_raw, spec.id_fields, spec.event_time_field)
            mapped = tuple(_map_records(spec, parsed))
        except Exception as exc:  # noqa: BLE001 - report only a safe class
            raise NewsCycleError(f"capture_{type(exc).__name__}") from exc
        if not mapped:
            if spec.history:
                validated.append(
                    {
                        "dataset": spec.dataset,
                        "history": True,
                        "record_count": 0,
                        "unchanged": True,
                    }
                )
                continue
            raise NewsCycleError("capture_empty")
        prepared.append(
            PreparedCapture(
                spec=spec,
                raw=full_raw,
                records=mapped,
                capture_mode=capture_mode,
            )
        )
        validated.append(
            {
                "dataset": spec.dataset,
                "history": spec.history,
                "record_count": len(mapped),
                "capture_mode": capture_mode,
            }
        )
    return tuple(prepared), validated


def _validate_captures(
    lake_uri: str,
    *,
    history_before: dict[str, bytes] | None = None,
) -> tuple[tuple[PreparedCapture, ...], list[dict[str, Any]]]:
    """Validate all News captures and retain their exact bytes for replay."""
    del lake_uri  # retained in the private signature for caller compatibility
    return _prepare_captures(history_before or {})


def _commit_captures(
    lake_uri: str,
    prepared: Sequence[PreparedCapture] | None = None,
) -> list[dict[str, Any]]:
    """Commit one validated capture snapshot, replaying it on transient errors."""
    committed: list[dict[str, Any]] = []
    captures = prepared
    if captures is None:
        # Compatibility path for direct callers that predate capture replay.
        captures = tuple(
            PreparedCapture(spec=spec, raw=_read_capture_bytes(spec), records=tuple(), capture_mode="snapshot")
            for spec in NEWS_CAPTURES
        )
    for capture in captures:
        spec = capture.spec
        try:
            result = ingest_capture(
                spec,
                lake_uri=lake_uri,
                update_lineage=True,
                raw_override=capture.raw,
                records_override=list(capture.records) if capture.records else None,
                capture_mode=capture.capture_mode,
            )
        except Exception as exc:  # noqa: BLE001 - retry is bounded and safe
            raise NewsCycleError(f"ingest_{type(exc).__name__}") from exc
        committed.append(
            {
                "dataset": spec.dataset,
                "history": spec.history,
                "record_count": int(result.get("record_count", 0)),
                "manifest_existed": bool(result.get("manifest_existed")),
                "bronze_existed": bool(result.get("bronze_existed")),
                "capture_mode": capture.capture_mode,
            }
        )
    return committed


def _load_news_store():
    if str(NEWS_REPO) not in sys.path:
        sys.path.insert(0, str(NEWS_REPO))
    from news_data import store  # type: ignore

    return store


def _feed_name(item: dict[str, Any]) -> str:
    return str(item.get("source") or item.get("publisher") or "").strip()


def _feed_error_token(value: str) -> str:
    token = "".join(char.lower() if char.isalnum() else "_" for char in value)
    return token.strip("_") or "unknown"


def check_news_health(
    lake_uri: str,
    *,
    max_age_hours: float = 24.0,
    store_module: Any | None = None,
) -> dict[str, Any]:
    """Check current/history Bronze reads and a bounded freshness threshold."""
    if max_age_hours <= 0:
        raise ValueError("freshness threshold must be positive")
    store = store_module or _load_news_store()
    try:
        current = store.load_records(data_lake_uri=lake_uri)
        history = store.load_history(data_lake_uri=lake_uri)
    except Exception as exc:  # noqa: BLE001 - health must fail closed
        raise NewsCycleError(f"health_{type(exc).__name__}") from exc
    current_items = current.get("items") if isinstance(current, dict) else None
    history_items = history.get("items") if isinstance(history, dict) else None
    if not isinstance(current_items, list) or not isinstance(history_items, list):
        raise NewsCycleError("health_invalid_shape")
    if not current_items or not history_items:
        raise NewsCycleError("health_empty")
    if current.get("data_status") != "ok":
        raise NewsCycleError(f"health_current_{current.get('data_status', 'unknown')}")
    if history.get("data_status") not in {"ok", "stale"}:
        raise NewsCycleError(f"health_history_{history.get('data_status', 'unknown')}")
    if current.get("read_mode") != "parquet" or current.get("read_fallback") != "error":
        raise NewsCycleError("health_read_mode")
    expected_feeds = {name.casefold(): name for name in NEWS_FEEDS}
    per_feed: dict[str, dict[str, Any]] = {}
    for display_name in NEWS_FEEDS:
        feed_items = [
            item
            for item in current_items
            if _feed_name(item).casefold() == display_name.casefold()
        ]
        feed_latest = _latest_observed_at(feed_items)
        feed_age_hours: float | None = None
        feed_status = "empty"
        if feed_latest is not None:
            feed_age_hours = max(
                0.0,
                (datetime.now(timezone.utc) - feed_latest).total_seconds() / 3600.0,
            )
            feed_status = "stale" if feed_age_hours > max_age_hours else "ok"
        elif feed_items:
            feed_status = "timestamp_unknown"
        per_feed[display_name] = {
            "status": feed_status,
            "record_count": len(feed_items),
            "latest_observed_at": (
                feed_latest.replace(microsecond=0).isoformat().replace("+00:00", "Z")
                if feed_latest is not None
                else None
            ),
            "age_hours": (
                round(feed_age_hours, 3) if feed_age_hours is not None else None
            ),
            "max_age_hours": max_age_hours,
        }
    for display_name, feed_report in per_feed.items():
        if feed_report["status"] != "ok":
            raise NewsCycleError(
                f"health_feed_{feed_report['status']}_{_feed_error_token(display_name)}"
            )
    latest = _latest_observed_at(current_items)
    if latest is None:
        raise NewsCycleError("health_timestamp_unknown")
    age_hours = max(0.0, (datetime.now(timezone.utc) - latest).total_seconds() / 3600.0)
    if age_hours > max_age_hours:
        raise NewsCycleError("health_stale")
    return {
        "data_status": "ok",
        "record_count": len(current_items),
        "history_count": len(history_items),
        "latest_observed_at": latest.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "age_hours": round(age_hours, 3),
        "max_age_hours": max_age_hours,
        "read_mode": current.get("read_mode"),
        "source_kind": current.get("source_kind"),
        "attribution_required": True,
        "feeds": per_feed,
        "unknown_feed_count": sum(
            1
            for item in current_items
            if _feed_name(item).casefold() not in expected_feeds
        ),
    }


def run_news_cycle(
    *,
    lake_uri: str,
    lock_path: Path = DEFAULT_LOCK_PATH,
    limit: int = 50,
    collector_timeout_seconds: float = 120.0,
    attempts: int = 2,
    retry_backoff_seconds: float = 1.0,
    lock_timeout_seconds: float = 0.0,
    max_age_hours: float = 24.0,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run capture, validation, ingest, and health as one bounded cycle."""
    if not str(lake_uri).strip():
        raise ValueError("lake URI is required")
    if attempts < 1:
        raise ValueError("attempts must be positive")
    if retry_backoff_seconds < 0:
        raise ValueError("retry backoff must be non-negative")
    started_at = utc_now_iso()
    if dry_run:
        return {
            "status": "dry-run",
            "product": "news",
            "schema_version": "news.v1",
            "attempts": 0,
            "started_at": started_at,
            "completed_at": utc_now_iso(),
            "plan": {
                "limit": limit,
                "collector_timeout_seconds": collector_timeout_seconds,
                "network": "not-used",
                "writes": "not-used",
            },
        }
    try:
        with acquire_lock(lock_path, timeout_seconds=lock_timeout_seconds):
            last_error = "unknown"
            history_before: dict[str, bytes] | None = None
            prepared: tuple[PreparedCapture, ...] | None = None
            validated: list[dict[str, Any]] | None = None
            for attempt in range(1, attempts + 1):
                try:
                    if prepared is None:
                        # Keep the cycle-start baseline across collector
                        # retries. If a collector fails after appending some
                        # valid history rows, the next successful capture can
                        # still ingest that complete append range.
                        if history_before is None:
                            history_before = _history_baselines()
                        _run_collector(
                            limit=limit,
                            timeout_seconds=collector_timeout_seconds,
                        )
                        validation = _validate_captures(
                            lake_uri,
                            history_before=history_before,
                        )
                        if isinstance(validation, tuple) and len(validation) == 2:
                            prepared, validated = validation
                        else:
                            # Keep tests and older internal callers that stubbed
                            # the former list-only validator working.  The real
                            # validator always returns the replay tuple above.
                            validated = list(validation)  # type: ignore[arg-type]
                    committed = _commit_captures(lake_uri, prepared)
                    health = check_news_health(
                        lake_uri,
                        max_age_hours=max_age_hours,
                    )
                    return {
                        "status": "ok",
                        "product": "news",
                        "schema_version": "news.v1",
                        "attempts": attempt,
                        "started_at": started_at,
                        "completed_at": utc_now_iso(),
                        "validated": validated,
                        "committed": committed,
                        "health": health,
                    }
                except NewsCycleError as exc:
                    last_error = exc.code
                    # Collector/validation failures need a fresh source read.
                    # Once bytes are validated, ingest and health retries replay
                    # exactly that in-memory capture and never hit RSS again.
                    if not (
                        prepared is not None
                        and (exc.code.startswith("ingest_") or exc.code.startswith("health_"))
                    ):
                        prepared = None
                        validated = None
                    if attempt < attempts:
                        delay = retry_backoff_seconds * (2 ** (attempt - 1))
                        if delay:
                            time.sleep(delay)
            return {
                "status": "failed",
                "product": "news",
                "schema_version": "news.v1",
                "attempts": attempts,
                "error": last_error,
                "started_at": started_at,
                "completed_at": utc_now_iso(),
            }
    except LockBusyError as exc:
        return {
            "status": "locked",
            "product": "news",
            "schema_version": "news.v1",
            "attempts": 0,
            "error": exc.code,
            "started_at": started_at,
            "completed_at": utc_now_iso(),
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_LAKE_URI))
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--collector-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--attempts", type=int, default=2)
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--lock-timeout-seconds", type=float, default=0.0)
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--max-age-hours", type=float, default=24.0)
    parser.add_argument("--dry-run", action="store_true", help="Print the bounded plan without collection or writes")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = run_news_cycle(
            lake_uri=args.lake_uri,
            lock_path=args.lock_path,
            limit=args.limit,
            collector_timeout_seconds=args.collector_timeout_seconds,
            attempts=args.attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
            lock_timeout_seconds=args.lock_timeout_seconds,
            max_age_hours=args.max_age_hours,
            dry_run=args.dry_run,
        )
    except ValueError as exc:
        print(f"News cycle blocked: {exc}", file=sys.stderr)
        return 2
    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(
            "news cycle: "
            f"{report['status']} (attempts={report['attempts']}, "
            f"error={report.get('error', 'none')})"
        )
        health = report.get("health")
        if isinstance(health, dict):
            print(
                "  health: "
                f"{health.get('record_count', 0)} current / "
                f"{health.get('history_count', 0)} history, "
                f"latest={health.get('latest_observed_at', 'unknown')}"
            )
    return 0 if report["status"] in {"ok", "dry-run"} else 2 if report["status"] == "locked" else 1


if __name__ == "__main__":
    from data_lake.observability import run_with_reporting

    raise SystemExit(run_with_reporting("news", main))
