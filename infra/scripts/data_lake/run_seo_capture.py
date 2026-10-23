#!/usr/bin/env python3
"""Run a bounded, lake-first owned-page SEO capture.

The SEO product owns provider calls and its CSV projection. This control-plane
runner supplies one explicit lake URI, a source lock, a bounded timeout, and
finite retries. It keeps child output in a local log and returns only stable
run metadata and aggregate counts. Paid SERP providers and external writes are
forced off for every child process.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import re
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
DEFAULT_LOCK_PATH = ROOT / "data" / "seo-feeds.lock"
DEFAULT_LOG_DIR = ROOT / "data" / "logs" / "seo-capture"
MAX_CHILD_OUTPUT_BYTES = 2 * 1024 * 1024

if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.capture_lock import acquire_lock as _acquire_lock  # noqa: E402
from data_lake.workspace import OWNER_ROOT  # noqa: E402


@dataclass(frozen=True)
class SeoCapture:
    """The approved bounded public-page query used by the scheduler."""

    keywords: str = "python developer thailand"
    domains: str = "bookchaowalit.com,chaowalit.com"
    location: str = "global"
    device: str = "desktop"
    cron_tag: str = "book-seo-data-feeds"
    schedule: str = "30 8 * * *"
    timeout_seconds: float = 180.0

    @property
    def workspace(self) -> Path:
        return OWNER_ROOT / "private/book-products/marketing-seo-ranking-data"

    @property
    def output_dir(self) -> Path:
        return self.workspace / "data" / "exported"


SEO_CAPTURE = SeoCapture()


class SeoCycleError(RuntimeError):
    """A bounded SEO cycle failed a safe operational gate."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class LockBusyError(SeoCycleError):
    """Another SEO cycle owns the source lock."""


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def validate_lake_uri(lake_uri: str) -> None:
    """Require an explicit existing writable local lake or a URI scheme."""
    if not str(lake_uri).strip():
        raise SeoCycleError("lake_uri_required")
    if "://" in str(lake_uri):
        return
    path = Path(lake_uri).expanduser()
    if not path.is_dir() or not os.access(path, os.W_OK):
        raise SeoCycleError("lake_unavailable")


def require_dependencies() -> None:
    """Fail before provider calls when the parent runtime is incomplete."""
    missing: list[str] = []
    for module in ("duckdb", "pyarrow", "httpx", "bs4"):
        try:
            importlib.import_module(module)
        except ImportError:
            missing.append(module)
    if missing:
        raise SeoCycleError("dependencies_missing")


@contextmanager
def acquire_lock(path: Path, *, timeout_seconds: float = 0.0) -> Iterator[None]:
    """Acquire an advisory lock and fail closed after a bounded wait."""
    with _acquire_lock(
        path,
        timeout_seconds=timeout_seconds,
        busy_error=LockBusyError,
    ):
        yield


def _safe_counts(output: str) -> dict[str, int]:
    """Extract aggregate counts without returning provider rows or payloads."""
    patterns = {
        "keywords": r"Keywords: (\d+)",
        "landed_batches": r"Landed batches: (\d+)",
        "lake_records": r"Lake [^\n]+records=(\d+)",
    }
    counts: dict[str, int] = {}
    for name, pattern in patterns.items():
        matches = [int(value) for value in re.findall(pattern, output)]
        if matches:
            counts[name] = max(matches)
    return counts


def _write_child_log(log_dir: Path, output: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / "seo.log"
    encoded = output.encode("utf-8", errors="replace")
    if len(encoded) > MAX_CHILD_OUTPUT_BYTES:
        encoded = encoded[-MAX_CHILD_OUTPUT_BYTES:]
        prefix = b"[output truncated to the last 2 MiB]\n"
    else:
        prefix = b""
    with path.open("ab") as handle:
        handle.write(f"\n[{utc_now_iso()}]\n".encode("utf-8"))
        handle.write(prefix)
        handle.write(encoded)
        if not encoded.endswith(b"\n"):
            handle.write(b"\n")


def run_capture(
    capture: SeoCapture = SEO_CAPTURE,
    *,
    lake_uri: str,
    log_dir: Path,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Run the nested SEO lake-first CLI with a redacted result."""
    workspace = capture.workspace
    if not workspace.is_dir() or not (workspace / "src").is_dir():
        return {"status": "failed", "error": "workspace_missing"}
    child_timeout = (
        timeout_seconds if timeout_seconds is not None else capture.timeout_seconds
    )
    if child_timeout <= 0:
        raise ValueError("child timeout must be positive")
    command = [
        sys.executable,
        "-m",
        "book_seo.ingest",
        "--keywords",
        capture.keywords,
        "--domains",
        capture.domains,
        "--location",
        capture.location,
        "--device",
        capture.device,
        "--data-lake-uri",
        str(lake_uri),
        "--output-dir",
        str(capture.output_dir),
    ]
    env = {
        **os.environ,
        "FREE_ONLY": "true",
        "ALLOW_PAID_PROVIDERS": "false",
        "ALLOW_EXTERNAL_WRITES": "false",
        "ALLOW_REFRESH": "false",
        "SOLO_EMPIRE_DATA_LAKE_URI": str(lake_uri),
        "PYTHONPATH": os.pathsep.join(
            [str(workspace / "src"), str(workspace), str(SCRIPTS)]
            + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else [])
        ),
    }
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=workspace,
            env=env,
            capture_output=True,
            text=True,
            timeout=child_timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        output = "\n".join(
            part for part in (exc.stdout or "", exc.stderr or "") if isinstance(part, str)
        )
        _write_child_log(log_dir, output)
        return {
            "status": "failed",
            "error": "child_timeout",
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    except OSError as exc:
        return {
            "status": "failed",
            "error": f"child_{type(exc).__name__}",
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    output = (completed.stdout or "") + (completed.stderr or "")
    _write_child_log(log_dir, output)
    report: dict[str, Any] = {
        "status": "ok" if completed.returncode == 0 else "failed",
        "exit_code": completed.returncode,
        "duration_seconds": round(time.monotonic() - started, 3),
    }
    counts = _safe_counts(output)
    if counts:
        report["counts"] = counts
    if completed.returncode != 0:
        report["error"] = "child_failed"
    return report


def run_seo_cycle(
    *,
    lake_uri: str,
    lock_path: Path = DEFAULT_LOCK_PATH,
    log_dir: Path = DEFAULT_LOG_DIR,
    attempts: int = 2,
    retry_backoff_seconds: float = 1.0,
    lock_timeout_seconds: float = 0.0,
    timeout_seconds: float | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run the bounded SEO capture and retry only failed child attempts."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    if retry_backoff_seconds < 0:
        raise ValueError("retry backoff must be non-negative")
    started_at = utc_now_iso()
    if dry_run:
        return {
            "status": "dry-run",
            "product": "seo",
            "attempts": 0,
            "started_at": started_at,
            "completed_at": utc_now_iso(),
            "plan": {
                "network": "not-used",
                "writes": "not-used",
                "timeout_seconds": timeout_seconds,
            },
            "result": {},
        }
    validate_lake_uri(lake_uri)
    require_dependencies()
    try:
        with acquire_lock(lock_path, timeout_seconds=lock_timeout_seconds):
            result: dict[str, Any] = {}
            for attempt in range(1, attempts + 1):
                result = run_capture(
                    lake_uri=lake_uri,
                    log_dir=log_dir,
                    timeout_seconds=timeout_seconds,
                )
                result["attempt"] = attempt
                if result["status"] == "ok":
                    break
                if attempt < attempts and retry_backoff_seconds:
                    time.sleep(retry_backoff_seconds * (2 ** (attempt - 1)))
            return {
                "status": result.get("status", "failed"),
                "product": "seo",
                "attempts": int(result.get("attempt", 0)),
                "started_at": started_at,
                "completed_at": utc_now_iso(),
                "result": result,
            }
    except LockBusyError as exc:
        return {
            "status": "locked",
            "product": "seo",
            "attempts": 0,
            "error": exc.code,
            "started_at": started_at,
            "completed_at": utc_now_iso(),
            "result": {},
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lake-uri",
        default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_LAKE_URI),
    )
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--attempts", type=int, default=2)
    parser.add_argument("--retry-backoff-seconds", type=float, default=1.0)
    parser.add_argument("--lock-timeout-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Print the bounded plan without collection or writes")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = run_seo_cycle(
            lake_uri=args.lake_uri,
            lock_path=args.lock_path,
            log_dir=args.log_dir,
            attempts=args.attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
            lock_timeout_seconds=args.lock_timeout_seconds,
            timeout_seconds=args.timeout_seconds,
            dry_run=args.dry_run,
        )
    except (SeoCycleError, ValueError) as exc:
        report = {
            "status": "blocked",
            "error": exc.code if isinstance(exc, SeoCycleError) else type(exc).__name__,
            "product": "seo",
            "attempts": 0,
            "started_at": utc_now_iso(),
            "completed_at": utc_now_iso(),
            "result": {},
        }
    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        result = report.get("result", {})
        print(
            "seo capture: "
            f"{report['status']} (attempt={report.get('attempts', 0)}, "
            f"error={report.get('error', result.get('error', 'none'))})"
        )
    return (
        0
        if report["status"] in {"ok", "dry-run"}
        else 2
        if report["status"] in {"locked", "blocked"}
        else 1
    )


if __name__ == "__main__":
    from data_lake.observability import run_with_reporting

    raise SystemExit(run_with_reporting("seo", main))
