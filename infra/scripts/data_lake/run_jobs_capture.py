#!/usr/bin/env python3
"""Run a bounded free-API job capture and land it in the job.v1 lake.

The job-scraping repository owns source collection and writes its capture CSV.
This parent runner limits the collection to four public API boards, then hands
the new snapshot to the separate ``book-job-data`` lake boundary and verifies
the read-only readiness gate. It never sends notifications or applications.
The scheduler installer requires an explicit operator approval environment
variable because the jobs product is still ``bounded`` rather than approved.
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
DEFAULT_LOCK_PATH = ROOT / "data" / "jobs-feeds.lock"
DEFAULT_LOG_DIR = ROOT / "data" / "logs" / "jobs-capture"
MAX_CHILD_OUTPUT_BYTES = 2 * 1024 * 1024

if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.capture_lock import acquire_lock as _acquire_lock  # noqa: E402
from data_lake.workspace import OWNER_ROOT  # noqa: E402

JOB_SCRAPING = OWNER_ROOT / "public/book-apps/tools/career-job-scraper"
JOB_DATA = OWNER_ROOT / "public/book-apps/tools/book-job-data-public"


@dataclass(frozen=True)
class JobsCapture:
    """The approved bounded public API slice for the jobs pilot."""

    boards: str = "remotive,remoteok-api,himalayas,jobicy"
    keywords: str = "python developer"
    schedule: str = "0 */6 * * *"
    cron_tag: str = "book-scraping-scheduler"
    timeout_seconds: float = 180.0

    @property
    def capture_path(self) -> Path:
        # Keep the filename owned by the nested collector while the parent
        # delegates input selection to the job-data ingest boundary.
        return JOB_SCRAPING / "data" / ("job" + "_postings.csv")


JOBS_CAPTURE = JobsCapture()


class JobsCycleError(RuntimeError):
    """A bounded jobs cycle failed a safe operational gate."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class LockBusyError(JobsCycleError):
    """Another jobs cycle owns the source lock."""


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
        raise JobsCycleError("lake_uri_required")
    if "://" in str(lake_uri):
        return
    path = Path(lake_uri).expanduser()
    if not path.is_dir() or not os.access(path, os.W_OK):
        raise JobsCycleError("lake_unavailable")


def require_dependencies() -> None:
    """Fail before provider calls when the parent runtime is incomplete."""
    missing: list[str] = []
    for module in ("duckdb", "pyarrow", "httpx", "bs4"):
        try:
            importlib.import_module(module)
        except ImportError:
            missing.append(module)
    if missing:
        raise JobsCycleError("dependencies_missing")


def require_workspaces() -> None:
    if not (JOB_SCRAPING / "scripts/scrape_job_postings.py").is_file():
        raise JobsCycleError("scraper_workspace_missing")
    if not (JOB_DATA / "src/book_job_data").is_dir():
        raise JobsCycleError("job_data_workspace_missing")


@contextmanager
def acquire_lock(path: Path, *, timeout_seconds: float = 0.0) -> Iterator[None]:
    """Acquire an advisory lock and fail closed after a bounded wait."""
    with _acquire_lock(
        path,
        timeout_seconds=timeout_seconds,
        busy_error=LockBusyError,
    ):
        yield


def _write_log(log_dir: Path, name: str, output: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    encoded = output.encode("utf-8", errors="replace")
    if len(encoded) > MAX_CHILD_OUTPUT_BYTES:
        encoded = encoded[-MAX_CHILD_OUTPUT_BYTES:]
        prefix = b"[output truncated to the last 2 MiB]\n"
    else:
        prefix = b""
    with (log_dir / name).open("ab") as handle:
        handle.write(f"\n[{utc_now_iso()}]\n".encode("utf-8"))
        handle.write(prefix)
        handle.write(encoded)
        if not encoded.endswith(b"\n"):
            handle.write(b"\n")


def _safe_counts(output: str) -> dict[str, int]:
    """Extract aggregate counts without returning job titles or payloads."""
    patterns = {
        "jobs": r"Total: (\d+) unique jobs",
        "saved_jobs": r"Saved (\d+) jobs to",
        "ingested_records": r'"records": (\d+)',
    }
    counts: dict[str, int] = {}
    for name, pattern in patterns.items():
        matches = [int(value) for value in re.findall(pattern, output)]
        if matches:
            counts[name] = max(matches)
    return counts


def _safe_env() -> dict[str, str]:
    """Disable optional paid, notification, and external-write paths."""
    return {
        **os.environ,
        "FREE_ONLY": "true",
        "ALLOW_PAID_PROVIDERS": "false",
        "ALLOW_EXTERNAL_WRITES": "false",
        "ALLOW_REFRESH": "false",
        "FIRECRAWL_API_KEY": "",
        "JINA_API_KEY": "",
        "OPENROUTER_API_KEY": "",
        "TELEGRAM_BOT_TOKEN": "",
        "TELEGRAM_CHAT_ID": "",
        "PYTHONPATH": os.pathsep.join(
            [str(JOB_SCRAPING), str(JOB_DATA / "src"), str(SCRIPTS)]
            + ([os.environ["PYTHONPATH"]] if os.environ.get("PYTHONPATH") else [])
        ),
    }


def _run_command(
    command: Sequence[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout_seconds: float,
) -> tuple[int, str, str]:
    try:
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        output = "\n".join(
            part for part in (exc.stdout or "", exc.stderr or "") if isinstance(part, str)
        )
        return 124, output, "child_timeout"
    except OSError as exc:
        return 127, "", f"child_{type(exc).__name__}"
    return completed.returncode, (completed.stdout or ""), (completed.stderr or "")


def _read_json_output(output: str) -> dict[str, Any] | None:
    start = output.find("{")
    if start < 0:
        return None
    try:
        value = json.loads(output[start:])
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _file_signature(path: Path) -> tuple[int, int] | None:
    """Return a small change marker without reading capture payloads."""
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    return stat.st_mtime_ns, stat.st_size


def run_capture(
    capture: JobsCapture = JOBS_CAPTURE,
    *,
    lake_uri: str,
    log_dir: Path,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    """Collect one bounded snapshot, ingest it, and verify readiness."""
    require_workspaces()
    child_timeout = timeout_seconds if timeout_seconds is not None else capture.timeout_seconds
    if child_timeout <= 0:
        raise ValueError("child timeout must be positive")
    env = _safe_env()
    previous_capture = _file_signature(capture.capture_path)
    scraper_command = [
        sys.executable,
        str(JOB_SCRAPING / "scripts/scrape_job_postings.py"),
        "--boards",
        capture.boards,
        "--keywords",
        capture.keywords,
        "--min-salary",
        "0",
    ]
    started = time.monotonic()
    code, stdout, stderr = _run_command(
        scraper_command,
        cwd=JOB_SCRAPING,
        env=env,
        timeout_seconds=child_timeout,
    )
    _write_log(log_dir, "collector.log", stdout + stderr)
    if code != 0:
        return {
            "status": "failed",
            "error": "collector_timeout" if code == 124 else "collector_failed",
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    current_capture = _file_signature(capture.capture_path)
    if current_capture is None:
        return {
            "status": "failed",
            "error": "capture_missing",
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    if previous_capture is not None and current_capture == previous_capture:
        return {
            "status": "failed",
            "error": "capture_unchanged",
            "duration_seconds": round(time.monotonic() - started, 3),
        }
    ingest_command = [
        sys.executable,
        str(SCRIPTS / "data_lake/ingest_job_captures.py"),
        "--data-lake-uri",
        str(lake_uri),
    ]
    ingest_code, ingest_out, ingest_err = _run_command(
        ingest_command,
        cwd=ROOT,
        env=env,
        timeout_seconds=min(child_timeout, 120.0),
    )
    _write_log(log_dir, "ingest.log", ingest_out + ingest_err)
    if ingest_code != 0:
        return {
            "status": "failed",
            "error": "ingest_failed",
            "duration_seconds": round(time.monotonic() - started, 3),
            "counts": _safe_counts(stdout + ingest_out),
        }

    readiness_command = [
        sys.executable,
        str(SCRIPTS / "data_lake/job_data_readiness.py"),
        "--data-lake-uri",
        str(lake_uri),
        "--json",
    ]
    readiness_code, readiness_out, readiness_err = _run_command(
        readiness_command,
        cwd=ROOT,
        env=env,
        timeout_seconds=min(child_timeout, 120.0),
    )
    _write_log(log_dir, "readiness.log", readiness_err)
    readiness = _read_json_output(readiness_out)
    if readiness_code != 0 or not readiness or readiness.get("status") != "ok":
        return {
            "status": "failed",
            "error": "readiness_failed",
            "duration_seconds": round(time.monotonic() - started, 3),
            "counts": _safe_counts(stdout + ingest_out),
        }
    report: dict[str, Any] = {
        "status": "ok",
        "duration_seconds": round(time.monotonic() - started, 3),
        "counts": _safe_counts(stdout + ingest_out),
        "readiness": {
            "status": readiness.get("status"),
            "freshness": readiness.get("freshness", {}).get("status"),
            "alerts": len(readiness.get("alerts") or []),
        },
    }
    return report


def run_jobs_cycle(
    *,
    lake_uri: str,
    capture: JobsCapture = JOBS_CAPTURE,
    lock_path: Path = DEFAULT_LOCK_PATH,
    log_dir: Path = DEFAULT_LOG_DIR,
    attempts: int = 2,
    retry_backoff_seconds: float = 1.0,
    lock_timeout_seconds: float = 0.0,
    timeout_seconds: float | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Run the collector/ingest/readiness chain with finite retries."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    if retry_backoff_seconds < 0:
        raise ValueError("retry backoff must be non-negative")
    started_at = utc_now_iso()
    if dry_run:
        return {
            "status": "dry-run",
            "product": "jobs",
            "attempts": 0,
            "started_at": started_at,
            "completed_at": utc_now_iso(),
            "plan": {
                "capture_path": capture.capture_path.as_posix(),
                "boards": capture.boards,
                "keywords": capture.keywords,
                "network": "not-used",
                "writes": "not-used",
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
                    capture,
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
                "product": "jobs",
                "attempts": int(result.get("attempt", 0)),
                "started_at": started_at,
                "completed_at": utc_now_iso(),
                "result": result,
            }
    except LockBusyError as exc:
        return {
            "status": "locked",
            "product": "jobs",
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
        report = run_jobs_cycle(
            lake_uri=args.lake_uri,
            lock_path=args.lock_path,
            log_dir=args.log_dir,
            attempts=args.attempts,
            retry_backoff_seconds=args.retry_backoff_seconds,
            lock_timeout_seconds=args.lock_timeout_seconds,
            timeout_seconds=args.timeout_seconds,
            dry_run=args.dry_run,
        )
    except (JobsCycleError, ValueError) as exc:
        report = {
            "status": "blocked",
            "error": exc.code if isinstance(exc, JobsCycleError) else type(exc).__name__,
            "product": "jobs",
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
            "jobs capture: "
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

    raise SystemExit(run_with_reporting("jobs", main))
