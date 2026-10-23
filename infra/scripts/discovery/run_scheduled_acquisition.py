#!/usr/bin/env python3
"""Run the local discovery acquisition and log metadata-only summaries."""

from __future__ import annotations

import json
import logging
from logging.handlers import RotatingFileHandler
from collections import Counter
from datetime import datetime, timezone
import os
from pathlib import Path
import subprocess
import sys
from time import gmtime


ROOT = Path(__file__).resolve().parents[3]
PORTFOLIO = ROOT / "infra/scripts/discovery/portfolio.py"
LOG_FILE = ROOT / "data/logs/discovery-portfolio-scheduled.log"
ACQUISITION_TIMEOUT_SECONDS = 3 * 60 * 60


def summarize_result(payload: dict) -> str:
    """Return counts and source IDs only; never include captured source data."""
    sources = payload.get("sources")
    if not isinstance(sources, list):
        sources = []

    counts = Counter(
        str(source.get("status", "unknown"))
        for source in sources
        if isinstance(source, dict)
    )
    rendered_counts = ",".join(f"{name}={counts[name]}" for name in sorted(counts)) or "none"
    failures = sorted(
        str(source.get("source_id") or source.get("product") or "unknown")
        for source in sources
        if isinstance(source, dict) and source.get("status") != "captured"
    )
    result = (
        f"sources={len(sources)}; statuses={rendered_counts}; "
        f"partial={str(bool(payload.get('partial', False))).lower()}"
    )
    if failures:
        result += "; non_captured_source_ids=" + ",".join(failures[:25])
        if len(failures) > 25:
            result += f",…(+{len(failures) - 25})"
    return result


def configure_logger() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("discovery.scheduled_acquisition")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=2_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
    formatter.converter = gmtime
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def main() -> int:
    logger = configure_logger()
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    logger.info("acquisition_started_at=%s", started)
    try:
        completed = subprocess.run(
            [sys.executable, "-X", "utf8", str(PORTFOLIO), "acquire"],
            cwd=ROOT,
            env={**os.environ, "PYTHONIOENCODING": "utf-8"},
            capture_output=True,
            text=True,
            encoding="utf-8",
            stdin=subprocess.DEVNULL,
            timeout=ACQUISITION_TIMEOUT_SECONDS,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            check=False,
        )
    except subprocess.TimeoutExpired:
        logger.error("acquisition_timed_out_after_seconds=%s", ACQUISITION_TIMEOUT_SECONDS)
        return 124
    except OSError as error:
        logger.error("acquisition_runner_failed=%s", type(error).__name__)
        return 1

    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError:
        logger.error(
            "acquisition_output_invalid_json; exit_code=%s; stderr_present=%s",
            completed.returncode,
            bool(completed.stderr),
        )
        return completed.returncode or 1

    if (
        not isinstance(payload, dict)
        or payload.get("contract") != "discovery.portfolio.v1"
        or payload.get("command") != "acquire"
    ):
        logger.error("acquisition_output_contract_invalid; stderr_present=%s", bool(completed.stderr))
        return completed.returncode or 1

    logger.info("acquisition_result %s", summarize_result(payload))
    if completed.stderr:
        logger.warning("acquisition_stderr_present=true")
    logger.info("acquisition_exit_code=%s", completed.returncode)
    if completed.returncode:
        return completed.returncode
    if payload.get("partial"):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
