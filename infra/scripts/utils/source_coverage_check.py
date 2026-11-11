#!/usr/bin/env python3
"""Check the source coverage registry (repository-catalog/registries/source-coverage.yaml).

Offline. Fails when a row misses a required field, uses an unknown value, breaks the
API -> MCP -> scraping order (scraping only where it is allowed and nothing better exists),
or when a batch is not complete (20 rows, a report file, a next batch). ``--summary`` prints
counts per verdict and status for the current batch.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PATH = ROOT / "repository-catalog" / "registries" / "source-coverage.yaml"
BATCH_SIZE = 20

REQUIRED = (
    "id", "name", "batch", "category", "popularity_rank", "method", "api_docs_url", "mcp_server",
    "auth_type", "rate_limit", "cost", "entities", "coverage_pct", "current_status", "tos_notes",
    "pii_level", "scores", "verdict", "verdict_reason", "owner_action", "evidence", "checked_at",
)
METHODS = {"api", "mcp", "scrape", "manual-export", "none"}
STATUSES = {"covered", "partial", "missing"}
VERDICTS = {"ingest-now", "later", "skip", "blocked-legal", "covered"}
PII = {"low", "medium", "high"}
SCORE_KEYS = ("value", "cost", "effort", "risk")
ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]*$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# A secret-looking value must never land in this file (names and URLs only).
SECRET_RE = re.compile(r"(sk_live_|sk_test_|rk_live_|ghp_|github_pat_|xox[abp]-|AKIA[0-9A-Z]{12}|-----BEGIN)")


def _check_source(row: dict[str, Any], errors: list[str], repo_root: Path) -> None:
    sid = row.get("id", "?")
    for field in REQUIRED:
        if field not in row:
            errors.append(f"{sid}: missing {field}")
    if not ID_RE.match(str(row.get("id", ""))):
        errors.append(f"{sid}: id must be kebab-case")
    if row.get("method") not in METHODS:
        errors.append(f"{sid}: method {row.get('method')!r} not in {sorted(METHODS)}")
    if row.get("current_status") not in STATUSES:
        errors.append(f"{sid}: current_status {row.get('current_status')!r}")
    if row.get("verdict") not in VERDICTS:
        errors.append(f"{sid}: verdict {row.get('verdict')!r}")
    if row.get("pii_level") not in PII:
        errors.append(f"{sid}: pii_level {row.get('pii_level')!r}")
    pct = row.get("coverage_pct")
    if not isinstance(pct, int) or not 0 <= pct <= 100:
        errors.append(f"{sid}: coverage_pct must be an integer 0-100")
    elif row.get("current_status") == "missing" and pct != 0:
        errors.append(f"{sid}: missing sources have coverage_pct 0")
    elif row.get("current_status") == "partial" and not 0 < pct < 100:
        errors.append(f"{sid}: partial sources have coverage_pct between 1 and 99")
    elif row.get("current_status") == "covered" and pct != 100:
        errors.append(f"{sid}: covered sources have coverage_pct 100")
    scores = row.get("scores") or {}
    for key in SCORE_KEYS:
        if not isinstance(scores.get(key), int) or not 1 <= scores[key] <= 5:
            errors.append(f"{sid}: scores.{key} must be 1-5")
    evidence = row.get("evidence") or []
    if not isinstance(evidence, list) or not evidence or not all(str(u).startswith("https://") for u in evidence):
        errors.append(f"{sid}: evidence needs at least one https URL")
    if not str(row.get("api_docs_url", "")).startswith("https://"):
        errors.append(f"{sid}: api_docs_url must be an https URL")
    if not isinstance(row.get("entities"), list) or not row.get("entities"):
        errors.append(f"{sid}: entities must be a non-empty list")
    if not DATE_RE.match(str(row.get("checked_at", ""))):
        errors.append(f"{sid}: checked_at must be YYYY-MM-DD")
    # Priority order API -> MCP -> scraping: a lower route needs the reason the better ones fail.
    if row.get("method") in {"mcp", "scrape"} and not row.get("api_gap"):
        errors.append(f"{sid}: method {row.get('method')} needs api_gap (why the official API is not enough)")
    if row.get("method") == "scrape" and not row.get("mcp_gap"):
        errors.append(f"{sid}: method scrape needs mcp_gap (why no MCP server is enough)")
    if row.get("method") == "scrape":
        if row.get("scraping_allowed") is not True:
            errors.append(f"{sid}: method scrape needs scraping_allowed: true (ToS and robots.txt checked)")
    if row.get("scraping_allowed") is False and row.get("method") == "scrape":
        errors.append(f"{sid}: scraping is not allowed for this source")
    verdict = row.get("verdict")
    pipeline = row.get("pipeline")
    if verdict == "ingest-now" and not (
        isinstance(pipeline, dict) and all(pipeline.get(k) for k in ("workflow", "heartbeat", "datasets"))
    ):
        errors.append(f"{sid}: ingest-now needs a pipeline (workflow, heartbeat, datasets)")
    elif isinstance(pipeline, dict) and pipeline.get("workflow") and not (repo_root / str(pipeline["workflow"])).is_file():
        errors.append(f"{sid}: pipeline workflow {pipeline['workflow']!r} not found")
    if verdict in {"covered", "ingest-now"} and (row.get("current_status") == "missing" or not pct):
        errors.append(f"{sid}: verdict {verdict} needs data on the platform (status not missing, coverage_pct > 0)")
    if verdict in {"later", "skip", "blocked-legal"} and not row.get("verdict_reason"):
        errors.append(f"{sid}: verdict {verdict} needs a verdict_reason")


def check(path: Path) -> tuple[list[str], dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    errors: list[str] = []
    if SECRET_RE.search(text):
        errors.append("file contains a secret-looking value")
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        return ["YAML root must be a mapping"], {}
    sources = data.get("sources") or []
    if not isinstance(sources, list):
        return ["sources must be a list"], {}
    seen: set[str] = set()
    for row in sources:
        if not isinstance(row, dict):
            errors.append("every source must be a mapping")
            continue
        _check_source(row, errors, path.parents[2])
        if row.get("id") in seen:
            errors.append(f"{row.get('id')}: duplicate id")
        seen.add(str(row.get("id")))

    batches = data.get("batches") or {}
    current = data.get("current_batch")
    for number, meta in batches.items():
        rows = [r for r in sources if isinstance(r, dict) and r.get("batch") == number]
        if len(rows) != BATCH_SIZE:
            errors.append(f"batch {number}: {len(rows)} rows, expected {BATCH_SIZE}")
        ranks = sorted(int(r.get("popularity_rank") or 0) for r in rows)
        if ranks != list(range(1, len(rows) + 1)):
            errors.append(f"batch {number}: popularity_rank must run 1..{len(rows)}")
        report = (meta or {}).get("report")
        if not report or not (path.parents[2] / report).is_file():
            errors.append(f"batch {number}: report file {report!r} not found")
    for row in sources:
        if isinstance(row, dict) and row.get("batch") not in batches:
            errors.append(f"{row.get('id')}: batch {row.get('batch')!r} is not declared in batches")
    if current not in batches:
        errors.append(f"current_batch {current!r} has no batches entry")
    nxt = data.get("next_batch") or {}
    next_sources = nxt.get("sources") or []
    if nxt.get("number") != (current or 0) + 1 or not next_sources:
        errors.append("next_batch must list the sources of batch current_batch + 1")
    elif set(next_sources) & seen:
        errors.append(f"next_batch repeats sources already registered: {sorted(set(next_sources) & seen)}")

    current_rows = [r for r in sources if isinstance(r, dict) and r.get("batch") == current]
    summary = {
        "batch": current,
        "sources": len(current_rows),
        "verdict": dict(Counter(r.get("verdict") for r in current_rows)),
        "current_status": dict(Counter(r.get("current_status") for r in current_rows)),
        "method": dict(Counter(r.get("method") for r in current_rows)),
    }
    return errors, summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("path", nargs="?", type=Path, default=DEFAULT_PATH)
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args(argv)
    errors, summary = check(args.path)
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if args.summary or not errors:
        print(json.dumps(summary, indent=2, sort_keys=True) if args.summary else f"source coverage OK: {summary.get('sources')} sources in batch {summary.get('batch')}")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
