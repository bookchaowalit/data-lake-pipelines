#!/usr/bin/env python3
"""Daily snapshot of Hugging Face trending models, datasets and spaces into Bronze.

Spec: docs/specs/hf-trending.md. Public Hub API, no token (robots.txt allows all; the
anonymous limit is 500 requests per 5 minutes, a run makes 3). One Bronze dataset per
kind under ``domain=market-intelligence``, ``source=huggingface``. Free-text fields
(descriptions, card data) are dropped. A record id is ``<repo id>@<snapshot date>`` so
every day is its own snapshot, and a same-day re-run with unchanged numbers writes nothing.
Only counts and object keys are printed, because the workflow log is public.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.ingest import ingest_records  # noqa: E402

SOURCE = "huggingface"
DOMAIN = "market-intelligence"
API = "https://huggingface.co/api"
KINDS = ("models", "datasets", "spaces")
LIMIT = 100
MAX_TAGS = 20
TIMEOUT_SECONDS = 30
DEFAULT_LAKE_URI = str(ROOT / "data" / "lake")

Fetch = Callable[[str], Any]


class HubError(RuntimeError):
    """The Hub API answered with an error or an unexpected shape."""


def http_fetch() -> Fetch:
    headers = {
        "Accept": "application/json",
        "User-Agent": "data-lake-pipelines-hf-trending",
    }

    def fetch(url: str) -> Any:
        # Only the fixed https API host: no file:// or other scheme can reach urlopen.
        if not url.startswith(API + "/"):
            raise HubError("URL outside the Hugging Face API host")
        request = urllib.request.Request(
            url, headers=headers
        )  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise HubError(f"HTTP {exc.code}") from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise HubError(type(exc).__name__) from None

    return fetch


def _row(
    kind: str, item: dict[str, Any], rank: int, snapshot_date: str
) -> dict[str, Any]:
    repo_id = str(item.get("id") or item.get("modelId") or "")
    if not repo_id:
        raise HubError("item without id")
    tags = [str(t) for t in (item.get("tags") or [])][:MAX_TAGS]
    return {
        "id": f"{kind.rstrip('s')}:{repo_id}@{snapshot_date}",
        "repo_id": repo_id,
        "kind": kind.rstrip("s"),
        "author": item.get("author") or repo_id.split("/")[0],
        "rank": rank,
        "snapshot_date": snapshot_date,
        "trending_score": item.get("trendingScore"),
        "likes": item.get("likes"),
        "downloads": item.get("downloads"),
        "pipeline_tag": item.get("pipeline_tag"),
        "library_name": item.get("library_name"),
        "sdk": item.get("sdk"),
        "gated": item.get("gated")
        if isinstance(item.get("gated"), (bool, str))
        else None,
        "tags": tags,
        "created_at": item.get("createdAt"),
        "last_modified": item.get("lastModified"),
    }


def collect(fetch: Fetch, snapshot_date: str) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for kind in KINDS:
        body = fetch(f"{API}/{kind}?sort=trendingScore&limit={LIMIT}")
        if not isinstance(body, list):
            raise HubError("expected a JSON list")
        items = [i for i in body if isinstance(i, dict) and not i.get("private")]
        if not items:
            # The top-100 list is never empty; an empty answer is an outage, not a quiet day.
            raise HubError(f"empty {kind} list")
        out[f"hf_trending_{kind}"] = [
            _row(kind, item, rank, snapshot_date)
            for rank, item in enumerate(items, start=1)
        ]
    return out


def capture(
    fetch: Fetch,
    data_lake_uri: str,
    *,
    snapshot_date: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    day = snapshot_date or datetime.now(timezone.utc).date().isoformat()
    records = collect(fetch, day)
    if dry_run:
        return {
            "status": "dry-run",
            "snapshot_date": day,
            "datasets": {k: {"records": len(v)} for k, v in records.items()},
        }
    results: dict[str, Any] = {}
    for dataset, rows in records.items():
        if not rows:
            results[dataset] = {"records": 0, "new": False, "manifest_key": None}
            continue
        result = ingest_records(
            rows,
            source=SOURCE,
            domain=DOMAIN,
            dataset=dataset,
            data_lake_uri=data_lake_uri,
            source_record_id_field="id",
            event_time_field="snapshot_date",
            metadata={"provider": SOURCE},
            privacy_class="public",
            retention_class="operational",
        )
        results[dataset] = {
            "records": len(rows),
            "new": not result["manifest_existed"],
            "manifest_key": result["manifest_key"],
        }
    return {"status": "captured", "snapshot_date": day, "datasets": results}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument(
        "--data-lake-uri",
        default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_LAKE_URI),
    )
    parser.add_argument(
        "--require-cloud",
        action="store_true",
        help="fail unless the lake URI is s3:// (scheduled runs must never fall back to a local path)",
    )
    parser.add_argument("--snapshot-date", help="YYYY-MM-DD; defaults to today (UTC)")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None, fetch: Fetch | None = None) -> int:
    args = build_parser().parse_args(argv)
    uri = (args.data_lake_uri or "").strip()
    if not uri or (args.require_cloud and not uri.startswith("s3://")):
        print(json.dumps({"status": "error", "error": "LakeUriNotConfigured"}))
        return 2
    try:
        if args.snapshot_date:
            datetime.strptime(args.snapshot_date, "%Y-%m-%d")
        report = capture(
            fetch or http_fetch(),
            uri,
            snapshot_date=args.snapshot_date,
            dry_run=args.dry_run,
        )
    except Exception as exc:  # noqa: BLE001 — public log: the exception type only
        print(json.dumps({"status": "error", "error": type(exc).__name__}))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
