#!/usr/bin/env python3
"""Daily top Wikipedia articles (Thai and English) into Bronze.

Spec: docs/specs/wiki-pageviews.md. Wikimedia Analytics API (pageviews/top), data CC0 1.0,
no credential, one request at a time with a descriptive User-Agent as the access policy asks.
One Bronze dataset ``wiki_top_articles`` under ``domain=market-intelligence``,
``source=wikimedia``. A day's ranking is published the day after, so the default date is
two days back (UTC). Only counts and object keys are printed (public log).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, TypeGuard

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.ingest import canonical_json, ingest_records  # noqa: E402
from data_lake.storage import ObjectNotFound, ObjectStore  # noqa: E402

SOURCE = "wikimedia"
DOMAIN = "market-intelligence"
DATASET = "wiki_top_articles"
API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top"
PROJECTS = ("th.wikipedia", "en.wikipedia")
TOP_N = 200
LAG_DAYS = 2
PAUSE_SECONDS = 1.0
TIMEOUT_SECONDS = 30
USER_AGENT = "data-lake-pipelines-wiki-pageviews (https://github.com/bookchaowalit/data-lake-pipelines)"
DEFAULT_LAKE_URI = str(ROOT / "data" / "lake")

Fetch = Callable[[str], Any]


class WikimediaError(RuntimeError):
    """The Analytics API answered with an error or an unexpected shape."""


def http_fetch() -> Fetch:
    headers = {"Accept": "application/json", "User-Agent": USER_AGENT}

    def fetch(url: str) -> Any:
        # Only the fixed https API host: no file:// or other scheme can reach urlopen.
        if not url.startswith(API + "/"):
            raise WikimediaError("URL outside the Wikimedia API host")
        request = urllib.request.Request(
            url, headers=headers
        )  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raise WikimediaError(f"HTTP {exc.code}") from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise WikimediaError(type(exc).__name__) from None

    return fetch


def _url(project: str, day: date) -> str:
    return f"{API}/{urllib.parse.quote(project)}/all-access/{day:%Y/%m/%d}"


def collect(
    fetch: Fetch, day: date, *, pause: float = PAUSE_SECONDS
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, project in enumerate(PROJECTS):
        if index:
            time.sleep(pause)  # one request at a time, as the access policy asks
        body = fetch(_url(project, day))
        try:
            articles = body["items"][0]["articles"]
        except (KeyError, IndexError, TypeError):
            raise WikimediaError("unexpected response shape") from None
        if not isinstance(articles, list) or not articles:
            raise WikimediaError(f"empty ranking for {project}")
        before = len(rows)
        for item in articles[:TOP_N]:
            if not isinstance(item, dict) or not item.get("article"):
                continue
            title = str(item["article"])
            rank, views = item.get("rank"), item.get("views")
            # A malformed ranking must fail before Bronze or the marker close the day for good.
            if not _count(rank) or rank < 1 or not _count(views):
                raise WikimediaError(f"bad rank or views for {project}")
            rows.append(
                {
                    "id": f"{project}:{title}@{day.isoformat()}",
                    "project": project,
                    "article": title,
                    "namespace_page": ":"
                    in title,  # Special:, Wikipedia: and similar, kept but flagged
                    "rank": rank,
                    "views": views,
                    "view_date": day.isoformat(),
                }
            )
        if len(rows) == before:
            raise WikimediaError(f"no usable rows for {project}")
    return rows


def _count(value: Any) -> TypeGuard[int]:
    """A non-negative int (bool is an int subclass, so it is refused explicitly)."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _marker_key(day: date) -> str:
    return f"control/state/source={SOURCE}/dataset={DATASET}/view_date={day.isoformat()}.json"


def _snapshot_key(day: date) -> str:
    return f"control/state/source={SOURCE}/dataset={DATASET}/view_date={day.isoformat()}.snapshot.json"


def capture(
    fetch: Fetch,
    data_lake_uri: str,
    *,
    day: date,
    dry_run: bool = False,
    pause: float = PAUSE_SECONDS,
) -> dict[str, Any]:
    store = ObjectStore(data_lake_uri)
    marker = _marker_key(day)
    if not dry_run:
        try:
            done = json.loads(store.get_bytes(marker))
        except ObjectNotFound:
            done = None
        if done is not None:
            # One ranking per view_date: a later fetch with revised numbers must not add a second one.
            return {
                "status": "exists",
                "view_date": day.isoformat(),
                "records": done.get("records"),
                "new": False,
                "manifest_key": done.get("manifest_key"),
            }
    if dry_run:
        rows = collect(fetch, day, pause=pause)
        return {"status": "dry-run", "view_date": day.isoformat(), "records": len(rows)}
    # The fetched ranking is checkpointed before Bronze is written. If the marker write below fails,
    # a retry ingests this same snapshot (same content, same batch) instead of a re-fetch whose
    # revised numbers would add a second ranking for the day.
    snapshot = _snapshot_key(day)
    try:
        rows = json.loads(store.get_bytes(snapshot))
    except ObjectNotFound:
        rows = collect(fetch, day, pause=pause)
        store.put_bytes(
            snapshot,
            canonical_json(rows).encode("utf-8"),
            content_type="application/json",
        )
    result = ingest_records(
        rows,
        source=SOURCE,
        domain=DOMAIN,
        dataset=DATASET,
        data_lake_uri=data_lake_uri,
        source_record_id_field="id",
        event_time_field="view_date",
        metadata={"provider": SOURCE, "licence": "CC0-1.0"},
        privacy_class="public",
        retention_class="operational",
    )
    store.put_bytes(
        marker,
        canonical_json(
            {"manifest_key": result["manifest_key"], "records": len(rows)}
        ).encode("utf-8"),
        content_type="application/json",
    )
    return {
        "status": "captured",
        "view_date": day.isoformat(),
        "records": len(rows),
        "new": not result["manifest_existed"],
        "manifest_key": result["manifest_key"],
    }


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
    parser.add_argument(
        "--date", help=f"YYYY-MM-DD; defaults to {LAG_DAYS} days ago (UTC)"
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(
    argv: list[str] | None = None,
    fetch: Fetch | None = None,
    pause: float = PAUSE_SECONDS,
) -> int:
    args = build_parser().parse_args(argv)
    uri = (args.data_lake_uri or "").strip()
    if not uri or (args.require_cloud and not uri.startswith("s3://")):
        print(json.dumps({"status": "error", "error": "LakeUriNotConfigured"}))
        return 2
    try:
        day = (
            datetime.strptime(args.date, "%Y-%m-%d").date()
            if args.date
            else datetime.now(timezone.utc).date() - timedelta(days=LAG_DAYS)
        )
        report = capture(
            fetch or http_fetch(), uri, day=day, dry_run=args.dry_run, pause=pause
        )
    except Exception as exc:  # noqa: BLE001 — public log: the exception type only
        print(json.dumps({"status": "error", "error": type(exc).__name__}))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
