#!/usr/bin/env python3
"""Capture the owner's public GitHub activity into Bronze (spec: docs/specs/github-activity.md).

Two datasets under ``domain=engineering``, ``source=github``:

* ``github_repos``  — public repositories the owner owns (daily snapshot);
* ``github_events`` — the owner's public events (GitHub keeps 90 days, at most 300).

Records are trimmed before landing: commit messages, author names and e-mails and
issue/comment bodies never leave this process. Batches are content-addressed, so a
re-run on unchanged data writes nothing. Only counts and object keys are printed,
because the workflow log is public.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from data_lake.ingest import ingest_records  # noqa: E402

SOURCE = "github"
DOMAIN = "engineering"
API = "https://api.github.com"
DEFAULT_OWNER = "bookchaowalit"
DEFAULT_LAKE_URI = str(ROOT / "data" / "lake")
MAX_REPO_PAGES = 10
MAX_EVENT_PAGES = 3
PER_PAGE = 100
TIMEOUT_SECONDS = 30
OWNER_RE = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9-]{0,38})$")

# fetch(url) -> (decoded JSON body, value of the Link header or "")
Fetch = Callable[[str], tuple[Any, str]]


class GitHubError(RuntimeError):
    """The GitHub API answered with an error or an unexpected shape."""


def http_fetch(token: str | None) -> Fetch:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "data-lake-pipelines-github-activity",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    def fetch(url: str) -> tuple[Any, str]:
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:  # noqa: S310 (fixed https host)
                body = json.loads(response.read().decode("utf-8"))
                return body, response.headers.get("Link", "") or ""
        except urllib.error.HTTPError as exc:
            raise GitHubError(f"HTTP {exc.code}") from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise GitHubError(type(exc).__name__) from None

    return fetch


def _next_link(link_header: str) -> str | None:
    for part in link_header.split(","):
        url, _, rel = part.partition(";")
        if 'rel="next"' in rel:
            url = url.strip().strip("<>")
            # Never follow a link off the API host (defence against a tampered header).
            return url if url.startswith(API + "/") else None
    return None


def _paged(fetch: Fetch, url: str, max_pages: int) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    next_url: str | None = url
    pages = 0
    while next_url and pages < max_pages:
        body, link = fetch(next_url)
        if not isinstance(body, list):
            raise GitHubError("expected a JSON list")
        items.extend(item for item in body if isinstance(item, dict))
        pages += 1
        next_url = _next_link(link)
    return items


def trim_repo(repo: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": repo.get("id"),
        "full_name": repo.get("full_name"),
        "name": repo.get("name"),
        "fork": bool(repo.get("fork")),
        "archived": bool(repo.get("archived")),
        "language": repo.get("language"),
        "topics": sorted(str(t) for t in (repo.get("topics") or [])),
        "stargazers_count": repo.get("stargazers_count"),
        "forks_count": repo.get("forks_count"),
        "open_issues_count": repo.get("open_issues_count"),
        "size_kb": repo.get("size"),
        "default_branch": repo.get("default_branch"),
        "created_at": repo.get("created_at"),
        "updated_at": repo.get("updated_at"),
        "pushed_at": repo.get("pushed_at"),
        "html_url": repo.get("html_url"),
    }


def trim_event(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload") or {}
    number = None
    for key in ("pull_request", "issue"):
        obj = payload.get(key)
        if isinstance(obj, dict) and obj.get("number") is not None:
            number = obj.get("number")
            break
    if number is None:
        number = payload.get("number")
    push_size = payload.get("size")
    if push_size is None and isinstance(payload.get("commits"), list):
        push_size = len(payload["commits"])
    return {
        "id": str(event.get("id")),
        "type": event.get("type"),
        "created_at": event.get("created_at"),
        "repo": (event.get("repo") or {}).get("name"),
        "actor": (event.get("actor") or {}).get("login"),
        "action": payload.get("action"),
        "ref": payload.get("ref"),
        "ref_type": payload.get("ref_type"),
        "push_size": push_size,
        "number": number,
    }


def collect(fetch: Fetch, owner: str) -> dict[str, list[dict[str, Any]]]:
    if not OWNER_RE.match(owner):
        raise ValueError("owner must be a GitHub login")
    repos = _paged(fetch, f"{API}/users/{owner}/repos?type=owner&sort=full_name&per_page={PER_PAGE}", MAX_REPO_PAGES)
    events = _paged(fetch, f"{API}/users/{owner}/events/public?per_page={PER_PAGE}", MAX_EVENT_PAGES)
    repo_rows = sorted((trim_repo(r) for r in repos if not r.get("private")), key=lambda r: r["id"] or 0)
    event_rows = sorted((trim_event(e) for e in events if e.get("public", True)), key=lambda e: e["id"])
    if any(row["id"] is None for row in repo_rows) or any(row["id"] in (None, "None") for row in event_rows):
        raise GitHubError("record without id")
    return {"github_repos": repo_rows, "github_events": event_rows}


EVENT_TIME = {"github_repos": "pushed_at", "github_events": "created_at"}


def capture(fetch: Fetch, owner: str, data_lake_uri: str, *, dry_run: bool = False) -> dict[str, Any]:
    records = collect(fetch, owner)
    if dry_run:
        return {"status": "dry-run", "datasets": {k: {"records": len(v)} for k, v in records.items()}}
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
            event_time_field=EVENT_TIME[dataset],
            # Constant metadata keeps the batch id a pure content hash (re-runs write nothing).
            metadata={"provider": SOURCE, "owner": owner},
            privacy_class="public",
            retention_class="operational",
        )
        results[dataset] = {
            "records": len(rows),
            "new": not result["manifest_existed"],
            "manifest_key": result["manifest_key"],
        }
    return {"status": "captured", "datasets": results}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--owner", default=DEFAULT_OWNER)
    parser.add_argument("--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_LAKE_URI))
    parser.add_argument("--require-cloud", action="store_true",
                        help="fail unless the lake URI is s3:// (scheduled runs must never fall back to a local path)")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None, fetch: Fetch | None = None) -> int:
    args = build_parser().parse_args(argv)
    fetch = fetch or http_fetch(os.environ.get("GITHUB_TOKEN"))
    uri = (args.data_lake_uri or "").strip()
    if not uri or (args.require_cloud and not uri.startswith("s3://")):
        # An empty or unset secret must fail the run, not write into the runner's checkout.
        print(json.dumps({"status": "error", "error": "LakeUriNotConfigured"}))
        return 2
    args.data_lake_uri = uri
    try:
        report = capture(fetch, args.owner, args.data_lake_uri, dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001 — public log: the exception type only
        print(json.dumps({"status": "error", "error": type(exc).__name__}))
        return 2
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
