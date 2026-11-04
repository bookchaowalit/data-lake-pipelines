#!/usr/bin/env python3
"""Export X feed pulls from the intake cell into the data lake (Bronze ``social/x_feed``).

The Grok Bot collector pushes pulls to intake-api (``POST /v1/x-feed/pulls``); each pull is one
``intakes`` row with dataset ``x_feed``. This adapter lists the pulls still ``landed``
(``GET /v1/x-feed/pulls?status=landed``), reads each as
flat x_feed rows (``GET /v1/x-feed/pulls/{pull_id}/rows``), ingests them with :func:`ingest_records`
(landing JSONL + Bronze Parquet + manifest, one batch per pull content), then tells intake the lake has
the pull (``POST /v1/x-feed/pulls/{pull_id}/exported``), which marks the intake ``verified`` and lets the
intake cell's 180-day text retention apply. The direction is one-way: intake -> lake.

A pull that gained rows between the read and the mark answers 409 ``pull_changed`` and is left
``landed`` for the next run; the lake keeps both batches and ``stg_x_feed`` deduplicates by
``sighting_key``.

Configuration::

    EMPIRE_INTAKE_API          intake-api base URL (https; http only on loopback)
    EMPIRE_TOKEN | EMPIRE_TOKEN_FILE   bearer of the x-feed-lake-export automation
                               (intake.x-post:read, intake.x-sighting:read|write; nothing on other sources)
    SOLO_EMPIRE_DATA_LAKE_URI  file:// or s3:// lake (R2 through the s3 endpoint settings of storage.py)

Examples:
    python3 x_feed_lake_export.py --dry-run --json
    python3 x_feed_lake_export.py --json
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import os
import re
import sys
import urllib.parse
from pathlib import Path
from typing import Any

import httpx

SCRIPTS_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS_ROOT / "utils"))
sys.path.insert(0, str(SCRIPTS_ROOT))

from _env import PROJECT_ROOT  # noqa: E402
from data_lake.ingest import ingest_records  # noqa: E402

DEFAULT_DATA_LAKE_URI = os.environ.get(
    "SOLO_EMPIRE_DATA_LAKE_URI",
    os.environ.get("DATA_LAKE_URI", str(PROJECT_ROOT / "data" / "lake")),
)
LAKE_SOURCE = "x-feed"
LAKE_DOMAIN = "social"
LAKE_DATASET = "x_feed"
LAKE_TABLE = "bronze/domain=social/dataset=x_feed/schema_version=1"
CONTRACT = "x_feed.v1"
PAGE = 100
# intake-api's own pull_id rule; anything else is refused before it reaches a URL path.
PULL_ID = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9._:-]{0,79}$")


class ExportError(RuntimeError):
    pass


def checked_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    host = parts.hostname or ""
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = host == "localhost"
    if parts.scheme != "https" and not (parts.scheme == "http" and loopback):
        raise ExportError("EMPIRE_INTAKE_API must be https (http only on loopback)")
    return url.rstrip("/")


def bearer(environ: Any = None) -> str:
    """The automation token from EMPIRE_TOKEN or EMPIRE_TOKEN_FILE; never printed."""
    environ = os.environ if environ is None else environ
    token = environ.get("EMPIRE_TOKEN", "")
    if not token and environ.get("EMPIRE_TOKEN_FILE"):
        token = Path(environ["EMPIRE_TOKEN_FILE"]).expanduser().read_text(encoding="utf-8")
    token = token.strip()
    if not token or any(c.isspace() for c in token) or not token.isascii():
        raise ExportError("set EMPIRE_TOKEN or EMPIRE_TOKEN_FILE to the x-feed-lake-export token")
    return token


def _json(response: httpx.Response, what: str) -> Any:
    if response.status_code >= 400:
        # Only the status and the API's error word: never echo row content.
        try:
            word = response.json().get("detail", {}).get("error", "")
        except (ValueError, AttributeError):
            word = ""
        raise ExportError(f"{what}: HTTP {response.status_code} {word}".strip())
    return response.json()


def pending_pulls(client: httpx.Client) -> list[dict[str, Any]]:
    """The landed x-feed pulls, oldest first (up to PAGE per run; the rest wait for the next run)."""
    params = {"status": "landed", "limit": PAGE}
    items: list[dict[str, Any]] = _json(client.get("/v1/x-feed/pulls", params=params), "list pulls")["items"]
    return items


def export_pull(client: httpx.Client, intake: dict[str, Any], data_lake_uri: str) -> dict[str, Any]:
    pull_id = str(intake["pull_id"])
    if not PULL_ID.match(pull_id):
        raise ExportError("refused a pull_id outside intake-api's pattern")
    path = f"/v1/x-feed/pulls/{urllib.parse.quote(pull_id, safe='')}"
    export = _json(client.get(f"{path}/rows"), f"read pull {pull_id}")
    if export.get("pull_id") != pull_id:
        raise ExportError(f"read pull {pull_id}: the answer is for another pull")
    rows = export["items"]
    lake = ingest_records(
        rows,
        source=LAKE_SOURCE,
        domain=LAKE_DOMAIN,
        dataset=LAKE_DATASET,
        data_lake_uri=data_lake_uri,
        schema_version="1",
        source_record_id_field="sighting_key",
        event_time_field="collected_at",
        metadata={"adapter": "x_feed_lake_export", "contract": CONTRACT, "pull_id": pull_id},
        privacy_class="private",
        retention_class="x-feed-history",
    )
    marked = client.post(
        f"{path}/exported",
        json={
            "record_count": export["record_count"],
            "lake_run_id": lake["run_id"],
            "lake_bronze_object_key": lake["bronze_key"],
            "lake_manifest_key": lake["manifest_key"],
            "lake_raw_object_key": lake["raw_key"],
        },
    )
    status = "verified"
    if marked.status_code == 409:
        status = "changed_retry_next_run"
    else:
        _json(marked, f"mark pull {pull_id}")
    return {
        "pull_id": pull_id,
        "intake_id": intake["intake_id"],
        "records": len(rows),
        "status": status,
        "run_id": lake["run_id"],
        "bronze_key": lake["bronze_key"],
        "bronze_existed": lake["bronze_existed"],
    }


def export_pending(
    client: httpx.Client, *, data_lake_uri: str = DEFAULT_DATA_LAKE_URI, dry_run: bool = False
) -> dict[str, Any]:
    pulls = pending_pulls(client)
    result: dict[str, Any] = {
        "pipeline": "x_feed_lake_export",
        "status": "dry_run" if dry_run else "success",
        "pending": len(pulls),
        "lake_table": LAKE_TABLE,
    }
    if dry_run:
        result["pull_ids"] = [p["pull_id"] for p in pulls]
        return result
    runs = []
    failed = 0
    for intake in pulls:
        try:
            runs.append(export_pull(client, intake, data_lake_uri))
        except ExportError as exc:  # one bad pull must not stop the others
            failed += 1
            runs.append({"pull_id": intake.get("pull_id"), "status": "failed", "error": str(exc)})
    result["runs"] = runs
    result["exported"] = sum(1 for r in runs if r["status"] == "verified")
    if failed:
        result["status"] = "partial" if failed < len(pulls) else "failed"
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--intake-api", default=os.environ.get("EMPIRE_INTAKE_API", ""))
    parser.add_argument("--data-lake-uri", default=DEFAULT_DATA_LAKE_URI)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        base = checked_url(args.intake_api)
        token = bearer()
        with httpx.Client(
            base_url=base,
            headers={"Authorization": f"Bearer {token}", "X-Empire-Channel": "workflow"},
            timeout=60,
            follow_redirects=False,
        ) as client:
            result = export_pending(client, data_lake_uri=args.data_lake_uri, dry_run=args.dry_run)
    except (ExportError, httpx.HTTPError) as exc:
        result = {"pipeline": "x_feed_lake_export", "status": "failed", "error": str(exc)}
    if args.as_json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(f"x_feed_lake_export: {result['status']} pending={result.get('pending', '?')}"
              f" exported={result.get('exported', 0)}")
    return 0 if result["status"] in ("success", "dry_run") else 1


if __name__ == "__main__":
    raise SystemExit(main())
