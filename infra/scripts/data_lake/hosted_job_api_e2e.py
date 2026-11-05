#!/usr/bin/env python3
"""Verify a hosted job.v1 API serves records from its configured lake path.

This is a read-only check. It never uploads to R2, never falls back to local
Bronze, and never prints the bearer token or object-store credentials.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any


def _get_json(url: str, *, token: str, timeout: float) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _assert_envelope(body: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(body, dict):
        raise RuntimeError(f"{label}: response is not an object")
    if body.get("schema_version") != "job.v1":
        raise RuntimeError(f"{label}: schema_version is not job.v1")
    if body.get("source") != "book-job-data":
        raise RuntimeError(f"{label}: source is not book-job-data")
    if not isinstance(body.get("items"), list):
        raise RuntimeError(f"{label}: items is not a list")
    return body


HOSTED_STORAGE = {
    "silver_s3_parquet": "lake_first_job_silver_s3_duckdb",
    "bronze_s3_parquet": "lake_first_job_bronze_s3_duckdb",
}


def verify(base_url: str, *, token: str, timeout: float) -> dict[str, Any]:
    base = base_url.rstrip("/")
    metadata = _assert_envelope(
        _get_json(f"{base}/v1/metadata", token=token, timeout=timeout),
        label="metadata",
    )
    records = _assert_envelope(
        _get_json(
            f"{base}/v1/records?dataset=job_postings&limit=500",
            token=token,
            timeout=timeout,
        ),
        label="records",
    )
    metadata_item = metadata["items"][0] if metadata["items"] else {}
    if records.get("data_status") not in {"ok", "stale"}:
        raise RuntimeError(f"records: unexpected data_status={records.get('data_status')}")
    if not records["items"]:
        raise RuntimeError("records: hosted API returned no job_postings rows")
    # job_postings is served from the newest jobs Silver part, or Bronze when
    # Silver is unavailable (career-job-market-data silver-job-postings-read spec).
    expected = HOSTED_STORAGE.get(metadata_item.get("records_source_kind"))
    if expected is None:
        raise RuntimeError(
            "metadata: expected records_source_kind in "
            f"{sorted(HOSTED_STORAGE)}; got {metadata_item.get('records_source_kind')!r}"
        )
    if metadata_item.get("storage_model") != expected:
        raise RuntimeError(
            f"metadata: expected storage_model={expected}; "
            f"got {metadata_item.get('storage_model')!r}"
        )
    return {
        "status": "verified",
        "base_url": base,
        "data_status": records.get("data_status"),
        "storage_model": metadata_item.get("storage_model"),
        "records_source_kind": metadata_item.get("records_source_kind"),
        "record_count": len(records["items"]),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default=os.environ.get("BOOK_JOB_DATA_URL", ""),
        help="Hosted API URL; defaults to BOOK_JOB_DATA_URL",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=float(os.environ.get("BOOK_JOB_DATA_TIMEOUT_SECONDS", "30")),
    )
    args = parser.parse_args()
    if not args.base_url:
        print("ERROR: set --base-url or BOOK_JOB_DATA_URL", file=sys.stderr)
        return 2
    try:
        result = verify(
            args.base_url,
            token=os.environ.get("BOOK_JOB_DATA_API_TOKEN", "").strip(),
            timeout=args.timeout_seconds,
        )
    except (OSError, ValueError, RuntimeError, urllib.error.HTTPError) as exc:
        print(f"ERROR: hosted job API verification failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
