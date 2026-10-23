#!/usr/bin/env python3
"""Register and query one existing Bronze part through an Iceberg REST catalog.

This is an operator-gated lab for the hosted R2 Data Catalog. It never scrapes
or creates a new raw batch: it reads an existing manifest, registers the
referenced Bronze Parquet file, retries the registration, and queries the
catalog-owned table with DuckDB. The write gate must be enabled only for this
command; the vault keeps it disabled by default.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.cloud_preflight import validate_config  # noqa: E402
from data_lake.iceberg import register_bronze_file  # noqa: E402
from data_lake.query import query_iceberg  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402


DEFAULT_R2_URI = "s3://solo-empire-portfolio/portfolio-demo/free-cloud-e2e-001"
DEFAULT_MANIFEST_KEY = (
    "control/manifests/source=free-cloud-e2e/"
    "batch_id=d213d48980f148154933e64b.json"
)
REQUIRED_RUNTIME_KEYS = (
    "DATA_LAKE_S3_ENDPOINT",
    "AWS_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "ICEBERG_CATALOG_URI",
    "ICEBERG_WAREHOUSE_URI",
    "ICEBERG_CATALOG_TOKEN",
)


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def validate_catalog_runtime(data_lake_uri: str) -> dict[str, Any]:
    """Validate catalog settings without contacting R2."""
    parsed = urlparse(data_lake_uri)
    if parsed.scheme.lower() != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError("catalog E2E requires a bounded s3:// bucket/prefix URI")

    missing = [name for name in REQUIRED_RUNTIME_KEYS if not os.environ.get(name)]
    if missing:
        raise ValueError(f"missing catalog runtime settings: {', '.join(missing)}")

    report = validate_config(
        data_lake_uri=data_lake_uri,
        catalog_uri=os.environ["ICEBERG_CATALOG_URI"],
        require_cloud_write=True,
    )
    if report["status"] != "ok":
        errors = "; ".join(str(item) for item in report["errors"])
        raise ValueError(f"catalog preflight failed: {errors}")
    return report


def run_catalog_e2e(
    *, data_lake_uri: str = DEFAULT_R2_URI, manifest_key: str = DEFAULT_MANIFEST_KEY
) -> dict[str, Any]:
    """Register one manifest Bronze file and verify query/idempotency."""
    config = validate_catalog_runtime(data_lake_uri)
    # PyArrow's S3 reader needs the endpoint override; keep this process-local
    # and never persist it in the repository or secret store.
    os.environ.setdefault("PYARROW_S3_ENDPOINT_OVERRIDE", os.environ["DATA_LAKE_S3_ENDPOINT"])
    os.environ.setdefault("AWS_ENDPOINT_URL_S3", os.environ["DATA_LAKE_S3_ENDPOINT"])

    store = ObjectStore(data_lake_uri)
    manifest = json.loads(store.get_bytes(manifest_key).decode("utf-8"))
    bronze_key = str(manifest.get("bronze", {}).get("key", ""))
    _assert(bronze_key.startswith("bronze/"), "manifest must reference a Bronze object")
    _assert(not bronze_key.endswith(".csv"), "catalog E2E must use Bronze Parquet")
    _assert(manifest.get("record_count", 0) > 0, "manifest must contain records")
    _assert(bool(store.get_bytes(bronze_key)), "Bronze object is empty or unavailable")

    registration = dict(
        data_lake_uri=data_lake_uri,
        bronze_key=bronze_key,
        domain=str(manifest["domain"]),
        dataset=str(manifest["dataset"]),
        catalog_uri=os.environ["ICEBERG_CATALOG_URI"],
        warehouse_uri=os.environ["ICEBERG_WAREHOUSE_URI"],
        run_id=str(manifest["run_id"]),
    )
    first = register_bronze_file(**registration)
    retry = register_bronze_file(**registration)
    _assert(first["status"] in {"registered", "already_registered"}, "unexpected registration status")
    _assert(retry["status"] == "already_registered", "retry created a duplicate registration")
    _assert(first["snapshot_id"] == retry["snapshot_id"], "retry changed the Iceberg snapshot")

    query_rows = query_iceberg(
        str(first["table_location"]),
        "SELECT count(*) AS record_count, count(raw_object_key) AS lineage_count "
        "FROM lake_table",
        metadata_location=str(first["metadata_location"]),
    )
    _assert(len(query_rows) == 1, "Iceberg query returned no aggregate row")
    queried_count = int(query_rows[0]["record_count"])
    lineage_count = int(query_rows[0]["lineage_count"])
    _assert(queried_count == int(manifest["record_count"]), "Iceberg row count differs from manifest")
    _assert(lineage_count == queried_count, "Iceberg lineage column is incomplete")

    return {
        "status": "verified",
        "mode": "hosted-iceberg-catalog",
        "data_lake": store.describe(),
        "catalog_uri": config["catalog_uri"],
        "warehouse": os.environ["ICEBERG_WAREHOUSE_URI"],
        "manifest_key": manifest_key,
        "bronze_key": bronze_key,
        "table_identifier": first["table_identifier"],
        "table_location": first["table_location"],
        "registration_status": first["status"],
        "retry_status": retry["status"],
        "snapshot_id": first["snapshot_id"],
        "idempotent_snapshot": first["snapshot_id"] == retry["snapshot_id"],
        "manifest_record_count": int(manifest["record_count"]),
        "queried_record_count": queried_count,
        "lineage_count": lineage_count,
        "query_engine": "duckdb",
        "query_path": "iceberg_rest_catalog",
        "raw_source": "existing_manifest_only",
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-lake-uri",
        default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", DEFAULT_R2_URI),
    )
    parser.add_argument(
        "--manifest-key",
        default=DEFAULT_MANIFEST_KEY,
        help="existing manifest key to register; no new landing batch is created",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        report = run_catalog_e2e(
            data_lake_uri=args.data_lake_uri,
            manifest_key=args.manifest_key,
        )
    except Exception as exc:  # noqa: BLE001 - CLI returns concise contract failures
        error = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        if args.as_json:
            print(json.dumps(error, ensure_ascii=False, sort_keys=True))
        else:
            print(f"ICEBERG-CATALOG-E2E: FAILED — {error['error']}", file=sys.stderr)
        return 2

    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print("ICEBERG-CATALOG-E2E: VERIFIED")
        print(f"  table: {report['table_identifier']}")
        print(f"  rows: {report['queried_record_count']}")
        print(f"  snapshot: {report['snapshot_id']}")
        print(f"  idempotent retry: {report['idempotent_snapshot']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
