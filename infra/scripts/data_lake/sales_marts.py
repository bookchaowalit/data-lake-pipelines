#!/usr/bin/env python3
"""Sales Bronze → dbt marts → Gold, as one bounded, replayable run.

docs/specs/sales-marts.md items 9–10. The sales models read committed Bronze
commerce events directly (every run rebuilds from full history, so late events
and refunds restate their day). This runner copies that Bronze and the commerce
stream manifests into a scratch lake, builds the sales models there, and
publishes each mart as a write-once Gold part plus a manifest and a publish
pointer. Unchanged inputs write nothing new.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.jobs_marts import _export_mart, _run_dbt, _writer_versions  # noqa: E402
from data_lake.product_adapter import default_data_lake_uri  # noqa: E402
from data_lake.silver import _canonical_json  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

DBT_PROJECT_DIR = ROOT / "infra" / "dbt"
GOLD_SCHEMA_VERSION = "1"
GOLD_SOURCE = "commerce-events"
GOLD_DOMAIN = "sales"
GOLD_MANIFEST_PREFIX = f"control/manifests/gold/source={GOLD_SOURCE}/domain={GOLD_DOMAIN}/"
GOLD_POINTER_PREFIX = f"control/pointers/gold/source={GOLD_SOURCE}/domain={GOLD_DOMAIN}/"
BRONZE_PREFIX = "bronze/domain=commerce/dataset=events/"
STREAM_MANIFEST_PREFIX = "control/manifests/stream-parquet/"
# Mart name → columns that order its rows, so exported bytes are stable.
SALES_MARTS: dict[str, tuple[str, ...]] = {
    "fact_sales": ("intent_id",),
    "dim_customer": ("customer_id", "version"),
    "sales_daily_revenue": ("business_date", "currency"),
    "sales_rejected_events": ("event_key", "reject_reason"),
}
DBT_SELECTOR = "+path:models/marts/sales"


def _model_fingerprint() -> str:
    digest = hashlib.sha256()
    paths = sorted(
        [
            *(DBT_PROJECT_DIR / "models" / "marts" / "sales").glob("*"),
            DBT_PROJECT_DIR / "models" / "staging" / "stg_commerce_events.sql",
            DBT_PROJECT_DIR / "macros" / "unique_combination.sql",
            DBT_PROJECT_DIR / "tests" / "sales_marts_quality.sql",
            # Holds sales_timezone, sales_late_after_hours and the VIP thresholds.
            DBT_PROJECT_DIR / "dbt_project.yml",
        ]
    )
    for path in paths:
        digest.update(path.relative_to(DBT_PROJECT_DIR).as_posix().encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _is_commerce_manifest(body: bytes) -> bool:
    try:
        manifest = json.loads(body)
    except ValueError:
        return False
    return isinstance(manifest, dict) and manifest.get("domain") == "commerce" and manifest.get("dataset") == "events"


def _copy_inputs(store: ObjectStore, scratch: Path) -> str:
    """Copy commerce Bronze + its stream manifests into `scratch`; return their content hash."""
    objects = {
        key: body
        for key, body in store.get_prefix(STREAM_MANIFEST_PREFIX).items()
        if key.endswith(".json") and _is_commerce_manifest(body)
    }
    parts = {key: body for key, body in store.get_prefix(BRONZE_PREFIX).items() if key.endswith(".parquet")}
    objects.update(parts)
    # Batch writers commit through control/manifests/source=<src>/batch_id=*.json, which the
    # staging model accepts too; copy those of every source that has commerce parts.
    sources = {
        segment.split("=", 1)[1]
        for key in parts
        for segment in key.split("/")
        if segment.startswith("source=")
    }
    for source in sorted(sources):
        objects.update(
            (key, body)
            for key, body in store.get_prefix(f"control/manifests/source={source}/").items()
            if "/batch_id=" in key and key.endswith(".json") and _is_commerce_manifest(body)
        )
    digest = hashlib.sha256()
    for key in sorted(objects):
        target = scratch / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(objects[key])
        digest.update(key.encode("utf-8"))
        digest.update(hashlib.sha256(objects[key]).digest())
    return digest.hexdigest()


def run_sales_marts(*, data_lake_uri: str = "", now: datetime | None = None) -> dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    store = ObjectStore(default_data_lake_uri(data_lake_uri=data_lake_uri))

    with tempfile.TemporaryDirectory(prefix="solo-empire-sales-marts-") as temp_dir:
        workdir = Path(temp_dir)
        scratch = workdir / "lake"
        (scratch / "bronze").mkdir(parents=True)
        (scratch / "control" / "manifests").mkdir(parents=True)
        inputs = _copy_inputs(store, scratch)
        gold_run_id = "gold-" + hashlib.sha256(
            _canonical_json(
                {"inputs": inputs, "models": _model_fingerprint(), "writers": _writer_versions()}
            ).encode("utf-8")
        ).hexdigest()[:24]
        dbt_result = _run_dbt(
            None,
            workdir,
            selector=DBT_SELECTOR,
            extra_vars={
                "bronze_root": str(scratch / "bronze"),
                "bronze_manifest_root": str(scratch / "control" / "manifests"),
            },
        )
        database = dbt_result["database"]
        marts: dict[str, dict[str, Any]] = {}
        for mart, order_by in SALES_MARTS.items():
            local = workdir / f"{mart}.parquet"
            rows = _export_mart(database, mart, order_by, local)
            key = (
                f"gold/domain={GOLD_DOMAIN}/dataset={mart}/schema_version={GOLD_SCHEMA_VERSION}/"
                f"part-{gold_run_id}.parquet"
            )
            put = store.put_bytes(key, local.read_bytes(), content_type="application/vnd.apache.parquet")
            marts[mart] = {"key": put.key, "sha256": put.sha256, "rows": rows, "existed": put.existed}

    manifest = {
        "manifest_version": "1",
        "layer": "gold",
        "source": GOLD_SOURCE,
        "domain": GOLD_DOMAIN,
        "schema_version": GOLD_SCHEMA_VERSION,
        "gold_run_id": gold_run_id,
        "input": {"layer": "bronze", "prefix": BRONZE_PREFIX, "content_sha256": inputs},
        "marts": [
            {"dataset": name, "key": mart["key"], "sha256": mart["sha256"], "rows": mart["rows"]}
            for name, mart in marts.items()
        ],
        "dbt": {"selector": DBT_SELECTOR, "statuses": dbt_result["statuses"]},
        "privacy_class": "internal",
    }
    manifest_put = store.put_bytes(
        f"{GOLD_MANIFEST_PREFIX}gold_run_id={gold_run_id}.json",
        (_canonical_json(manifest) + "\n").encode("utf-8"),
        content_type="application/json",
    )
    existing = store.list_keys(GOLD_POINTER_PREFIX)
    newest = max(existing) if existing else ""
    if newest and f"-{gold_run_id}-" in newest.rsplit("/", 1)[-1]:
        pointer_key, pointer_existed = newest, True
    else:
        pointer = {
            "pointer_version": "1",
            "layer": "gold",
            "source": GOLD_SOURCE,
            "domain": GOLD_DOMAIN,
            "published_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "gold_run_id": gold_run_id,
            "manifest_key": manifest_put.key,
        }
        body = (_canonical_json(pointer) + "\n").encode("utf-8")
        put = store.put_bytes(
            f"{GOLD_POINTER_PREFIX}published_at={now.strftime('%Y%m%dT%H%M%SZ')}-{gold_run_id}-"
            f"{hashlib.sha256(body).hexdigest()[:12]}.json",
            body,
            content_type="application/json",
        )
        pointer_key, pointer_existed = put.key, put.existed
    rejected = marts["sales_rejected_events"]["rows"]
    # Counts, keys and run ids only: the public CI log prints this result.
    return {
        "status": "rejects" if rejected else "success",
        "layer": "gold",
        "gold_run_id": gold_run_id,
        "marts": marts,
        "manifest_key": manifest_put.key,
        "manifest_existed": manifest_put.existed,
        "pointer_key": pointer_key,
        "pointer_existed": pointer_existed,
        "dbt": manifest["dbt"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-lake-uri", default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI", ""))
    parser.add_argument("--json", action="store_true", dest="as_json")
    args = parser.parse_args()
    try:
        result = run_sales_marts(data_lake_uri=args.data_lake_uri)
    except Exception as exc:  # noqa: BLE001 - concise fail-closed CLI boundary
        payload = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        if args.as_json:
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(f"Sales marts: FAILED — {payload['error']}", file=sys.stderr)
        return 2
    if args.as_json:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    else:
        print(f"Sales marts: {result['status'].upper()} ({result['gold_run_id']})")
        for name, mart in result["marts"].items():
            print(f"  {name}: {mart['rows']} rows")
    # Rejected events are still published (they are correct for their input), but
    # exit 3 lets the heartbeat flag the upstream contract break.
    return 3 if result["status"] == "rejects" else 0


if __name__ == "__main__":
    raise SystemExit(main())
