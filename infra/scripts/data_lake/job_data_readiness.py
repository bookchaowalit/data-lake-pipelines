#!/usr/bin/env python3
"""Run a read-only readiness gate for the ``book-job-data`` lake product.

The gate is deliberately anchored on committed ingest manifests.  For each
selected dataset it verifies the immutable landing and Bronze bytes, compares
the manifest record count with the Parquet row count, and checks the Bronze
identity columns before evaluating freshness.  An optional restore drill
copies one verified batch into a local evidence directory; it never writes to
object storage or calls the source provider.

The output is redacted operational metadata only.  It contains object keys,
checksums, counts, and stable alert codes, never source rows or payload JSON.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import sys
import tempfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeGuard

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS_DIR = ROOT / "infra" / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from data_lake.maintenance import restore_manifest
from data_lake.query import DuckDBUnavailable, query_parquet
from data_lake.storage import ObjectNotFound, ObjectStore, StorageError

DEFAULT_SOURCE = "book-job-data"
DEFAULT_DOMAIN = "jobs"
DEFAULT_SCHEMA_VERSION = "1"
DEFAULT_LAKE_URI = str(ROOT / "data" / "lake")
DEFAULT_MAX_MANIFESTS = 5000

# These are the active requirements in scraping-data-coverage.yaml.  History
# datasets remain available for an explicit ``--dataset`` run, but are not
# silently made required when only the current snapshot is scheduled.
DEFAULT_DATASET_MAX_AGE_HOURS: dict[str, float] = {
    "job_postings": 8.0,
    "job_matches": 30.0,
}
DEFAULT_DATASETS = tuple(DEFAULT_DATASET_MAX_AGE_HOURS)
SHA256_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")


def _iso_utc(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _non_negative_int(value: Any) -> TypeGuard[int]:
    return not isinstance(value, bool) and isinstance(value, int) and value >= 0


def _safe_metric(value: Any) -> int:
    """Convert DuckDB aggregate values to stable JSON integers."""
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return 0


def _manifest_batch_id(manifest_key: str) -> str | None:
    match = re.search(r"(?:^|/)batch_id=([^/]+)\.json$", manifest_key)
    return match.group(1) if match else None


def _read_manifest(
    store: ObjectStore, key: str
) -> tuple[dict[str, Any] | None, str | None]:
    """Read one manifest and return a stable public error code on failure."""
    try:
        raw = store.get_bytes(key)
    except ObjectNotFound:
        return None, "manifest_missing"
    except (StorageError, PermissionError):
        return None, "manifest_unreadable"
    except Exception:  # noqa: BLE001 - provider adapters must fail closed
        return None, "manifest_read_error"
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, "manifest_invalid_json"
    if not isinstance(value, dict):
        return None, "manifest_not_object"
    return value, None


def _verify_object(
    store: ObjectStore,
    ref: Any,
    *,
    label: str,
) -> tuple[bytes | None, dict[str, Any], list[str]]:
    """Download and verify one immutable object without exposing its payload."""
    checks: dict[str, Any] = {
        "reference_shape": isinstance(ref, dict),
        "checksum": False,
        "byte_count": False,
    }
    errors: list[str] = []
    if not isinstance(ref, dict):
        errors.append(f"{label}_reference_invalid")
        return None, checks, errors

    key = ref.get("key")
    if not isinstance(key, str) or not key.strip():
        errors.append(f"{label}_key_invalid")
        return None, checks, errors
    expected_sha = ref.get("sha256")
    if not isinstance(expected_sha, str) or not SHA256_PATTERN.fullmatch(expected_sha):
        errors.append(f"{label}_sha256_invalid")
    expected_bytes = ref.get("bytes")
    if expected_bytes is not None and not _non_negative_int(expected_bytes):
        errors.append(f"{label}_bytes_invalid")
    if errors:
        return None, checks, errors

    try:
        payload = store.get_bytes(key.strip("/"))
    except ObjectNotFound:
        errors.append(f"{label}_object_missing")
        return None, checks, errors
    except (StorageError, PermissionError):
        errors.append(f"{label}_object_unreadable")
        return None, checks, errors
    except Exception:  # noqa: BLE001 - fail closed and keep provider errors private
        errors.append(f"{label}_object_read_error")
        return None, checks, errors

    actual_sha = hashlib.sha256(payload).hexdigest()
    checks["checksum"] = actual_sha == str(expected_sha).lower()
    checks["byte_count"] = expected_bytes is None or len(payload) == int(expected_bytes)
    if not checks["checksum"]:
        errors.append(f"{label}_checksum_mismatch")
    if not checks["byte_count"]:
        errors.append(f"{label}_byte_count_mismatch")
    checks["sha256"] = actual_sha
    checks["bytes"] = len(payload)
    return payload, checks, errors


def _source_record_count(payload: bytes, key: str) -> int | None:
    """Count common source formats without returning or logging their rows."""
    suffix = Path(key).suffix.lower()
    if suffix in {".jsonl", ".ndjson"}:
        return sum(
            1 for line in payload.decode("utf-8-sig").splitlines() if line.strip()
        )
    if suffix == ".json":
        value = json.loads(payload.decode("utf-8-sig"))
        return len(value) if isinstance(value, list) else 1
    if suffix == ".csv":
        reader = csv.reader(io.StringIO(payload.decode("utf-8-sig", errors="strict")))
        rows = list(reader)
        if not rows:
            return 0
        count = 0
        for row in rows[1:]:
            # The history copy appends a deterministic dataset marker after
            # the CSV body. It is provenance, not a logical source record.
            if row and str(row[0]).strip().startswith("# book-job-data dataset="):
                continue
            if any(str(value).strip() for value in row):
                count += 1
        return count
    return None


def _query_bronze_metrics(
    payload: bytes, expected: Mapping[str, str]
) -> dict[str, int]:
    """Query only Bronze envelope metadata; never materialize payload rows."""
    with tempfile.TemporaryDirectory(prefix="solo-empire-job-readiness-") as temp_dir:
        parquet_path = Path(temp_dir) / "part.parquet"
        parquet_path.write_bytes(payload)
        rows = query_parquet(
            str(parquet_path),
            """
            SELECT
              COUNT(*) AS row_count,
              COUNT(source_record_id) AS source_record_id_count,
              COUNT(DISTINCT source_record_id) AS unique_source_record_count,
              COUNT(event_id) AS event_id_count,
              COUNT(DISTINCT event_id) AS unique_event_id_count,
              COALESCE(SUM(CASE WHEN source = ? THEN 0 ELSE 1 END), 0) AS invalid_source_count,
              COALESCE(SUM(CASE WHEN domain = ? THEN 0 ELSE 1 END), 0) AS invalid_domain_count,
              COALESCE(SUM(CASE WHEN dataset = ? THEN 0 ELSE 1 END), 0) AS invalid_dataset_count,
              COALESCE(SUM(CASE WHEN schema_version = ? THEN 0 ELSE 1 END), 0) AS invalid_schema_version_count,
              COALESCE(SUM(CASE WHEN received_at = ? THEN 0 ELSE 1 END), 0) AS invalid_received_at_count,
              COALESCE(SUM(CASE WHEN raw_object_key = ? THEN 0 ELSE 1 END), 0) AS invalid_raw_object_key_count,
              COALESCE(SUM(CASE WHEN raw_sha256 = ? THEN 0 ELSE 1 END), 0) AS invalid_raw_sha256_count,
              COALESCE(SUM(CASE WHEN ingest_run_id = ? THEN 0 ELSE 1 END), 0) AS invalid_ingest_run_id_count,
              COALESCE(SUM(CASE WHEN source_record_id IS NULL OR trim(source_record_id) = '' THEN 1 ELSE 0 END), 0) AS missing_source_record_id_count,
              COALESCE(SUM(CASE WHEN event_id IS NULL OR trim(event_id) = '' THEN 1 ELSE 0 END), 0) AS missing_event_id_count
            FROM lake_table
            """,
            [
                # Keep the predicates parameterized; these values come from
                # the expected contract, not from SQL or source payloads.
                expected["source"],
                expected["domain"],
                expected["dataset"],
                expected["schema_version"],
                expected["received_at"],
                expected["raw_object_key"],
                expected["raw_sha256"],
                expected["ingest_run_id"],
            ],
        )
    if not rows:
        return {key: 0 for key in _METRIC_KEYS}
    return {key: _safe_metric(rows[0].get(key)) for key in _METRIC_KEYS}


_METRIC_KEYS = (
    "row_count",
    "source_record_id_count",
    "unique_source_record_count",
    "event_id_count",
    "unique_event_id_count",
    "invalid_source_count",
    "invalid_domain_count",
    "invalid_dataset_count",
    "invalid_schema_version_count",
    "invalid_received_at_count",
    "invalid_raw_object_key_count",
    "invalid_raw_sha256_count",
    "invalid_ingest_run_id_count",
    "missing_source_record_id_count",
    "missing_event_id_count",
)


def _dataset_thresholds(
    datasets: Sequence[str], max_age_hours: float | None
) -> dict[str, float]:
    if max_age_hours is not None and (
        not math.isfinite(max_age_hours) or max_age_hours <= 0
    ):
        raise ValueError("max_age_hours must be positive and finite")
    thresholds: dict[str, float] = {}
    for dataset in datasets:
        if not isinstance(dataset, str) or not dataset.strip():
            raise ValueError("dataset names must be non-empty strings")
        thresholds[dataset] = (
            float(max_age_hours)
            if max_age_hours is not None
            else DEFAULT_DATASET_MAX_AGE_HOURS.get(dataset, 30.0)
        )
    return thresholds


def _validate_key_prefix(key: Any, *, expected: str, label: str) -> str | None:
    if not isinstance(key, str) or not key.strip():
        return f"{label}_key_invalid"
    normalized = key.strip("/")
    if not normalized.startswith(expected):
        return f"{label}_key_outside_contract"
    return None


def _reconcile_manifest(
    store: ObjectStore,
    manifest_key: str,
    manifest: dict[str, Any],
    *,
    source: str,
    domain: str,
    schema_version: str,
    dataset: str,
) -> dict[str, Any]:
    """Return a redacted reconciliation record for one committed batch."""
    errors: list[str] = []
    batch_id = manifest.get("batch_id")
    run_id = manifest.get("run_id")
    received_at_value = manifest.get("received_at")
    received_at = _parse_timestamp(received_at_value)
    record_count = manifest.get("record_count")
    if manifest.get("manifest_version") != "1":
        errors.append("manifest_version_invalid")
    for field, expected in (
        ("source", source),
        ("domain", domain),
        ("dataset", dataset),
        ("schema_version", schema_version),
    ):
        if manifest.get(field) != expected:
            errors.append(f"identity_{field}_mismatch")
    if not isinstance(batch_id, str) or not batch_id:
        errors.append("batch_id_invalid")
    else:
        key_batch_id = _manifest_batch_id(manifest_key)
        if key_batch_id is None:
            errors.append("batch_id_key_missing")
        elif key_batch_id != batch_id:
            errors.append("batch_id_key_mismatch")
    if not isinstance(run_id, str) or not run_id:
        errors.append("run_id_invalid")
    if received_at is None:
        errors.append("received_at_invalid")
    if not _non_negative_int(record_count) or int(record_count) == 0:
        errors.append("record_count_invalid_or_empty")

    raw_ref = manifest.get("raw")
    bronze_ref = manifest.get("bronze")
    raw_key = raw_ref.get("key") if isinstance(raw_ref, dict) else None
    bronze_key = bronze_ref.get("key") if isinstance(bronze_ref, dict) else None
    raw_key_error = _validate_key_prefix(
        raw_key,
        expected=f"landing/source={source}/",
        label="raw",
    )
    bronze_key_error = _validate_key_prefix(
        bronze_key,
        expected=(
            f"bronze/domain={domain}/dataset={dataset}/schema_version={schema_version}/"
        ),
        label="bronze",
    )
    if bronze_key_error is None and (
        not isinstance(bronze_key, str)
        or f"/source={source}/" not in bronze_key.strip("/")
        or not bronze_key.strip("/").endswith(".parquet")
    ):
        bronze_key_error = "bronze_key_outside_contract"
    if raw_key_error:
        errors.append(raw_key_error)
    if bronze_key_error:
        errors.append(bronze_key_error)

    checkpoint_key = manifest.get("checkpoint_key")
    if not isinstance(checkpoint_key, str) or not checkpoint_key.startswith(
        f"control/checkpoints/source={source}/"
    ):
        errors.append("checkpoint_key_invalid")
    elif (
        isinstance(batch_id, str)
        and batch_id
        and not checkpoint_key.strip("/").endswith(f"batch_id={batch_id}.json")
    ):
        errors.append("checkpoint_key_mismatch")

    raw_payload: bytes | None = None
    bronze_payload: bytes | None = None
    raw_checks: dict[str, Any] = {
        "reference_shape": isinstance(raw_ref, dict),
        "checksum": False,
        "byte_count": False,
    }
    bronze_checks: dict[str, Any] = {
        "reference_shape": isinstance(bronze_ref, dict),
        "checksum": False,
        "byte_count": False,
    }
    if raw_key_error is None:
        raw_payload, raw_checks, object_errors = _verify_object(
            store, raw_ref, label="raw"
        )
        errors.extend(object_errors)
    else:
        errors.append("raw_object_not_checked")
    if bronze_key_error is None:
        bronze_payload, bronze_checks, object_errors = _verify_object(
            store, bronze_ref, label="bronze"
        )
        errors.extend(object_errors)
    else:
        errors.append("bronze_object_not_checked")

    raw_record_count: int | None = None
    raw_record_count_expected = (
        int(record_count) if _non_negative_int(record_count) else None
    )
    # A capture producer may deterministically deduplicate source rows before
    # writing Bronze.  The manifest count is then the published logical grain,
    # while metadata records the source row count that must reconcile with the
    # immutable landing bytes.  Fall back to the old one-to-one expectation for
    # producers that do not declare this mapping.
    metadata = manifest.get("metadata")
    if isinstance(metadata, dict) and _non_negative_int(
        metadata.get("record_count_before_dedupe")
    ):
        raw_record_count_expected = int(metadata["record_count_before_dedupe"])
    if raw_payload is not None and isinstance(raw_key, str):
        try:
            raw_record_count = _source_record_count(raw_payload, raw_key)
        except (UnicodeDecodeError, csv.Error, json.JSONDecodeError, ValueError):
            errors.append("raw_record_count_unreadable")
        if (
            raw_record_count is not None
            and raw_record_count_expected is not None
            and raw_record_count != raw_record_count_expected
        ):
            errors.append("raw_record_count_mismatch")

    checkpoint_present = False
    if isinstance(checkpoint_key, str) and not any(
        error == "checkpoint_key_invalid" for error in errors
    ):
        try:
            checkpoint_payload = store.get_bytes(checkpoint_key.strip("/"))
            checkpoint_present = True
            checkpoint = json.loads(checkpoint_payload.decode("utf-8"))
            if not isinstance(checkpoint, dict):
                errors.append("checkpoint_invalid")
            else:
                expected_checkpoint = {
                    "source": source,
                    "domain": domain,
                    "dataset": dataset,
                    "schema_version": schema_version,
                    "raw_sha256": (
                        str(raw_ref.get("sha256")).lower()
                        if isinstance(raw_ref, dict)
                        else ""
                    ),
                    "received_at": str(received_at_value),
                    "run_id": str(run_id),
                    "raw_key": str(raw_key).strip("/")
                    if isinstance(raw_key, str)
                    else "",
                    "bronze_key": str(bronze_key).strip("/")
                    if isinstance(bronze_key, str)
                    else "",
                }
                if any(
                    checkpoint.get(field) != value
                    for field, value in expected_checkpoint.items()
                ):
                    errors.append("checkpoint_identity_mismatch")
        except (UnicodeDecodeError, json.JSONDecodeError):
            errors.append("checkpoint_invalid")
        except ObjectNotFound:
            errors.append("checkpoint_missing")
        except (StorageError, PermissionError):
            errors.append("checkpoint_unreadable")
        except Exception:  # noqa: BLE001 - keep provider errors out of reports
            errors.append("checkpoint_read_error")

    metrics: dict[str, int] = {key: 0 for key in _METRIC_KEYS}
    if bronze_payload is not None:
        expected_row = {
            "source": source,
            "domain": domain,
            "dataset": dataset,
            "schema_version": schema_version,
            "received_at": str(received_at_value),
            "raw_object_key": str(raw_key).strip("/")
            if isinstance(raw_key, str)
            else "",
            "raw_sha256": (
                str(raw_ref.get("sha256")).lower() if isinstance(raw_ref, dict) else ""
            ),
            "ingest_run_id": str(run_id),
        }
        try:
            metrics = _query_bronze_metrics(bronze_payload, expected_row)
        except DuckDBUnavailable:
            errors.append("duckdb_unavailable")
        except Exception:  # noqa: BLE001 - Parquet/provider errors are redacted
            errors.append("bronze_query_failed")

    if (
        metrics["row_count"] != int(record_count)
        if _non_negative_int(record_count)
        else True
    ):
        errors.append("record_count_row_count_mismatch")
    if metrics["row_count"] == 0:
        errors.append("bronze_empty")
    if metrics["source_record_id_count"] != metrics["row_count"]:
        errors.append("source_record_id_missing")
    if metrics["unique_source_record_count"] != metrics["row_count"]:
        errors.append("source_record_id_duplicate")
    if metrics["event_id_count"] != metrics["row_count"]:
        errors.append("event_id_missing")
    if metrics["unique_event_id_count"] != metrics["row_count"]:
        errors.append("event_id_duplicate")
    for metric_key in (
        "invalid_source_count",
        "invalid_domain_count",
        "invalid_dataset_count",
        "invalid_schema_version_count",
        "invalid_received_at_count",
        "invalid_raw_object_key_count",
        "invalid_raw_sha256_count",
        "invalid_ingest_run_id_count",
        "missing_source_record_id_count",
        "missing_event_id_count",
    ):
        if metrics[metric_key] > 0:
            errors.append(metric_key)

    record: dict[str, Any] = {
        "status": "passed" if not errors else "failed",
        "manifest_key": manifest_key,
        "batch_id": batch_id,
        "run_id": run_id,
        "dataset": dataset,
        "received_at": _iso_utc(received_at) if received_at else None,
        "manifest_record_count": int(record_count)
        if _non_negative_int(record_count)
        else None,
        "raw_key": str(raw_key).strip("/") if isinstance(raw_key, str) else None,
        "bronze_key": str(bronze_key).strip("/")
        if isinstance(bronze_key, str)
        else None,
        "raw_sha256": raw_checks.get("sha256"),
        "bronze_sha256": bronze_checks.get("sha256"),
        "raw_bytes": raw_checks.get("bytes"),
        "bronze_bytes": bronze_checks.get("bytes"),
        "raw_record_count": raw_record_count,
        "raw_record_count_expected": raw_record_count_expected,
        "checks": {
            "raw_checksum": bool(raw_checks.get("checksum")),
            "raw_byte_count": bool(raw_checks.get("byte_count")),
            "bronze_checksum": bool(bronze_checks.get("checksum")),
            "bronze_byte_count": bool(bronze_checks.get("byte_count")),
            "checkpoint_key_shape": not any(
                error in {"checkpoint_key_invalid", "checkpoint_key_mismatch"}
                for error in errors
            ),
            "checkpoint_present": checkpoint_present,
            "checkpoint_identity": checkpoint_present
            and not any(
                error in {"checkpoint_invalid", "checkpoint_identity_mismatch"}
                for error in errors
            ),
        },
        "metrics": metrics,
        "errors": sorted(set(errors)),
    }
    return record


def _freshness_report(
    dataset: str,
    batches: Iterable[dict[str, Any]],
    *,
    threshold_hours: float,
    now: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    passed = [
        item
        for item in batches
        if item.get("status") == "passed" and _parse_timestamp(item.get("received_at"))
    ]
    latest_item = max(
        passed,
        key=lambda item: (
            _parse_timestamp(item.get("received_at"))
            or datetime.min.replace(tzinfo=timezone.utc)
        ),
        default=None,
    )
    alerts: list[dict[str, Any]] = []
    if latest_item is None:
        freshness = {
            "status": "missing",
            "latest_received_at": None,
            "age_hours": None,
            "max_age_hours": threshold_hours,
            "passed_batches": len(passed),
        }
        alerts.append(
            {
                "code": "JOB_DATA_DATASET_MISSING",
                "severity": "error",
                "dataset": dataset,
                "message": "no verified committed batch is available",
            }
        )
        return freshness, alerts

    latest = _parse_timestamp(latest_item.get("received_at"))
    assert latest is not None
    age_hours = (now - latest).total_seconds() / 3600
    freshness = {
        "status": "fresh" if 0 <= age_hours <= threshold_hours else "stale",
        "latest_received_at": _iso_utc(latest),
        "latest_manifest_key": latest_item.get("manifest_key"),
        "age_hours": round(age_hours, 2),
        "max_age_hours": threshold_hours,
        "passed_batches": len(passed),
    }
    if age_hours < 0:
        freshness["status"] = "future"
        alerts.append(
            {
                "code": "JOB_DATA_FRESHNESS_CLOCK_SKEW",
                "severity": "error",
                "dataset": dataset,
                "message": "latest verified batch timestamp is in the future",
            }
        )
    elif age_hours > threshold_hours:
        alerts.append(
            {
                "code": "JOB_DATA_FRESHNESS_STALE",
                "severity": "error",
                "dataset": dataset,
                "message": "latest verified batch exceeds its freshness threshold",
            }
        )
    return freshness, alerts


def build_job_readiness_report(
    data_lake_uri: str,
    *,
    source: str = DEFAULT_SOURCE,
    domain: str = DEFAULT_DOMAIN,
    schema_version: str = DEFAULT_SCHEMA_VERSION,
    datasets: Sequence[str] | None = None,
    max_age_hours: float | None = None,
    now: datetime | None = None,
    restore_manifest_key: str | None = None,
    restore_latest: bool = False,
    output_dir: str | Path | None = None,
    max_manifests: int = DEFAULT_MAX_MANIFESTS,
) -> dict[str, Any]:
    """Build a redacted, read-only readiness report for selected job datasets."""
    selected = tuple(dict.fromkeys(datasets or DEFAULT_DATASETS))
    if not selected:
        raise ValueError("at least one dataset is required")
    thresholds = _dataset_thresholds(selected, max_age_hours)
    if (
        not isinstance(max_manifests, int)
        or isinstance(max_manifests, bool)
        or max_manifests < 1
    ):
        raise ValueError("max_manifests must be a positive integer")
    if restore_manifest_key and restore_latest:
        raise ValueError("choose either restore_manifest_key or restore_latest")
    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

    store = ObjectStore(data_lake_uri)
    if store.scheme == "s3" and not store.prefix:
        raise ValueError("cloud readiness requires an explicit s3://bucket/prefix")

    manifest_prefix = f"control/manifests/source={source}"
    try:
        manifest_keys = [
            key
            for key in store.list_keys(manifest_prefix)
            if key.startswith(manifest_prefix + "/") and key.endswith(".json")
        ]
    except (StorageError, PermissionError):
        return {
            "status": "attention",
            "mode": "read_only_book_job_data_readiness",
            "source": source,
            "domain": domain,
            "schema_version": schema_version,
            "checked_at": _iso_utc(now_utc),
            "data_lake": store.describe(),
            "datasets": {dataset: {"status": "unavailable"} for dataset in selected},
            "batches": [],
            "alerts": [
                {
                    "code": "JOB_DATA_MANIFEST_LIST_FAILED",
                    "severity": "error",
                    "message": "cannot list the bounded source manifest prefix",
                }
            ],
            "reconciliation": {
                "manifest_count": 0,
                "checked_batches": 0,
                "passed_batches": 0,
                "failed_batches": 0,
                "skipped_manifests": 0,
                "duplicate_batch_ids": [],
            },
            "freshness": {"status": "attention", "datasets": {}},
            "recovery": {"requested": bool(restore_manifest_key or restore_latest)},
            "safety": {
                "object_storage_writes": False,
                "deletes": False,
                "source_provider_calls": False,
            },
        }
    manifest_keys = sorted(manifest_keys)
    alerts: list[dict[str, Any]] = []
    if len(manifest_keys) > max_manifests:
        alerts.append(
            {
                "code": "JOB_DATA_MANIFEST_LIMIT_EXCEEDED",
                "severity": "error",
                "message": "manifest scan limit exceeded; use a bounded lake prefix or raise the reviewed limit",
            }
        )
        manifest_keys = manifest_keys[:max_manifests]

    batches: list[dict[str, Any]] = []
    skipped_manifests = 0
    for manifest_key in manifest_keys:
        manifest, read_error = _read_manifest(store, manifest_key)
        if read_error:
            batches.append(
                {
                    "status": "failed",
                    "manifest_key": manifest_key,
                    "batch_id": _manifest_batch_id(manifest_key),
                    "dataset": None,
                    "errors": [read_error],
                }
            )
            alerts.append(
                {
                    "code": "JOB_DATA_RECONCILIATION_FAILED",
                    "severity": "error",
                    "manifest_key": manifest_key,
                    "message": read_error,
                }
            )
            continue
        assert manifest is not None
        manifest_dataset = manifest.get("dataset")
        if manifest_dataset not in selected:
            skipped_manifests += 1
            continue
        batch = _reconcile_manifest(
            store,
            manifest_key,
            manifest,
            source=source,
            domain=domain,
            schema_version=schema_version,
            dataset=str(manifest_dataset),
        )
        batches.append(batch)
        if batch["status"] != "passed":
            alerts.append(
                {
                    "code": "JOB_DATA_RECONCILIATION_FAILED",
                    "severity": "error",
                    "dataset": batch.get("dataset"),
                    "manifest_key": manifest_key,
                    "message": "one or more manifest, object, or Bronze quality checks failed",
                    "checks_failed": batch.get("errors", []),
                }
            )

    batch_ids: dict[str, list[str]] = defaultdict(list)
    for batch in batches:
        batch_id = batch.get("batch_id")
        if isinstance(batch_id, str) and batch_id:
            batch_ids[batch_id].append(str(batch.get("manifest_key")))
    duplicate_batch_ids = sorted(
        batch_id for batch_id, keys in batch_ids.items() if len(keys) > 1
    )
    for batch_id in duplicate_batch_ids:
        alerts.append(
            {
                "code": "JOB_DATA_DUPLICATE_BATCH_ID",
                "severity": "error",
                "batch_id": batch_id,
                "message": "multiple selected manifests share one batch identity",
            }
        )

    dataset_reports: dict[str, dict[str, Any]] = {}
    freshness_reports: dict[str, dict[str, Any]] = {}
    for dataset in selected:
        dataset_batches = [
            batch for batch in batches if batch.get("dataset") == dataset
        ]
        passed = [batch for batch in dataset_batches if batch.get("status") == "passed"]
        dataset_reports[dataset] = {
            "status": "verified"
            if passed and len(passed) == len(dataset_batches)
            else "attention",
            "manifest_count": len(dataset_batches),
            "passed_batches": len(passed),
            "failed_batches": len(dataset_batches) - len(passed),
            "verified_record_count": sum(
                int(batch.get("metrics", {}).get("row_count", 0)) for batch in passed
            ),
        }
        freshness, freshness_alerts = _freshness_report(
            dataset,
            dataset_batches,
            threshold_hours=thresholds[dataset],
            now=now_utc,
        )
        freshness_reports[dataset] = freshness
        alerts.extend(freshness_alerts)

    recovery: dict[str, Any] = {
        "requested": bool(restore_manifest_key or restore_latest),
        "object_storage_writes": False,
    }
    selected_restore_key = restore_manifest_key
    if restore_latest and selected_restore_key is None:
        successful_batches = [
            batch for batch in batches if batch.get("status") == "passed"
        ]
        if successful_batches:
            selected_restore_key = max(
                successful_batches,
                key=lambda item: (
                    _parse_timestamp(item.get("received_at"))
                    or datetime.min.replace(tzinfo=timezone.utc)
                ),
            ).get("manifest_key")
        else:
            alerts.append(
                {
                    "code": "JOB_DATA_RESTORE_UNAVAILABLE",
                    "severity": "error",
                    "message": "restore-latest requested but no verified batch is available",
                }
            )
    if selected_restore_key:
        normalized_restore_key = str(selected_restore_key).strip("/")
        verified_restore = next(
            (
                batch
                for batch in batches
                if batch.get("manifest_key") == normalized_restore_key
                and batch.get("status") == "passed"
            ),
            None,
        )
        if (
            not normalized_restore_key.startswith(f"control/manifests/source={source}/")
            or verified_restore is None
        ):
            recovery = {
                "requested": True,
                "status": "failed",
                "manifest_key": normalized_restore_key,
                "object_storage_writes": False,
            }
            alerts.append(
                {
                    "code": "JOB_DATA_RESTORE_NOT_VERIFIED",
                    "severity": "error",
                    "manifest_key": normalized_restore_key,
                    "message": "restore must target a verified selected book-job-data batch",
                }
            )
        else:
            try:
                recovery = restore_manifest(
                    data_lake_uri,
                    normalized_restore_key,
                    output_dir=output_dir,
                )
                recovery["requested"] = True
                recovery["object_storage_writes"] = False
            except Exception:  # noqa: BLE001 - evidence output must remain redacted
                recovery = {
                    "requested": True,
                    "status": "failed",
                    "manifest_key": normalized_restore_key,
                    "object_storage_writes": False,
                }
                alerts.append(
                    {
                        "code": "JOB_DATA_RESTORE_FAILED",
                        "severity": "error",
                        "manifest_key": normalized_restore_key,
                        "message": "read-only restore verification failed",
                    }
                )

    failed_batches = sum(1 for batch in batches if batch.get("status") != "passed")
    reconciliation = {
        "manifest_count": len(manifest_keys),
        "checked_batches": len(batches),
        "passed_batches": len(batches) - failed_batches,
        "failed_batches": failed_batches,
        "skipped_manifests": skipped_manifests,
        "duplicate_batch_ids": duplicate_batch_ids,
    }
    status = "ok" if not alerts and batches else "attention"
    return {
        "status": status,
        "mode": "read_only_book_job_data_readiness",
        "source": source,
        "domain": domain,
        "schema_version": schema_version,
        "checked_at": _iso_utc(now_utc),
        "data_lake": store.describe(),
        "datasets": dataset_reports,
        "batches": batches,
        "alerts": alerts,
        "reconciliation": reconciliation,
        "freshness": {
            "status": "ok"
            if not any(
                alert["code"].startswith("JOB_DATA_FRESHNESS")
                or alert["code"] == "JOB_DATA_DATASET_MISSING"
                for alert in alerts
            )
            else "attention",
            "datasets": freshness_reports,
        },
        "recovery": recovery,
        "safety": {
            "object_storage_writes": False,
            "deletes": False,
            "source_provider_calls": False,
            "payloads_included": False,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-lake-uri",
        default=(
            os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI")
            or os.environ.get("DATA_LAKE_URI")
            or DEFAULT_LAKE_URI
        ),
    )
    parser.add_argument("--source", default=DEFAULT_SOURCE)
    parser.add_argument("--domain", default=DEFAULT_DOMAIN)
    parser.add_argument("--schema-version", default=DEFAULT_SCHEMA_VERSION)
    parser.add_argument(
        "--dataset",
        action="append",
        dest="datasets",
        help="Dataset to check; repeat to check multiple datasets (defaults to postings and matches)",
    )
    parser.add_argument(
        "--max-age-hours",
        type=float,
        help="Override the per-dataset freshness threshold for this run",
    )
    parser.add_argument(
        "--now",
        help="Optional ISO-8601 clock for deterministic/offline checks",
    )
    parser.add_argument("--restore-manifest-key")
    parser.add_argument("--restore-latest", action="store_true")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--max-manifests", type=int, default=DEFAULT_MAX_MANIFESTS)
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        now = _parse_timestamp(args.now) if args.now else None
        if args.now and now is None:
            raise ValueError("--now must be a valid ISO-8601 timestamp")
        report = build_job_readiness_report(
            args.data_lake_uri,
            source=args.source,
            domain=args.domain,
            schema_version=args.schema_version,
            datasets=args.datasets,
            max_age_hours=args.max_age_hours,
            now=now,
            restore_manifest_key=args.restore_manifest_key,
            restore_latest=args.restore_latest,
            output_dir=args.output_dir,
            max_manifests=args.max_manifests,
        )
    except (ValueError, StorageError, DuckDBUnavailable) as exc:
        report = {
            "status": "failed",
            "mode": "read_only_book_job_data_readiness",
            "error": type(exc).__name__,
            "safety": {
                "object_storage_writes": False,
                "deletes": False,
                "source_provider_calls": False,
            },
        }
        if args.as_json:
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        else:
            print(
                f"book-job-data readiness: FAILED — {report['error']}", file=sys.stderr
            )
        return 2

    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print(f"book-job-data readiness: {report['status']}")
        print(
            "  batches: "
            f"{report['reconciliation']['passed_batches']} passed / "
            f"{report['reconciliation']['failed_batches']} failed"
        )
        for dataset, freshness in report["freshness"]["datasets"].items():
            print(f"  {dataset}: {freshness['status']}")
        if report["alerts"]:
            print(f"  alerts: {len(report['alerts'])}")
        if report.get("recovery", {}).get("requested"):
            print(f"  recovery: {report['recovery'].get('status', 'failed')}")
    return 0 if report["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
