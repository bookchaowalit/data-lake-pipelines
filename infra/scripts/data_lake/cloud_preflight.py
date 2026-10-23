#!/usr/bin/env python3
"""Validate a cloud data-lake target and optionally inspect bucket controls.

This command is deliberately safe to run in CI. It checks URI shape, required
R2/S3 endpoint settings, explicit remote-write opt-in, catalog token presence,
and the per-object upload budget. The default mode never contacts Object
Storage or a catalog service. ``--check-storage-controls`` is an explicit,
read-only provider call for supported durability, encryption, lifecycle, and
public-access settings; it accounts for provider differences (for example,
Cloudflare R2 does not expose S3 bucket-versioning or public-access-block
operations) and never writes objects or prints secret values.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any
from urllib.parse import urlparse

try:
    from .storage import ObjectStore, StorageError
except ImportError:  # direct ``python path/to/cloud_preflight.py`` execution
    from storage import ObjectStore, StorageError  # type: ignore[no-redef]


TRUE_VALUES = {"1", "true", "yes", "on"}
DEFAULT_MAX_OBJECT_BYTES = 64 * 1024 * 1024

_S3_STORAGE_CONTROL_OPERATIONS = (
    "get-bucket-versioning",
    "get-bucket-encryption",
    "get-bucket-lifecycle-configuration",
    "get-public-access-block",
)
_R2_STORAGE_CONTROL_OPERATIONS = ("get-bucket-lifecycle-configuration",)
_SUPPORTED_STORAGE_PROVIDERS = {"s3", "minio", "r2"}


def _is_true(value: str | None) -> bool:
    return str(value or "").strip().lower() in TRUE_VALUES


def _is_local_endpoint(endpoint: str | None) -> bool:
    if not endpoint:
        return False
    return urlparse(endpoint).hostname in {
        "localhost",
        "127.0.0.1",
        "::1",
        "minio",
        "se-minio",
    }


def _storage_provider(endpoint: str | None) -> str:
    """Return the provider profile used by the read-only control check."""
    configured = (
        os.environ.get("DATA_LAKE_STORAGE_PROVIDER")
        or os.environ.get("DATA_LAKE_PROVIDER")
        or ""
    ).strip().lower()
    if configured in _SUPPORTED_STORAGE_PROVIDERS:
        return configured
    if configured:
        return "unknown"
    if _is_local_endpoint(endpoint):
        return "minio"
    hostname = (urlparse(endpoint or "").hostname or "").lower().rstrip(".")
    if hostname == "r2.cloudflarestorage.com" or hostname.endswith(
        ".r2.cloudflarestorage.com"
    ):
        return "r2"
    return "s3"


def _max_object_bytes() -> int:
    raw = os.environ.get(
        "DATA_LAKE_CLOUD_MAX_OBJECT_BYTES",
        str(DEFAULT_MAX_OBJECT_BYTES),
    )
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(
            "DATA_LAKE_CLOUD_MAX_OBJECT_BYTES must be a non-negative integer"
        ) from exc
    if value < 0:
        raise ValueError("DATA_LAKE_CLOUD_MAX_OBJECT_BYTES must be non-negative")
    return value


def validate_config(
    *,
    data_lake_uri: str,
    catalog_uri: str | None = None,
    require_cloud_write: bool = False,
) -> dict[str, Any]:
    """Return a redacted validation report for local or hosted lake settings."""
    parsed = urlparse(data_lake_uri)
    scheme = parsed.scheme.lower() or "file"
    errors: list[str] = []
    warnings: list[str] = []
    endpoint = os.environ.get("DATA_LAKE_S3_ENDPOINT") or os.environ.get(
        "AWS_ENDPOINT_URL"
    )
    local_s3 = _is_local_endpoint(endpoint)

    if scheme == "file":
        mode = "local"
    elif scheme == "s3":
        mode = "s3-local" if local_s3 else "cloud"
        if not parsed.netloc:
            errors.append("s3:// URI must include a bucket")
        if not endpoint:
            errors.append(
                "DATA_LAKE_S3_ENDPOINT or AWS_ENDPOINT_URL is required for "
                "an explicit S3-compatible target"
            )
        if not local_s3 and require_cloud_write and not _is_true(
            os.environ.get("DATA_LAKE_CLOUD_WRITE_ENABLED")
        ):
            errors.append(
                "DATA_LAKE_CLOUD_WRITE_ENABLED=true is required for cloud writes"
            )
        if not local_s3 and not require_cloud_write:
            warnings.append("cloud writes are not requested by this preflight")
    else:
        errors.append("data lake URI must use file:// or s3://")
        mode = "invalid"

    if catalog_uri:
        catalog_scheme = urlparse(catalog_uri).scheme.lower()
        if catalog_scheme in {"http", "https"}:
            if not os.environ.get("ICEBERG_CATALOG_TOKEN") and not os.environ.get(
                "R2_DATA_CATALOG_TOKEN"
            ):
                errors.append(
                    "REST catalog requires ICEBERG_CATALOG_TOKEN or "
                    "R2_DATA_CATALOG_TOKEN"
                )
        elif not (
            catalog_uri.startswith("sqlite://")
            or catalog_uri.startswith("postgresql")
        ):
            errors.append("catalog URI must be SQLite, PostgreSQL, or HTTP(S) REST")
    elif mode == "cloud":
        warnings.append("cloud data is configured without an Iceberg catalog")

    try:
        max_bytes = _max_object_bytes()
    except ValueError as exc:
        errors.append(str(exc))
        max_bytes = 0

    return {
        "status": "error" if errors else "ok",
        "mode": mode,
        "data_lake_uri": data_lake_uri,
        "catalog_uri": catalog_uri,
        "endpoint_configured": bool(endpoint),
        "cloud_write_enabled": _is_true(
            os.environ.get("DATA_LAKE_CLOUD_WRITE_ENABLED")
        ),
        "max_object_bytes": max_bytes,
        "errors": errors,
        "warnings": warnings,
    }


def _control_failure(operation: str, reason: str) -> dict[str, object]:
    return {
        "operation": operation,
        "status": "unavailable",
        "reason": reason,
    }


def _read_bucket_control(store: ObjectStore, operation: str) -> dict[str, object]:
    """Read one bucket control without exposing provider stderr or secrets."""
    try:
        result = store._run_aws(  # noqa: SLF001 - storage owns the AWS adapter
            [
                "s3api",
                operation,
                "--bucket",
                str(store.bucket),
                *store._aws_args(),  # noqa: SLF001 - central endpoint handling
            ]
        )
    except StorageError:
        return _control_failure(operation, "aws_cli_unavailable")
    if result.returncode != 0:
        stderr = result.stderr.decode("utf-8", errors="replace").lower()
        reason = (
            "provider_unsupported"
            if any(
                marker in stderr
                for marker in ("not implemented", "not supported", "unknown operation")
            )
            else "provider_or_permission_error"
        )
        return _control_failure(operation, reason)
    try:
        payload = json.loads(result.stdout.decode("utf-8") or "{}")
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _control_failure(operation, "invalid_provider_response")
    if not isinstance(payload, dict):
        return _control_failure(operation, "invalid_provider_response")
    return {"operation": operation, "status": "read", "payload": payload}


def check_storage_controls(data_lake_uri: str) -> dict[str, object]:
    """Verify bucket durability/privacy controls with read-only S3 calls.

    The result contains only control summaries and no provider response
    payload, endpoint credentials, or object contents.
    """
    store = ObjectStore(data_lake_uri)
    if store.scheme != "s3":
        return {
            "status": "not_applicable",
            "mode": "local",
            "data_lake_uri": data_lake_uri,
            "object_storage_writes": False,
            "deletes": False,
        }
    if not store.bucket:
        return {
            "status": "blocked",
            "mode": "s3",
            "data_lake_uri": data_lake_uri,
            "reason": "s3_uri_missing_bucket",
            "object_storage_writes": False,
            "deletes": False,
        }

    endpoint = os.environ.get("DATA_LAKE_S3_ENDPOINT") or os.environ.get(
        "AWS_ENDPOINT_URL"
    )
    provider = _storage_provider(endpoint)
    if provider == "unknown":
        return {
            "status": "blocked",
            "mode": "s3_read_only",
            "provider": provider,
            "data_lake_uri": data_lake_uri,
            "bucket": store.bucket,
            "prefix": store.prefix,
            "reason": "unsupported_DATA_LAKE_STORAGE_PROVIDER",
            "object_storage_writes": False,
            "deletes": False,
        }

    operations = (
        _R2_STORAGE_CONTROL_OPERATIONS
        if provider == "r2"
        else _S3_STORAGE_CONTROL_OPERATIONS
    )
    raw = {
        operation: _read_bucket_control(store, operation)
        for operation in operations
    }
    controls: dict[str, dict[str, object]] = {}

    def payload_for(operation: str) -> dict[str, object]:
        value = raw[operation].get("payload")
        return value if isinstance(value, dict) else {}

    if provider == "r2":
        # R2 encrypts every object at rest automatically, but its S3 API does
        # not expose bucket versioning or public-access-block operations. The
        # replacement durability control is immutable object keys plus a
        # separately evidenced off-host copy; the public exposure check stays
        # manual because custom domains and r2.dev are Cloudflare controls.
        controls["versioning"] = {
            "status": "not_applicable",
            "required": False,
            "operation": "provider-capability",
            "reason": "r2_does_not_support_bucket_versioning",
        }
        controls["encryption"] = {
            "status": "pass",
            "required": True,
            "operation": "provider-managed-encryption",
            "evidence": "r2_automatic_aes256_encryption",
        }
    else:
        versioning_payload = payload_for("get-bucket-versioning")
        controls["versioning"] = {
            "status": "pass"
            if versioning_payload.get("Status") == "Enabled"
            else "fail",
            "required": True,
            "operation": "get-bucket-versioning",
        }

        encryption_payload = payload_for("get-bucket-encryption")
        encryption_config = encryption_payload.get(
            "ServerSideEncryptionConfiguration", {}
        )
        encryption_rules = (
            encryption_config.get("Rules", [])
            if isinstance(encryption_config, dict)
            else []
        )
        controls["encryption"] = {
            "status": "pass"
            if isinstance(encryption_rules, list) and bool(encryption_rules)
            else "fail",
            "required": True,
            "operation": "get-bucket-encryption",
        }

    lifecycle_payload = payload_for("get-bucket-lifecycle-configuration")
    lifecycle_rules = lifecycle_payload.get("Rules", [])
    controls["lifecycle"] = {
        "status": "pass"
        if isinstance(lifecycle_rules, list)
        and any(
            str(rule.get("Status", "")).lower() == "enabled"
            for rule in lifecycle_rules
            if isinstance(rule, dict)
        )
        else "fail",
        "required": True,
        "operation": "get-bucket-lifecycle-configuration",
    }

    if provider == "r2":
        controls["public_access_block"] = {
            "status": "manual",
            "required": True,
            "operation": "cloudflare-control-plane",
            "reason": "verify_r2_public_urls_and_custom_domains_are_disabled",
        }
    else:
        public_payload = payload_for("get-public-access-block")
        public_config = public_payload.get("PublicAccessBlockConfiguration", {})
        public_flags = (
            "BlockPublicAcls",
            "IgnorePublicAcls",
            "BlockPublicPolicy",
            "RestrictPublicBuckets",
        )
        controls["public_access_block"] = {
            "status": "pass"
            if isinstance(public_config, dict)
            and all(public_config.get(flag) is True for flag in public_flags)
            else "fail",
            "required": True,
            "operation": "get-public-access-block",
        }

    unavailable = {
        operation: value["reason"]
        for operation, value in raw.items()
        if value.get("status") == "unavailable"
    }
    failed = sorted(
        name
        for name, value in controls.items()
        if value.get("required", True)
        and value["status"] not in {"pass", "not_applicable"}
    )
    manual = sorted(
        name for name, value in controls.items() if value["status"] == "manual"
    )
    return {
        "status": "ok"
        if not failed and not unavailable and not manual
        else "attention",
        "mode": "s3_read_only",
        "provider": provider,
        "data_lake_uri": data_lake_uri,
        "bucket": store.bucket,
        "prefix": store.prefix,
        "controls": controls,
        "unavailable": unavailable,
        "failed_controls": failed,
        "manual_controls": manual,
        "object_storage_writes": False,
        "deletes": False,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-lake-uri",
        default=os.environ.get("SOLO_EMPIRE_DATA_LAKE_URI")
        or os.environ.get("DATA_LAKE_URI")
        or "file://./data/lake",
    )
    parser.add_argument(
        "--catalog-uri",
        default=os.environ.get("ICEBERG_CATALOG_URI"),
    )
    parser.add_argument(
        "--require-cloud-write",
        action="store_true",
        help="Fail unless explicit cloud-write opt-in is present",
    )
    parser.add_argument(
        "--check-storage-controls",
        action="store_true",
        help="Perform read-only S3 bucket-control checks",
    )
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    report = validate_config(
        data_lake_uri=args.data_lake_uri,
        catalog_uri=args.catalog_uri,
        require_cloud_write=args.require_cloud_write,
    )
    if args.check_storage_controls:
        if report["status"] != "ok":
            report["storage_controls"] = {
                "status": "blocked",
                "reason": "cloud_preflight_failed",
                "object_storage_writes": False,
                "deletes": False,
            }
        else:
            report["storage_controls"] = check_storage_controls(args.data_lake_uri)
            if report["storage_controls"]["status"] != "ok":
                report["status"] = "error"
                report["errors"].append("required storage controls are not verified")
    if args.as_json:
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    else:
        print(f"Cloud lake preflight: {report['status']}")
        print(f"  mode: {report['mode']}")
        print(f"  max object bytes: {report['max_object_bytes']}")
        for warning in report["warnings"]:
            print(f"  warning: {warning}")
        for error in report["errors"]:
            print(f"  error: {error}", file=sys.stderr)
    return 0 if report["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
