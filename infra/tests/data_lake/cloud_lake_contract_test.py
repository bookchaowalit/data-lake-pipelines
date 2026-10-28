#!/usr/bin/env python3
"""Offline contract tests for the hosted cloud-lake target."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.cloud_preflight import check_storage_controls, validate_config  # noqa: E402
from data_lake.iceberg import _catalog_properties  # noqa: E402
from data_lake.storage import ObjectStore, StorageConflict, StorageError  # noqa: E402


class CloudLakeContractTests(unittest.TestCase):
    def test_local_preflight_is_network_free(self):
        report = validate_config(data_lake_uri="file:///tmp/solo-empire-lake")
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["mode"], "local")

    def test_cloud_preflight_requires_explicit_endpoint_and_write_opt_in(self):
        with patch.dict(
            os.environ,
            {
                "DATA_LAKE_S3_ENDPOINT": "https://account.r2.cloudflarestorage.com",
                "DATA_LAKE_CLOUD_WRITE_ENABLED": "false",
            },
            clear=False,
        ):
            report = validate_config(
                data_lake_uri="s3://solo-empire-portfolio/lake",
                require_cloud_write=True,
            )
        self.assertEqual(report["status"], "error")
        self.assertIn("DATA_LAKE_CLOUD_WRITE_ENABLED", " ".join(report["errors"]))

    def test_cloud_rest_catalog_properties_use_scoped_token(self):
        with patch.dict(
            os.environ,
            {"ICEBERG_CATALOG_TOKEN": "test-token"},
            clear=False,
        ):
            properties = _catalog_properties(
                "https://catalog.example.com/v1",
                "portfolio-warehouse",
            )
        self.assertEqual(properties["type"], "rest")
        self.assertEqual(properties["warehouse"], "portfolio-warehouse")
        self.assertEqual(properties["token"], "test-token")

    def test_rest_catalog_requires_token(self):
        with patch.dict(
            os.environ,
            {"ICEBERG_CATALOG_TOKEN": "", "R2_DATA_CATALOG_TOKEN": ""},
            clear=False,
        ):
            with self.assertRaises(StorageError):
                _catalog_properties("https://catalog.example.com/v1", "warehouse")

    def test_cloud_write_requires_explicit_opt_in_without_network_call(self):
        with patch.dict(
            os.environ,
            {
                "DATA_LAKE_S3_ENDPOINT": "https://account.r2.cloudflarestorage.com",
                "DATA_LAKE_CLOUD_WRITE_ENABLED": "false",
            },
            clear=False,
        ):
            with self.assertRaises(StorageError):
                ObjectStore("s3://solo-empire-portfolio/lake").put_bytes(
                    "landing/demo.json", b"{}"
                )

    def test_existing_cloud_object_must_match_sha256_metadata(self):
        store = ObjectStore("s3://solo-empire-portfolio/lake")
        with patch.dict(
            os.environ,
            {
                "DATA_LAKE_S3_ENDPOINT": "http://minio:9000",
                "DATA_LAKE_CLOUD_WRITE_ENABLED": "true",
            },
            clear=False,
        ), patch.object(
            store,
            "_run_aws",
            return_value=subprocess.CompletedProcess(
                ["aws"],
                0,
                b'{"Metadata":{"sha256":"' + b"0" * 64 + b'"}}',
                b"",
            ),
        ):
            with self.assertRaises(StorageConflict):
                store.put_bytes("landing/demo.json", b"new-content")

    def test_cloud_object_budget_rejects_large_object_without_network_call(self):
        with patch.dict(
            os.environ,
            {
                "DATA_LAKE_S3_ENDPOINT": "https://account.r2.cloudflarestorage.com",
                "DATA_LAKE_CLOUD_WRITE_ENABLED": "true",
                "DATA_LAKE_CLOUD_MAX_OBJECT_BYTES": "3",
            },
            clear=False,
        ):
            with self.assertRaises(StorageError):
                ObjectStore("s3://solo-empire-portfolio/lake").put_bytes(
                    "landing/demo.json", b"toolarge"
                )

    def test_storage_controls_report_all_required_controls(self):
        responses = [
            {"Status": "Enabled"},
            {
                "ServerSideEncryptionConfiguration": {
                    "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
                }
            },
            {"Rules": [{"ID": "expire-operational", "Status": "Enabled"}]},
            {
                "PublicAccessBlockConfiguration": {
                    "BlockPublicAcls": True,
                    "IgnorePublicAcls": True,
                    "BlockPublicPolicy": True,
                    "RestrictPublicBuckets": True,
                }
            },
        ]
        completed = [
            subprocess.CompletedProcess(["aws"], 0, json.dumps(payload).encode(), b"")
            for payload in responses
        ]
        with patch.dict(
            os.environ,
            {"DATA_LAKE_S3_ENDPOINT": "https://r2.example.test", "AWS_REGION": "auto"},
            clear=False,
        ), patch.object(ObjectStore, "_run_aws", side_effect=completed):
            report = check_storage_controls("s3://bucket/lake")
        self.assertEqual(report["status"], "ok")
        self.assertEqual(
            {name: value["status"] for name, value in report["controls"].items()},
            {
                "versioning": "pass",
                "encryption": "pass",
                "lifecycle": "pass",
                "public_access_block": "pass",
            },
        )
        self.assertFalse(report["object_storage_writes"])
        self.assertFalse(report["deletes"])
        self.assertNotIn("payload", report)

    def test_storage_controls_fail_closed_on_disabled_or_unavailable_control(self):
        responses = [
            subprocess.CompletedProcess(["aws"], 0, b'{"Status":"Suspended"}', b""),
            subprocess.CompletedProcess(["aws"], 1, b"", b"AccessDenied"),
            subprocess.CompletedProcess(["aws"], 0, b'{"Rules":[]}', b""),
            subprocess.CompletedProcess(["aws"], 0, b'{"PublicAccessBlockConfiguration":{}}', b""),
        ]
        with patch.dict(
            os.environ,
            {"DATA_LAKE_S3_ENDPOINT": "https://r2.example.test", "AWS_REGION": "auto"},
            clear=False,
        ), patch.object(ObjectStore, "_run_aws", side_effect=responses):
            report = check_storage_controls("s3://bucket/lake")
        self.assertEqual(report["status"], "attention")
        self.assertIn("get-bucket-encryption", report["unavailable"])
        self.assertIn("versioning", report["failed_controls"])
        self.assertFalse(report["object_storage_writes"])
        self.assertFalse(report["deletes"])

    def test_r2_storage_controls_use_provider_capabilities(self):
        lifecycle = subprocess.CompletedProcess(
            ["aws"],
            0,
            b'{"Rules":[{"ID":"expire-operational","Status":"Enabled"}]}',
            b"",
        )
        with patch.dict(
            os.environ,
            {
                "DATA_LAKE_S3_ENDPOINT": "https://account.r2.cloudflarestorage.com",
                "AWS_REGION": "auto",
                "DATA_LAKE_STORAGE_PROVIDER": "",
            },
            clear=False,
        ), patch.object(ObjectStore, "_run_aws", return_value=lifecycle) as run_aws:
            report = check_storage_controls("s3://bucket/lake")

        self.assertEqual(report["provider"], "r2")
        self.assertEqual(report["status"], "attention")
        self.assertEqual(report["controls"]["versioning"]["status"], "not_applicable")
        self.assertEqual(report["controls"]["encryption"]["status"], "pass")
        self.assertEqual(report["controls"]["lifecycle"]["status"], "pass")
        self.assertEqual(report["controls"]["public_access_block"]["status"], "manual")
        self.assertNotIn("versioning", report["failed_controls"])
        self.assertIn("public_access_block", report["manual_controls"])
        self.assertEqual(run_aws.call_count, 1)
        self.assertIn("get-bucket-lifecycle-configuration", run_aws.call_args.args[0])

    def test_unknown_storage_provider_is_blocked_without_provider_call(self):
        with patch.dict(
            os.environ,
            {
                "DATA_LAKE_S3_ENDPOINT": "https://storage.example.test",
                "DATA_LAKE_STORAGE_PROVIDER": "unsupported-provider",
            },
            clear=False,
        ), patch.object(ObjectStore, "_run_aws") as run_aws:
            report = check_storage_controls("s3://bucket/lake")

        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["reason"], "unsupported_DATA_LAKE_STORAGE_PROVIDER")
        run_aws.assert_not_called()

    def test_get_prefix_reads_a_prefix_with_one_recursive_copy(self):
        def copy(args, **_kwargs):
            destination = Path(args[3])
            (destination / "nested").mkdir()
            (destination / "a.json").write_bytes(b"a")
            (destination / "nested" / "b.json").write_bytes(b"b")
            return subprocess.CompletedProcess(["aws"], 0, b"", b"")

        store = ObjectStore("s3://bucket/lake")
        with patch.object(store, "_run_aws", side_effect=copy) as run_aws:
            bodies = store.get_prefix("control/manifests/source=news")
        self.assertEqual(
            bodies,
            {"control/manifests/source=news/a.json": b"a", "control/manifests/source=news/nested/b.json": b"b"},
        )
        run_aws.assert_called_once()
        args = run_aws.call_args.args[0]
        self.assertEqual(args[:3], ["s3", "cp", "s3://bucket/lake/control/manifests/source=news/"])
        self.assertIn("--recursive", args)

    def test_get_prefix_failure_raises_storage_error(self):
        store = ObjectStore("s3://bucket/lake")
        failed = subprocess.CompletedProcess(["aws"], 1, b"", b"AccessDenied")
        with patch.object(store, "_run_aws", return_value=failed), self.assertRaises(StorageError):
            store.get_prefix("control/manifests/source=news")

    def test_get_prefix_on_a_file_lake_reads_below_the_prefix_only(self):
        import tempfile

        with tempfile.TemporaryDirectory() as root:
            (Path(root) / "control" / "m").mkdir(parents=True)
            (Path(root) / "control" / "m" / "x.json").write_bytes(b"x")
            (Path(root) / "control" / "other.json").write_bytes(b"o")
            store = ObjectStore(Path(root).as_uri())
            self.assertEqual(store.get_prefix("control/m"), {"control/m/x.json": b"x"})
            self.assertEqual(store.get_prefix("control/missing"), {})


if __name__ == "__main__":
    unittest.main()
