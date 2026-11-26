#!/usr/bin/env python3
"""Offline tests for the SPEC-002 catalog export (platform-empire rollout step 5)."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import catalog_lake_export as catalog  # noqa: E402
from data_lake import scheduled_registry_export as job  # noqa: E402
from data_lake.registry_lake_export import DATASETS  # noqa: E402

BASE = "https://tasks.example"
ENV = {"EMPIRE_TASKS_API": BASE + "/", "CATALOG_EXPORT_TOKEN": "tok"}


def concept(slug: str) -> dict:
    return {
        "urn": f"empire:platform:domain:ws:{slug}", "kind": "domain", "slug": slug, "label": slug.title(),
        "description": "", "parent": None, "status": "active", "replaced_by": [], "introduced_in": 1,
        "retired_in": None, "was": [], "updated_at": "2026-11-24T00:00:00Z",
    }


def metric(key: str, classification: str) -> dict:
    return {
        "urn": f"empire:platform:metric:ws:{key}", "key": key, "label": key, "description": "d",
        "domain": "health", "domain_urn": "empire:platform:domain:ws:health", "unit": "h", "scale": "ratio",
        "value_range": {"min": 0}, "kind": "lagging", "objectivity": "objective",
        "source": {"cell": "health", "resource": "health-metrics"}, "aggregation": "mean", "cadence": "daily",
        "criteria": [], "target": None, "classification": classification,
        "read_permission": "health.health-metric:read", "status": "active", "replaced_by": None, "version": 1,
        "updated_at": "2026-11-24T00:00:00Z",
    }


BODIES = {
    f"{BASE}/v1/catalog/concepts": {"items": [concept("work"), concept("health")]},
    f"{BASE}/v1/catalog/metrics": {"items": [
        metric("health.sleep_hours", "internal"),
        metric("time.work_minutes", "public"),
        metric("diary.mood", "confidential"),
        metric("health.weight", "restricted"),
    ]},
}


def fake_get(seen: list | None = None):
    def get(url: str, token: str):
        if seen is not None:
            seen.append((url, token))
        return BODIES[url]
    return get


def has_pyarrow() -> bool:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return True


class CatalogExportTests(unittest.TestCase):
    def test_reads_both_lists_with_the_token(self):
        seen: list = []
        records = catalog.load_records(BASE + "/", "tok", fake_get(seen))
        self.assertEqual(sorted(seen), [(f"{BASE}/v1/catalog/concepts", "tok"), (f"{BASE}/v1/catalog/metrics", "tok")])
        self.assertEqual([row["slug"] for row in records["catalog_concepts"]], ["health", "work"])
        self.assertEqual(records["catalog_concepts"][0]["id"], "empire:platform:domain:ws:health")

    def test_only_public_and_internal_metrics_leave_the_platform(self):  # D7b, AC-34, AC-46
        rows = catalog.load_records(BASE, "tok", fake_get())["catalog_metric_definitions"]
        self.assertEqual([row["key"] for row in rows], ["health.sleep_hours", "time.work_minutes"])
        self.assertTrue(all("read_permission" not in row for row in rows))

    def test_not_configured_is_skipped_half_configured_fails(self):
        self.assertEqual(catalog.export({}, "s3://unused", get=lambda *a: self.fail("called")), {"status": "skipped"})
        with self.assertRaises(catalog.CatalogError):
            catalog.export({"CATALOG_EXPORT_TOKEN": "tok"}, "s3://unused", get=fake_get())

    def test_malformed_body_fails(self):
        with self.assertRaises(catalog.CatalogError):
            catalog.load_records(BASE, "tok", lambda url, token: {"items": "nope"})
        with self.assertRaises(catalog.CatalogError):
            catalog.load_records(BASE, "tok", lambda url, token: {"items": [{"slug": "no-urn"}]})

    def test_http_errors_name_only_the_route(self):
        response = mock.Mock(status_code=403)
        with mock.patch.object(catalog.httpx, "get", return_value=response) as get:
            with self.assertRaisesRegex(catalog.CatalogError, r"^HTTP 403 for concepts$"):
                catalog.http_get(f"{BASE}/v1/catalog/concepts", "tok")
        self.assertEqual(get.call_args.kwargs["headers"]["Authorization"], "Bearer tok")

    def test_exports_twice_writes_once(self):
        if not has_pyarrow():
            self.skipTest("pyarrow not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            lake = (Path(temp_dir) / "lake").as_uri()
            first = catalog.export(ENV, lake, get=fake_get())
            second = catalog.export(ENV, lake, get=fake_get())
        self.assertEqual(first["status"], "exported")
        self.assertEqual({k: v["records"] for k, v in first["datasets"].items()},
                         {"catalog_concepts": 2, "catalog_metric_definitions": 2})
        self.assertTrue(all(v["new"] for v in first["datasets"].values()))
        self.assertFalse(any(v["new"] for v in second["datasets"].values()))

    def test_no_exportable_metric_writes_no_empty_batch(self):
        if not has_pyarrow():
            self.skipTest("pyarrow not installed")
        bodies = {**BODIES, f"{BASE}/v1/catalog/metrics": {"items": [metric("diary.mood", "confidential")]}}
        with tempfile.TemporaryDirectory() as temp_dir:
            result = catalog.export(ENV, (Path(temp_dir) / "lake").as_uri(), get=lambda url, token: bodies[url])
        self.assertEqual(result["datasets"]["catalog_metric_definitions"], {"records": 0, "new": False})


class ScheduledJobCatalogTests(unittest.TestCase):
    def run_job(self, env, catalog_get, argv=None):
        from scheduled_registry_export_test import STORE_ENV, fixture_fetch, fixture_store

        self.beats: list[bool] = []
        return job.run(argv or [], env={**STORE_ENV, **env}, fetch=fixture_fetch, heartbeat=self.beats.append,
                       publish=lambda *a: "written", catalog_get=catalog_get, fetch_store=fixture_store)

    def test_cron_exports_registries_and_catalog(self):
        if not has_pyarrow():
            self.skipTest("pyarrow not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            lake = Path(temp_dir) / "lake"
            code = self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": lake.as_uri(), **ENV}, fake_get())
            manifests = list((lake / "control" / "manifests").rglob("*.json"))
        self.assertEqual(code, 0)
        self.assertEqual(self.beats, [True])
        self.assertEqual(len(manifests), len(DATASETS) + len(catalog.DATASETS))

    def test_catalog_failure_fails_the_run(self):
        def broken(url, token):
            raise catalog.CatalogError("HTTP 401 for concepts")

        code = self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://x", **ENV}, broken, ["--dry-run"])
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
