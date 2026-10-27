#!/usr/bin/env python3
"""Offline tests for the registry and metadata-catalog export into the lake."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.registry_lake_export import (  # noqa: E402
    DATASETS,
    export,
    load_records,
)

FIXTURES = ROOT / "infra" / "tests" / "fixtures" / "registry_lake_export"
REGISTRIES = FIXTURES / "registries"
CATALOG = FIXTURES / "metadata-catalog"


class LoadRecordsTests(unittest.TestCase):
    def setUp(self):
        self.records = load_records(REGISTRIES, CATALOG)

    def test_every_dataset_has_records(self):
        self.assertEqual(set(self.records), set(DATASETS))
        for dataset in DATASETS:
            self.assertTrue(self.records[dataset], dataset)

    def test_data_sources_flatten_destination_and_flags(self):
        flows = {row["id"]: row for row in self.records["data_sources"]}
        jobs = flows["job-capture"]
        self.assertEqual(jobs["destination_system"], "book-job-data")
        self.assertEqual(jobs["catalog_assets"], ["data-product.jobs"])
        self.assertEqual(jobs["probe_count"], 1)
        self.assertFalse(jobs["candidate"])
        bank = flows["bank-accounts"]
        self.assertEqual(bank["automation"], "not-captured")
        self.assertIsNone(bank["destination_system"])
        self.assertEqual(bank["catalog_assets"], [])
        self.assertTrue(bank["candidate"])

    def test_systems_keep_host_and_repo(self):
        systems = {row["id"]: row for row in self.records["systems"]}
        self.assertEqual(systems["book-job-data"]["host_provider"], "railway")
        self.assertEqual(systems["book-job-data"]["repo"], "bookchaowalit/book-job-data")

    def test_catalog_and_lineage(self):
        assets = {row["id"] for row in self.records["catalog_assets"]}
        self.assertIn("data-product.jobs", assets)
        edge = self.records["lineage_edges"][0]
        self.assertEqual(
            {key: edge[key] for key in ("from_asset", "to_asset")},
            {"from_asset": "data-product.jobs", "to_asset": "consumer.portfolio-frontend"},
        )

    def test_capability_rows(self):
        domains = [row["id"] for row in self.records["capability_domains"]]
        self.assertEqual(domains, ["market-intelligence", "offer-product", "data-knowledge"])
        caps = {row["id"]: row for row in self.records["system_capabilities"]}
        self.assertTrue(caps["book-job-data:market-intelligence"]["is_primary"])
        self.assertEqual(caps["book-job-data:data-knowledge"]["rank"], 2)
        self.assertFalse(caps["book-job-data:data-knowledge"]["is_primary"])

    def test_value_stream_rows(self):
        streams = {row["id"]: row for row in self.records["value_streams"]}
        self.assertEqual(streams["lead-to-customer"]["stage_count"], 2)
        stages = {row["id"]: row for row in self.records["value_stream_stages"]}
        self.assertEqual(stages["lead-to-customer:2"]["name"], "Send proposal")
        self.assertEqual(stages["lead-to-customer:2"]["systems"], [])

    def test_empty_capability_list_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registries = Path(temp_dir)
            for path in REGISTRIES.iterdir():
                (registries / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
            caps = registries / "business-capabilities.yaml"
            caps.write_text(caps.read_text(encoding="utf-8").replace("data-lake: [data-knowledge]", "data-lake: []"),
                            encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "non-empty list"):
                load_records(registries, CATALOG)

    def test_no_free_text_notes_leave_the_registry(self):
        # notes may hold operator detail; only structure is exported
        for row in self.records["data_sources"]:
            self.assertNotIn("notes", row)

    def test_duplicate_ids_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registries = Path(temp_dir)
            for path in REGISTRIES.iterdir():
                (registries / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
            sources = registries / "data-sources.yaml"
            text = sources.read_text(encoding="utf-8")
            sources.write_text(text + text[text.index("  - id: job-capture"):], encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate id"):
                load_records(registries, CATALOG)


class ExportTests(unittest.TestCase):
    def test_export_writes_bronze_and_is_idempotent(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            lake = Path(temp_dir) / "lake"
            first = export(REGISTRIES, CATALOG, lake.as_uri())
            self.assertEqual(sorted(first["datasets"]), sorted(DATASETS))
            manifests = sorted((lake / "control" / "manifests").rglob("*.json"))
            self.assertEqual(len(manifests), len(DATASETS))
            manifest = json.loads(manifests[0].read_text(encoding="utf-8"))
            self.assertEqual(manifest["domain"], "metadata")
            self.assertTrue((lake / manifest["bronze"]["key"]).is_file())

            second = export(REGISTRIES, CATALOG, lake.as_uri())
            self.assertTrue(all(result["new"] for result in first["datasets"].values()))
            self.assertFalse(any(result["new"] for result in second["datasets"].values()))
            self.assertEqual(
                {name: result["manifest_key"] for name, result in first["datasets"].items()},
                {name: result["manifest_key"] for name, result in second["datasets"].items()},
            )
            self.assertEqual(len(sorted((lake / "control" / "manifests").rglob("*.json"))), len(DATASETS))

    def test_changing_one_file_only_adds_that_dataset(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            registries = Path(temp_dir) / "registries"
            registries.mkdir()
            for path in REGISTRIES.iterdir():
                (registries / path.name).write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
            lake = Path(temp_dir) / "lake"
            export(registries, CATALOG, lake.as_uri())
            sources = registries / "data-sources.yaml"
            sources.write_text(sources.read_text(encoding="utf-8").replace("data: transactions", "data: statements"), encoding="utf-8")
            second = export(registries, CATALOG, lake.as_uri())
            self.assertEqual(
                {name for name, result in second["datasets"].items() if result["new"]},
                {"data_sources"},
            )

    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            lake = Path(temp_dir) / "lake"
            report = export(REGISTRIES, CATALOG, lake.as_uri(), dry_run=True)
            self.assertEqual(report["status"], "dry-run")
            self.assertGreater(report["datasets"]["data_sources"]["records"], 0)
            self.assertFalse(lake.exists())


if __name__ == "__main__":
    unittest.main()
