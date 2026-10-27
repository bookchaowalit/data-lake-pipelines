#!/usr/bin/env python3
"""Offline tests for the generated system landscape page."""

from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import system_landscape as landscape  # noqa: E402
from data_lake.registry_lake_export import load_records  # noqa: E402

FIXTURES = ROOT / "infra" / "tests" / "fixtures" / "registry_lake_export"
ARGS = ["--registries", str(FIXTURES / "registries"), "--catalog", str(FIXTURES / "metadata-catalog")]


class BuildTests(unittest.TestCase):
    def setUp(self):
        self.records = load_records(FIXTURES / "registries", FIXTURES / "metadata-catalog")

    def test_system_rows_follow_flows_and_lineage(self):
        systems = {s["id"]: s for s in landscape.build(self.records)["systems"]}
        jobs = systems["book-job-data"]
        self.assertEqual(jobs["domains"], ["market-intelligence", "data-knowledge"])
        self.assertEqual(jobs["primary_domain"], "market-intelligence")
        self.assertEqual((jobs["inbound_flows"], jobs["automated_flows"]), (1, 1))
        # data-product.jobs plus consumer.portfolio-frontend downstream
        self.assertEqual((jobs["catalog_assets"], jobs["consumers"]), (2, 1))
        self.assertEqual(systems["data-lake"]["inbound_flows"], 0)

    def test_coverage_labels(self):
        coverage = {c["id"]: c for c in landscape.build(self.records)["coverage"]}
        self.assertEqual(coverage["market-intelligence"]["platform_coverage"], "served")
        self.assertEqual(coverage["offer-product"]["platform_coverage"], "missing")
        self.assertEqual(coverage["data-knowledge"]["system_ids"], ["book-job-data", "data-lake"])

    def test_partial_when_running_without_automated_flows(self):
        records = copy.deepcopy(self.records)
        records["system_capabilities"] = [r for r in records["system_capabilities"] if r["system_id"] == "data-lake"]
        coverage = {c["id"]: c for c in landscape.build(records)["coverage"]}
        self.assertEqual(coverage["data-knowledge"]["platform_coverage"], "partial")

    def test_unmapped_system_is_reported(self):
        records = copy.deepcopy(self.records)
        records["system_capabilities"] = [r for r in records["system_capabilities"] if r["system_id"] != "data-lake"]
        text = landscape.render(landscape.build(records))
        self.assertIn("Systems without a capability domain: `data-lake`.", text)
        self.assertIn("systems without a capability: 1", text)


class CliTests(unittest.TestCase):
    def test_render_is_deterministic_and_check_detects_drift(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            out = Path(temp_dir) / "landscape.md"
            self.assertEqual(landscape.main([*ARGS, "--output", str(out)]), 0)
            first = out.read_text(encoding="utf-8")
            self.assertIn("| 2 | Offer & Product | 0 (0) | 0 | 0 | 0 | missing | — |", first)
            self.assertEqual(landscape.main([*ARGS, "--output", str(out), "--check"]), 0)
            out.write_text(first.replace("missing", "served"), encoding="utf-8")
            self.assertEqual(landscape.main([*ARGS, "--output", str(out), "--check"]), 1)


if __name__ == "__main__":
    unittest.main()
