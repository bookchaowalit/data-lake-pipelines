#!/usr/bin/env python3
"""Tests for the source coverage registry checker, including the live registry file."""

from __future__ import annotations

import copy
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts" / "utils"))

from source_coverage_check import DEFAULT_PATH, check  # noqa: E402


class LiveRegistryTests(unittest.TestCase):
    def test_registry_in_repo_passes(self):
        errors, summary = check(DEFAULT_PATH)
        self.assertEqual(errors, [])
        self.assertEqual(summary["sources"], 20)
        data = yaml.safe_load(DEFAULT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(len(data["sources"]), 20 * len(data["batches"]))


class RuleTests(unittest.TestCase):
    def setUp(self):
        self.data = yaml.safe_load(DEFAULT_PATH.read_text(encoding="utf-8"))
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.path = base / "repository-catalog" / "registries" / "source-coverage.yaml"
        self.path.parent.mkdir(parents=True)
        for meta in self.data["batches"].values():
            report = base / meta["report"]
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text("report", encoding="utf-8")
        for row in self.data["sources"]:
            workflow = (row.get("pipeline") or {}).get("workflow")
            if workflow:
                (base / workflow).parent.mkdir(parents=True, exist_ok=True)
                (base / workflow).write_text("name: x", encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def _errors(self, data):
        self.path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
        return check(self.path)[0]

    def _row(self, data, sid):
        return next(r for r in data["sources"] if r["id"] == sid)

    def test_copy_passes(self):
        self.assertEqual(self._errors(self.data), [])

    def test_scrape_needs_permission(self):
        data = copy.deepcopy(self.data)
        row = self._row(data, "linkedin")
        row["method"] = "scrape"
        self.assertTrue(any("scraping_allowed" in e or "not allowed" in e for e in self._errors(data)))

    def test_ingest_now_needs_pipeline(self):
        data = copy.deepcopy(self.data)
        self._row(data, "sentry")["verdict"] = "ingest-now"
        self.assertTrue(any("needs a pipeline" in e for e in self._errors(data)))

    def test_empty_pipeline_and_covered_without_data(self):
        data = copy.deepcopy(self.data)
        self._row(data, "github")["pipeline"] = {}
        x = self._row(data, "x")
        x["current_status"], x["coverage_pct"] = "missing", 0
        errors = self._errors(data)
        self.assertTrue(any("github: ingest-now needs a pipeline" in e for e in errors))
        self.assertTrue(any("x: verdict covered needs data" in e for e in errors))

    def test_priority_order_needs_reasons(self):
        data = copy.deepcopy(self.data)
        row = self._row(data, "stripe")
        row["method"], row["scraping_allowed"] = "scrape", True
        errors = self._errors(data)
        self.assertTrue(any("stripe: method scrape needs api_gap" in e for e in errors))
        self.assertTrue(any("stripe: method scrape needs mcp_gap" in e for e in errors))

    def test_covered_needs_100_and_batch_must_exist(self):
        data = copy.deepcopy(self.data)
        x = self._row(data, "x")
        x["current_status"] = "covered"
        self._row(data, "reddit")["batch"] = 99
        errors = self._errors(data)
        self.assertTrue(any("x: covered sources have coverage_pct 100" in e for e in errors))
        self.assertTrue(any("reddit: batch 99 is not declared" in e for e in errors))

    def test_missing_field_and_bad_values(self):
        data = copy.deepcopy(self.data)
        row = self._row(data, "stripe")
        del row["auth_type"]
        row["coverage_pct"] = 150
        row["verdict"] = "maybe"
        errors = self._errors(data)
        self.assertTrue(any("missing auth_type" in e for e in errors))
        self.assertTrue(any("coverage_pct" in e for e in errors))
        self.assertTrue(any("verdict 'maybe'" in e for e in errors))

    def test_incomplete_batch_and_secret(self):
        data = copy.deepcopy(self.data)
        data["sources"].pop()
        data["sources"][0]["tos_notes"] = "token ghp_abc"
        errors = self._errors(data)
        self.assertTrue(any("19 rows" in e for e in errors))
        self.assertTrue(any("secret-looking" in e for e in errors))

    def test_next_batch_required(self):
        data = copy.deepcopy(self.data)
        data["next_batch"] = {"number": data["current_batch"] + 1, "sources": ["github"]}
        self.assertTrue(any("repeats sources" in e for e in self._errors(data)))


if __name__ == "__main__":
    unittest.main()
