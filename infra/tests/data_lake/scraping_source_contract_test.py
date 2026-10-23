#!/usr/bin/env python3
"""Offline guard for draft scraping contracts and redacted fixtures."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[3]
CONTRACT_ROOT = ROOT / "systems" / "architecture" / "scraping-contracts"
FIXTURE_ROOT = ROOT / "infra" / "tests" / "fixtures" / "scraping_contracts"


class ScrapingSourceContractTests(unittest.TestCase):
    CONTRACTS = {
        "marketplace": ("marketplace.v1.yaml", "marketplace.json", "marketplace.v1"),
        "restaurant": ("restaurant.v1.yaml", "restaurant.json", "restaurant.v1"),
        "directory": ("directory.v1.yaml", "directory.json", "directory.v1"),
        "property": ("property.v1.yaml", "property.json", "property.v1"),
    }

    def test_draft_contracts_match_redacted_offline_fixtures(self) -> None:
        for name, (contract_name, fixture_name, schema_version) in self.CONTRACTS.items():
            with self.subTest(contract=name):
                contract = yaml.safe_load((CONTRACT_ROOT / contract_name).read_text(encoding="utf-8"))
                fixture = json.loads((FIXTURE_ROOT / fixture_name).read_text(encoding="utf-8"))
                self.assertEqual(contract["status"], "draft")
                self.assertEqual(contract["collection_approval"], "pending_operator_approval")
                self.assertTrue(contract["not_yet_approved_for_collection"])
                self.assertEqual(contract["schema_version"], schema_version)
                self.assertEqual(fixture["schema_version"], schema_version)
                self.assertTrue(set(contract["quality"]["required"]).issubset(fixture))
                self.assertNotIn("password", fixture)
                self.assertNotIn("api_key", fixture)
                self.assertNotIn("token", fixture)


if __name__ == "__main__":
    unittest.main()
