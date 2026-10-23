#!/usr/bin/env python3
"""The v3 freeze retires ai_tools (ai_tools.v1 / :8107, book-ai-tools-data).

Owner approved the retirement on 2026-10-23 so bookchaowalit/tech-ai-tools-data
can be archived. Nothing in this repository may still launch, ingest, schedule
or require the retired product, and the contract guard must check v3.
"""

from __future__ import annotations

import io
import sys
import unittest
from contextlib import redirect_stderr
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(ROOT / "infra" / "scripts" / "utils"))

import data_product_contract_guard as guard  # noqa: E402
from data_lake import run_data_product_stack as stack  # noqa: E402
from data_lake.ingest_scraping_captures import CAPTURES, main as replay_main  # noqa: E402

V3_IDS = ["crypto", "stocks", "fx", "defi", "flights", "seo"]
V3_PORTS = [8101, 8102, 8103, 8104, 8105, 8106]


class DataProductContractsV3Tests(unittest.TestCase):
    def test_guard_checks_the_v3_freeze(self) -> None:
        self.assertEqual(guard.FREEZE_PATH.name, "data-product-contracts-v3.yaml")
        freeze = guard.parse_freeze_yaml()
        self.assertEqual(freeze["freeze_id"], "data-product-contracts-v3")
        self.assertEqual([p["id"] for p in freeze["products"]], V3_IDS)
        errors: list[str] = []
        guard.check_freeze_file(errors, freeze)
        self.assertEqual(errors, [])

    def test_guard_mirror_and_consumer_ports_drop_8107(self) -> None:
        self.assertEqual([p["id"] for p in guard.EXPECTED_PRODUCTS], V3_IDS)
        for spec in guard.CONSUMER_SPECS:
            with self.subTest(consumer=spec["id"]):
                self.assertEqual(spec["ports"], V3_PORTS)

    def test_v3_freeze_keeps_flights_and_records_the_retired_row(self) -> None:
        freeze = yaml.safe_load(guard.FREEZE_PATH.read_text(encoding="utf-8"))
        self.assertEqual(freeze["status"], "frozen")
        self.assertIn("flights", [p["id"] for p in freeze["products"]])
        self.assertEqual(freeze["retired_rows"]["products"], ["ai_tools"])
        self.assertEqual(freeze["retired_rows"]["ports"], [8107])
        v2 = yaml.safe_load(
            (ROOT / "systems/architecture/data-product-contracts-v2.yaml").read_text(encoding="utf-8")
        )
        self.assertEqual(v2["status"], "superseded")

    def test_stack_no_longer_launches_ai_tools(self) -> None:
        self.assertNotIn("ai_tools", [p.product for p in stack.PRODUCTS])
        self.assertNotIn(8107, [p.port for p in stack.PRODUCTS])
        with self.assertRaises(ValueError):
            stack.select_products(["ai_tools"])

    def test_capture_replay_no_longer_ingests_ai_tools(self) -> None:
        self.assertNotIn("ai_tools", {spec.product for spec in CAPTURES})
        for spec in CAPTURES:
            self.assertNotIn("tech-ai-tools-data", spec.path)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            replay_main(["--product", "ai_tools", "--dry-run"])

    def test_coverage_and_schedule_fixture_drop_the_retired_requirement(self) -> None:
        coverage_path = ROOT / "systems/ai-data/scraping-data-coverage.yaml"
        coverage = yaml.safe_load(coverage_path.read_text(encoding="utf-8"))
        retired = [p for p in coverage["products"] if p["id"] == "ai-tools"]
        for product in retired:
            # Kept only to map the pinned snapshot's ai_tools job; never required or scheduled.
            self.assertFalse(product["required"])
            self.assertEqual(product["permission"]["status"], "retired")
            self.assertEqual(product["cron_tags"], [])
            self.assertFalse(product["lake"]["required"])
            self.assertNotIn("api_port", product["delivery"])
        for product in coverage["products"]:
            contract_path = (product.get("delivery") or {}).get("contract_path") or ""
            self.assertNotIn("data-product-contracts-v2", contract_path, product["id"])
        crontab = ROOT / "infra/tests/fixtures/data_lake/scraping-host-crontab.example"
        self.assertNotIn("book-ai-tools-data-feeds", crontab.read_text(encoding="utf-8"))

    def test_draft_scraping_contract_is_marked_retired(self) -> None:
        contract = yaml.safe_load(
            (ROOT / "systems/architecture/scraping-contracts/ai-tools.v1.yaml").read_text(encoding="utf-8")
        )
        self.assertEqual(contract["status"], "retired")
        self.assertEqual(contract["retired_by"], "data-product-contracts-v3")

    def test_no_runtime_script_references_the_retired_producer(self) -> None:
        for path in sorted((ROOT / "infra" / "scripts").rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            for needle in ("tech-ai-tools-data", "book_ai_tools", "book-ai-tools-data"):
                rel = path.relative_to(ROOT).as_posix()
                with self.subTest(path=rel, needle=needle):
                    self.assertFalse(needle in text, f"{rel} still references {needle}")


if __name__ == "__main__":
    unittest.main()
