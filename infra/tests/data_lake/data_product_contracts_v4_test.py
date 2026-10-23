#!/usr/bin/env python3
"""The v4 freeze retires flights (flight.v1 / :8105, book-flight-data).

Owner approved the retirement on 2026-10-23 so
bookchaowalit/travel-flight-price-data can be archived. v3 had already retired
ai_tools (ai_tools.v1 / :8107). Nothing in this repository may still launch,
ingest, schedule or require either retired product, the retired ports are never
reused, and the contract guard must check v4.
"""

from __future__ import annotations

import io
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from unittest.mock import patch

import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(ROOT / "infra" / "scripts" / "utils"))

import data_product_contract_guard as guard  # noqa: E402
from data_lake import run_data_product_stack as stack  # noqa: E402
from data_lake.ingest_scraping_captures import CAPTURES, main as replay_main  # noqa: E402

V4_IDS = ["crypto", "stocks", "fx", "defi", "seo"]
V4_PORTS = [8101, 8102, 8103, 8104, 8106]
RETIRED = {
    "flights": {"port": 8105, "repos": ("travel-flight-price-data", "book_flight", "book-flight-data")},
    "ai_tools": {"port": 8107, "repos": ("tech-ai-tools-data", "book_ai_tools", "book-ai-tools-data")},
}
RETIRED_ROWS = {
    "flights": (
        "  - id: flights\n    port: 8105\n    schema_version: flight.v1\n"
        "    repository: book-flight-data\n    package: book_flight\n    role: domain_source\n"
    ),
    "ai_tools": (
        "  - id: ai_tools\n    port: 8107\n    schema_version: ai_tools.v1\n"
        "    repository: book-ai-tools-data\n    package: book_ai_tools\n    role: domain_source\n"
    ),
}


class DataProductContractsV4Tests(unittest.TestCase):
    def test_guard_checks_the_v4_freeze(self) -> None:
        self.assertEqual(guard.FREEZE_PATH.name, "data-product-contracts-v4.yaml")
        freeze = guard.parse_freeze_yaml()
        self.assertEqual(freeze["freeze_id"], "data-product-contracts-v4")
        self.assertEqual([p["id"] for p in freeze["products"]], V4_IDS)
        errors: list[str] = []
        guard.check_freeze_file(errors, freeze)
        self.assertEqual(errors, [])

    def test_guard_rejects_a_freeze_that_re_adds_a_retired_row(self) -> None:
        original = guard.FREEZE_PATH.read_text(encoding="utf-8")
        for product, row in RETIRED_ROWS.items():
            with self.subTest(product=product):
                text = original.replace("\nconsumers:\n", "\n" + row + "consumers:\n", 1)
                with tempfile.TemporaryDirectory() as temp:
                    path = Path(temp) / "data-product-contracts-v4.yaml"
                    path.write_text(text, encoding="utf-8")
                    with patch.object(guard, "FREEZE_PATH", path):
                        freeze = guard.parse_freeze_yaml()
                        errors: list[str] = []
                        guard.check_freeze_file(errors, freeze)
                port = str(RETIRED[product]["port"])
                self.assertTrue(any(port in error for error in errors), errors)

    def test_guard_mirror_and_consumer_ports_drop_8105_and_8107(self) -> None:
        self.assertEqual([p["id"] for p in guard.EXPECTED_PRODUCTS], V4_IDS)
        self.assertEqual(tuple(sorted(guard.RETIRED_PORTS)), (8105, 8107))
        for spec in guard.CONSUMER_SPECS:
            with self.subTest(consumer=spec["id"]):
                self.assertEqual(spec["ports"], V4_PORTS)

    def test_v4_freeze_records_the_retired_rows(self) -> None:
        freeze = yaml.safe_load(guard.FREEZE_PATH.read_text(encoding="utf-8"))
        self.assertEqual(freeze["status"], "frozen")
        self.assertEqual(freeze["supersedes"], "data-product-contracts-v3")
        self.assertNotIn("flights", [p["id"] for p in freeze["products"]])
        self.assertEqual(freeze["retired_rows"]["products"], ["ai_tools", "flights"])
        self.assertEqual(freeze["retired_rows"]["ports"], [8105, 8107])
        self.assertEqual(freeze["retired_rows"]["never_reused_ports"], [8105, 8107])
        for older in ("v2", "v3"):
            with self.subTest(freeze=older):
                data = yaml.safe_load(
                    (ROOT / f"systems/architecture/data-product-contracts-{older}.yaml").read_text(encoding="utf-8")
                )
                self.assertEqual(data["status"], "superseded")

    def test_stack_no_longer_launches_retired_products(self) -> None:
        for product, info in RETIRED.items():
            with self.subTest(product=product):
                self.assertNotIn(product, [p.product for p in stack.PRODUCTS])
                self.assertNotIn(info["port"], [p.port for p in stack.PRODUCTS])
                with self.assertRaises(ValueError):
                    stack.select_products([product])

    def test_capture_replay_no_longer_ingests_retired_products(self) -> None:
        for product, info in RETIRED.items():
            with self.subTest(product=product):
                self.assertNotIn(product, {spec.product for spec in CAPTURES})
                for spec in CAPTURES:
                    self.assertNotIn(info["repos"][0], spec.path)
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    replay_main(["--product", product, "--dry-run"])

    def test_coverage_and_schedule_fixture_drop_the_retired_requirements(self) -> None:
        coverage_path = ROOT / "systems/ai-data/scraping-data-coverage.yaml"
        coverage = yaml.safe_load(coverage_path.read_text(encoding="utf-8"))
        retired = [p for p in coverage["products"] if p["id"] in {"ai-tools", "flights"}]
        self.assertEqual({p["id"] for p in retired}, {"ai-tools", "flights"})
        for product in retired:
            with self.subTest(product=product["id"]):
                # Kept only to map the pinned snapshot's job; never required or scheduled.
                self.assertFalse(product["required"])
                self.assertEqual(product["permission"]["status"], "retired")
                self.assertEqual(product["cron_tags"], [])
                self.assertFalse(product["lake"]["required"])
                self.assertNotIn("api_port", product["delivery"])
        for product in coverage["products"]:
            contract_path = (product.get("delivery") or {}).get("contract_path") or ""
            self.assertNotIn("data-product-contracts-v2", contract_path, product["id"])
            if product["id"] != "ai-tools":
                self.assertNotIn("data-product-contracts-v3", contract_path, product["id"])
        crontab = (ROOT / "infra/tests/fixtures/data_lake/scraping-host-crontab.example").read_text(encoding="utf-8")
        self.assertNotIn("book-ai-tools-data-feeds", crontab)
        self.assertNotIn("book-flight-data-feeds", crontab)

    def test_draft_scraping_contract_is_marked_retired(self) -> None:
        contract = yaml.safe_load(
            (ROOT / "systems/architecture/scraping-contracts/ai-tools.v1.yaml").read_text(encoding="utf-8")
        )
        self.assertEqual(contract["status"], "retired")
        self.assertEqual(contract["retired_by"], "data-product-contracts-v3")

    def test_no_runtime_script_references_a_retired_producer(self) -> None:
        guard_path = ROOT / "infra/scripts/utils/data_product_contract_guard.py"
        for path in sorted((ROOT / "infra" / "scripts").rglob("*.py")):
            if path == guard_path:
                continue  # the guard names the retired rows to reject them
            text = path.read_text(encoding="utf-8")
            rel = path.relative_to(ROOT).as_posix()
            for info in RETIRED.values():
                for needle in info["repos"]:
                    with self.subTest(path=rel, needle=needle):
                        self.assertFalse(needle in text, f"{rel} still references {needle}")


if __name__ == "__main__":
    unittest.main()
