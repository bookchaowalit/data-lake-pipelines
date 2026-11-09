#!/usr/bin/env python3
"""Tests for the revenue page (docs/specs/sales-marts.md item 11): Bronze → runner → Gold → snapshot → HTML."""

from __future__ import annotations

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import sales_marts_test  # noqa: E402
from sales_marts_test import NOW, _envelope, _land, _pay  # noqa: E402

from data_lake.storage import ObjectStore  # noqa: E402

HOSTILE = '<script>alert(1)</script>"&'
PAGES = ["index.html", "sales/index.html", "404.html"]


class RevenuePageTests(unittest.TestCase):
    setUp = sales_marts_test.SalesMartsTests.setUp

    def _site(self, lake, out):
        from data_lake.sales_bi_site import build_site
        from data_lake.sales_bi_snapshot import build_bi_snapshot
        from data_lake.sales_marts import run_sales_marts

        result = run_sales_marts(data_lake_uri=lake, now=NOW)
        db = Path(out) / "sales.duckdb"
        build_bi_snapshot(result["manifest_key"], db, data_lake_uri=lake)
        build_site(db, Path(out) / "build")
        return {p: (Path(out) / "build" / p).read_text(encoding="utf-8") for p in PAGES}

    def test_money_is_per_currency_in_major_units_and_ids_are_short(self):
        with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
            _land(lake, "2026-11-09T14:05:00Z", [
                _pay("intent-aaaaaaaa-1111", 2000, "2026-11-09T14:08:00Z", ws="workspace-bbbbbbbb-2222"),
                _pay("i2", 123_456, "2026-11-09T02:00:00Z", ws="ws-c", currency="usd"),
                _pay("i3", 500, "2026-11-09T03:00:00Z", ws="ws-d", currency="jpy"),
            ])
            pages = self._site(lake, out)
            overview = pages["index.html"]
            self.assertIn("20.00 THB", overview)
            self.assertIn("1,234.56 USD", overview)
            self.assertIn("500 JPY", overview)  # zero-decimal currency
            self.assertNotIn("1,234.56 THB", overview)
            recent = pages["sales/index.html"]
            self.assertIn("<code>intent-a</code>", recent)
            self.assertNotIn("intent-aaaaaaaa-1111", recent)
            self.assertNotIn("workspace-bbbbbbbb-2222", recent)

    def test_no_script_strict_csp_and_escaped_text(self):
        with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
            hostile = _envelope("payment.intent.succeeded:h1", "payment.intent.succeeded", "2026-11-09T14:08:00Z",
                                workspace_id="ws-h", intent_id="h1", amount=100, currency="thb", provider=HOSTILE)
            _land(lake, "2026-11-09T14:05:00Z", [hostile])
            pages = self._site(lake, out)
            for html in pages.values():
                self.assertNotIn("<script", html)
            self.assertIn("&lt;script&gt;", pages["sales/index.html"])
            headers = (Path(out) / "build" / "_headers").read_text(encoding="utf-8")
            self.assertIn("script-src 'none'", headers)
            self.assertFalse((Path(out) / "build" / "assets" / "filter.js").exists())

    def test_rejected_events_show_a_warning_and_empty_lake_builds(self):
        with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
            self.assertIn("No sales yet", self._site(lake, out)["index.html"])
        with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
            _land(lake, "2026-11-09T14:05:00Z", [_pay("i1", 2000, "2026-11-09T14:08:00Z"),
                                                  _pay("bad", 0, "2026-11-09T14:09:00Z")])
            self.assertRegex(self._site(lake, out)["index.html"], r"1 event\(s\) were rejected")

    def test_snapshot_rejects_tampered_parts_and_foreign_manifests(self):
        from data_lake.sales_bi_snapshot import BiSnapshotError, build_bi_snapshot
        from data_lake.sales_marts import run_sales_marts

        with tempfile.TemporaryDirectory() as lake, tempfile.TemporaryDirectory() as out:
            _land(lake, "2026-11-09T14:05:00Z", [_pay("i1", 2000, "2026-11-09T14:08:00Z")])
            result = run_sales_marts(data_lake_uri=lake, now=NOW)
            store = ObjectStore(lake)
            manifest = json.loads(store.get_bytes(result["manifest_key"]))
            manifest["marts"][0]["sha256"] = "0" * 64
            bad = re.sub(r"gold_run_id=.*\.json$", "gold_run_id=gold-bad.json", result["manifest_key"])
            store.put_bytes(bad, json.dumps(manifest).encode("utf-8"))
            target = Path(out) / "sales.duckdb"
            with self.assertRaises(BiSnapshotError):
                build_bi_snapshot(bad, target, data_lake_uri=lake)
            self.assertFalse(target.exists())
            with self.assertRaises(BiSnapshotError):
                build_bi_snapshot("control/manifests/gold/source=book-news-scraping/domain=news/x.json", target,
                                  data_lake_uri=lake)



class MoneyFormatTests(unittest.TestCase):
    def test_minor_units_follow_the_iso_4217_exponent(self):
        from data_lake.sales_bi_site import _money

        self.assertEqual(_money(2000, "thb"), "20.00 THB")
        self.assertEqual(_money(500, "KRW"), "500 KRW")
        self.assertEqual(_money(1234, "kwd"), "1.234 KWD")
        self.assertEqual(_money(-1234, "BHD"), "-1.234 BHD")
        self.assertEqual(_money(5, "XYZ"), "0.05 XYZ")  # unknown code: the ISO default of 2

if __name__ == "__main__":
    unittest.main()
