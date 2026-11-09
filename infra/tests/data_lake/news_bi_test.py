#!/usr/bin/env python3
"""Tests for the news BI snapshot and static site (docs/specs/news-marts.md item 7)."""

from __future__ import annotations

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.storage import ObjectStore  # noqa: E402

HOSTILE = '<script>alert(1)</script>"&\''
PAGES = ["index.html", "latest/index.html", "404.html"]


def _snapshot(path: Path, *, empty: bool = False) -> None:
    """A snapshot with the tables and columns news_bi_snapshot.py writes."""
    import duckdb

    con = duckdb.connect(str(path))
    try:
        con.execute(
            "create table news_articles_daily (published_date date, publisher varchar, section varchar, "
            "articles bigint, with_image bigint)"
        )
        con.execute(
            "create table news_feed_health (publisher varchar, articles_total bigint, articles_7d bigint, "
            "last_published_at timestamp, last_seen_at timestamp, as_of_date date)"
        )
        con.execute(
            "create table news_latest_articles (publisher varchar, section varchar, published_at timestamp, "
            "headline varchar, canonical_url varchar, rank bigint)"
        )
        con.execute(
            "create table gold_meta as select 'gold-abc' as gold_run_id, 'silver-def' as silver_run_id, "
            "date '2026-11-09' as as_of_date, timestamp '2026-11-09 09:15:00' as max_seen_at"
        )
        if empty:
            return
        con.execute(
            """insert into news_articles_daily values
                (date '2026-11-09', 'Matichon', 'unspecified', 5, 1),
                (date '2026-11-09', 'Matichon', 'politics', 2, 0),
                (date '2026-11-08', 'Blognone', 'technology', 3, 0),
                (date '2026-10-01', 'Blognone', 'technology', 99, 0)"""
        )
        con.execute(
            """insert into news_feed_health values
                ('Matichon', 7, 7, timestamp '2026-11-09 08:00:00', timestamp '2026-11-09 09:15:00', date '2026-11-09'),
                ('Blognone', 102, 3, timestamp '2026-11-08 05:00:00', timestamp '2026-11-09 01:15:00', date '2026-11-09')"""
        )
        con.execute(
            "insert into news_latest_articles values "
            "('Matichon', 'unspecified', timestamp '2026-11-09 08:00:00', 'ข่าวเศรษฐกิจวันนี้', 'https://www.matichon.co.th/a/1', 1),"
            "('Matichon', 'unspecified', timestamp '2026-11-09 07:00:00', ?, 'javascript:alert(1)', 2),"
            "('Blognone', 'technology', timestamp '2026-11-08 05:00:00', 'Gemini news', 'https://www.blognone.com/node/1', 1)",
            [HOSTILE],
        )
    finally:
        con.close()


class NewsBiSiteTests(unittest.TestCase):
    def setUp(self):
        try:
            import duckdb  # noqa: F401
        except ImportError:
            self.skipTest("duckdb not installed")
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        _snapshot(self.root / "news.duckdb")

    def tearDown(self):
        self.tmp.cleanup()

    def _build(self, out="build"):
        from data_lake.news_bi_site import build_site

        build_site(self.root / "news.duckdb", self.root / out)
        return {page: (self.root / out / page).read_text(encoding="utf-8") for page in PAGES}

    def test_every_page_is_written_without_any_script(self):
        pages = self._build()
        self.assertEqual(set(pages), set(PAGES))
        self.assertFalse((self.root / "build" / "assets" / "filter.js").exists())
        for html in pages.values():
            self.assertNotIn("<script", html.replace("&lt;script", ""))
        headers = (self.root / "build" / "_headers").read_text(encoding="utf-8")
        self.assertIn("script-src 'none'", headers)
        self.assertIn("default-src 'none'", headers)

    def test_scraped_text_is_escaped_and_unsafe_links_are_dropped(self):
        latest = self._build()["latest/index.html"]
        self.assertNotIn(HOSTILE, latest)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", latest)
        self.assertNotIn("javascript:", latest)
        self.assertIn('href="https://www.matichon.co.th/a/1" rel="noopener noreferrer" lang="th"', latest)

    def test_pages_load_nothing_from_other_hosts(self):
        for page, html in self._build().items():
            for ref in re.findall(r'(?:src|href)="([^"]+)"', html.split("<main", 1)[0]):
                self.assertTrue(ref.startswith("/") or ref.startswith("#"), (page, ref))

    def test_output_is_byte_for_byte_reproducible(self):
        self.assertEqual(self._build("a"), self._build("b"))

    def test_chart_window_is_14_days_and_feed_status_flags_old_captures(self):
        overview = self._build()["index.html"]
        self.assertIn("2026-10-27 to 2026-11-09", overview)
        self.assertIn("10 articles over 14 days", overview)  # the 2026-10-01 row is outside
        self.assertIn("No capture for 6+ h", overview)  # Blognone: 8 h before the newest capture
        self.assertIn("Capturing", overview)

    def test_empty_marts_still_build(self):
        _snapshot(self.root / "empty.duckdb", empty=True)
        from data_lake.news_bi_site import build_site

        build_site(self.root / "empty.duckdb", self.root / "empty")
        self.assertIn("No articles", (self.root / "empty" / "latest" / "index.html").read_text(encoding="utf-8"))


class NewsBiSnapshotTests(unittest.TestCase):
    def setUp(self):
        try:
            import duckdb  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("duckdb/pyarrow not installed")

    def _publish(self, lake: str) -> str:
        """A news Gold manifest whose parts come from a fixture snapshot."""
        import duckdb

        from data_lake.news_marts import GOLD_MANIFEST_PREFIX, NEWS_MARTS

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "fixture.duckdb"
            _snapshot(db)
            con = duckdb.connect(str(db))
            store = ObjectStore(lake)
            marts = []
            for name in NEWS_MARTS:
                part = Path(tmp) / f"{name}.parquet"
                con.execute(f"copy {name} to '{part}' (format parquet)")
                put = store.put_bytes(f"gold/domain=news/dataset={name}/part-x.parquet", part.read_bytes())
                marts.append({"dataset": name, "key": put.key, "sha256": put.sha256, "rows": 1})
            con.close()
        key = f"{GOLD_MANIFEST_PREFIX}gold_run_id=gold-x.json"
        manifest = {"layer": "gold", "gold_run_id": "gold-x", "input": {"silver_run_id": "silver-y"}, "marts": marts}
        store.put_bytes(key, json.dumps(manifest).encode("utf-8"))
        return key

    def test_snapshot_loads_marts_and_gold_meta(self):
        import duckdb

        from data_lake.news_bi_snapshot import build_bi_snapshot

        with tempfile.TemporaryDirectory() as lake:
            key = self._publish(lake)
            target = Path(lake) / "out" / "news.duckdb"
            result = build_bi_snapshot(key, target, data_lake_uri=lake)
            self.assertEqual(result["gold_run_id"], "gold-x")
            con = duckdb.connect(str(target), read_only=True)
            meta = con.sql("select gold_run_id, cast(as_of_date as varchar) from gold_meta").fetchall()
            con.close()
            self.assertEqual(meta, [("gold-x", "2026-11-09")])

    def test_tampered_part_and_foreign_manifest_are_rejected(self):
        from data_lake.news_bi_snapshot import BiSnapshotError, build_bi_snapshot

        with tempfile.TemporaryDirectory() as lake:
            key = self._publish(lake)
            store = ObjectStore(lake)
            manifest = json.loads(store.get_bytes(key))
            manifest["marts"][0]["sha256"] = "0" * 64
            bad = key.replace("gold-x", "gold-bad")
            store.put_bytes(bad, json.dumps(manifest).encode("utf-8"))
            target = Path(lake) / "out" / "news.duckdb"
            with self.assertRaises(BiSnapshotError):
                build_bi_snapshot(bad, target, data_lake_uri=lake)
            self.assertFalse(target.exists())
            with self.assertRaises(BiSnapshotError):
                build_bi_snapshot("control/manifests/gold/source=book-job-data/domain=jobs/x.json", target,
                                  data_lake_uri=lake)


if __name__ == "__main__":
    unittest.main()
