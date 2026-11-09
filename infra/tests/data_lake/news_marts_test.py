#!/usr/bin/env python3
"""Tests for the news Silver → dbt marts → Gold runner (docs/specs/news-marts.md)."""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.ingest import ingest_payload  # noqa: E402
from data_lake.query import query_parquet  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

# 2026-11-09 10:00 UTC = 17:00 Bangkok.
NOW = datetime(2026, 11, 9, 10, 0, tzinfo=timezone.utc)
FEEDS = ("Matichon", "Bangkok Post Business", "Blognone", "Notebookspec")


def _article(publisher, article_id, **overrides):
    record = {
        "id": f"{publisher}|{article_id}",
        "source": publisher,
        "publisher": publisher,
        "article_id": str(article_id),
        "headline": f"{publisher} headline {article_id}",
        "canonical_url": f"https://example.com/{publisher.replace(' ', '-')}/{article_id}",
        "published_at": "2026-11-09T02:00:00Z",
        "section": "",
        "author": "desk",
        "summary": "article text that must never reach Gold",
        "image_url": "",
        "source_feed_url": "https://example.com/feed",
        "captured_at": "2026-11-09T08:15:00Z",
        "capture_kind": "rss_article",
    }
    record.update(overrides)
    return record


def _fixture():
    rows = [_article(feed, 1) for feed in FEEDS]
    rows += [
        # Same article seen again later: Silver keeps the newest capture.
        _article("Matichon", 1, headline="Matichon headline 1 (updated)", captured_at="2026-11-09T09:15:00Z"),
        # 17:30 UTC on 11-08 = 00:30 on 11-09 in Bangkok.
        _article("Matichon", 2, published_at="2026-11-08T17:30:00Z", image_url="https://example.com/i.jpg"),
        # Outside the 7-date window that ends on 2026-11-09.
        _article("Blognone", 2, published_at="2026-11-01T05:00:00Z", section="technology"),
        # Published after the as-of date: left out of every mart.
        _article("Blognone", 3, published_at="2026-11-11T05:00:00Z"),
        _article("Notebookspec", 2, headline=""),  # quarantined
    ]
    return rows


def _ingest(root, records, raw=b"news-marts-fixture"):
    ingest_payload(
        raw,
        records,
        source="book-news-scraping",
        domain="news",
        dataset="articles",
        data_lake_uri=root,
        input_format="text",
        event_time_field="captured_at",
        privacy_class="public",
    )


def _dbt_available() -> bool:
    from data_lake.jobs_marts import resolve_dbt_bin

    return resolve_dbt_bin() is not None


class NewsMartsTests(unittest.TestCase):
    def setUp(self):
        try:
            import duckdb  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("duckdb/pyarrow not installed")
        if not _dbt_available():
            if os.environ.get("SOLO_EMPIRE_CI") == "1":
                self.fail("dbt not found; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
            self.skipTest("dbt not installed")

    def _run(self, root, **kwargs):
        from data_lake.news_marts import run_news_marts

        kwargs.setdefault("now", NOW)
        # The fixture quarantines 1 of 11 articles on purpose.
        kwargs.setdefault("max_invalid_ratio", 0.2)
        return run_news_marts(data_lake_uri=root, **kwargs)

    def _rows(self, root, result, mart, sql):
        return [tuple(r.values()) for r in query_parquet(str(Path(root) / result["marts"][mart]["key"]), sql)]

    def test_marts_are_built_from_the_current_silver_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, _fixture())
            result = self._run(root)
            self.assertEqual(result["status"], "success", result["freshness"])
            self.assertEqual(result["silver"]["record_count"], 7)
            self.assertEqual(result["silver"]["invalid_count"], 1)

            daily = self._rows(
                root, result, "news_articles_daily",
                "SELECT CAST(published_date AS VARCHAR), publisher, section, articles, with_image "
                "FROM lake_table ORDER BY 1, 2, 3",
            )
            self.assertEqual(
                daily,
                [
                    ("2026-11-01", "Blognone", "technology", 1, 0),
                    ("2026-11-09", "Bangkok Post Business", "unspecified", 1, 0),
                    ("2026-11-09", "Blognone", "unspecified", 1, 0),
                    ("2026-11-09", "Matichon", "unspecified", 2, 1),
                    ("2026-11-09", "Notebookspec", "unspecified", 1, 0),
                ],
            )

            health = self._rows(
                root, result, "news_feed_health",
                "SELECT publisher, articles_total, articles_7d, CAST(as_of_date AS VARCHAR), "
                "strftime(last_seen_at, '%H:%M') FROM lake_table ORDER BY publisher",
            )
            self.assertEqual(
                health,
                [
                    ("Bangkok Post Business", 1, 1, "2026-11-09", "08:15"),
                    ("Blognone", 2, 1, "2026-11-09", "08:15"),
                    ("Matichon", 2, 2, "2026-11-09", "09:15"),
                    ("Notebookspec", 1, 1, "2026-11-09", "08:15"),
                ],
            )

            latest = self._rows(
                root, result, "news_latest_articles",
                "SELECT publisher, rank, headline FROM lake_table WHERE publisher = 'Matichon' ORDER BY rank",
            )
            self.assertEqual(
                latest,
                [("Matichon", 1, "Matichon headline 1 (updated)"), ("Matichon", 2, "Matichon headline 2")],
            )

    def test_gold_never_carries_article_text_or_authors(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, _fixture())
            result = self._run(root)
            store = ObjectStore(root)
            for mart in result["marts"].values():
                body = store.get_bytes(mart["key"])
                self.assertNotIn(b"must never reach Gold", body)
                self.assertNotIn(b"desk", body)

    def test_latest_articles_keeps_at_most_30_per_publisher(self):
        rows = [_article(feed, 1) for feed in FEEDS]
        rows += [
            _article("Matichon", i, published_at=f"2026-11-09T{i % 10:02d}:{i:02d}:00Z") for i in range(2, 40)
        ]
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, rows)
            result = self._run(root)
            counts = self._rows(
                root, result, "news_latest_articles",
                "SELECT publisher, count(*), max(rank) FROM lake_table GROUP BY 1 ORDER BY 1",
            )
            self.assertIn(("Matichon", 30, 30), counts)

    def test_missing_or_old_feed_makes_the_run_stale_but_gold_is_written(self):
        rows = [_article(feed, 1) for feed in FEEDS[:3]]
        rows.append(_article("Matichon", 9, captured_at="2026-11-09T09:30:00Z"))
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, rows)
            result = self._run(root)
            self.assertEqual(result["status"], "stale")
            # Notebookspec is missing; the other two last captured at 08:15 (1 h 45 m < 6 h).
            self.assertEqual(result["freshness"]["stale_publishers"], ["Notebookspec"])
            self.assertTrue(result["manifest_key"])
            late = self._run(root, now=datetime(2026, 11, 9, 15, 0, tzinfo=timezone.utc))
            self.assertEqual(
                late["freshness"]["stale_publishers"],
                ["Bangkok Post Business", "Blognone", "Notebookspec"],
            )

    def test_rerun_is_idempotent_and_publishes_a_sortable_pointer(self):
        prefix = "control/pointers/gold/source=book-news-scraping/domain=news/"
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, _fixture())
            first = self._run(root)
            self.assertTrue(all(m["key"].startswith("gold/domain=news/") for m in first["marts"].values()))
            self.assertRegex(
                first["pointer_key"],
                rf"^{re.escape(prefix)}published_at=20261109T100000Z-{first['gold_run_id']}-[0-9a-f]{{12}}\.json$",
            )
            again = self._run(root)
            self.assertEqual(again["gold_run_id"], first["gold_run_id"])
            self.assertTrue(again["silver"]["existed"])
            self.assertTrue(all(m["existed"] for m in again["marts"].values()))
            self.assertTrue(again["manifest_existed"])
            self.assertTrue(again["pointer_existed"])
            manifest = json.loads(ObjectStore(root).get_bytes(first["manifest_key"]))
            self.assertEqual(manifest["source"], "book-news-scraping")
            self.assertEqual(manifest["privacy_class"], "public")
            self.assertEqual(manifest["input"]["silver_run_id"], first["silver_run_id"])
            pointer = json.loads(ObjectStore(root).get_bytes(first["pointer_key"]))
            self.assertEqual(pointer["manifest_key"], first["manifest_key"])
            self.assertEqual(pointer["domain"], "news")

    def test_rerun_with_a_later_clock_reuses_the_pointer_until_gold_changes(self):
        prefix = "control/pointers/gold/source=book-news-scraping/domain=news/"
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, _fixture())
            first = self._run(root)
            later = self._run(root, now=datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc))
            self.assertEqual(later["pointer_key"], first["pointer_key"])
            self.assertTrue(later["pointer_existed"])
            self.assertEqual(len(ObjectStore(root).list_keys(prefix)), 1)
            # New Bronze → new Gold run → a new, greater pointer.
            _ingest(root, [_article("Matichon", 50, captured_at="2026-11-09T11:15:00Z")], raw=b"batch-2")
            newer = self._run(root, now=datetime(2026, 11, 9, 12, 0, tzinfo=timezone.utc))
            self.assertNotEqual(newer["gold_run_id"], first["gold_run_id"])
            self.assertFalse(newer["pointer_existed"])
            self.assertEqual(max(ObjectStore(root).list_keys(prefix)), newer["pointer_key"])

    def test_quality_gate_failure_writes_no_gold(self):
        from data_lake.silver import SilverQualityError

        with tempfile.TemporaryDirectory() as root:
            _ingest(root, _fixture())
            with self.assertRaises(SilverQualityError):
                self._run(root, max_invalid_ratio=0.02)
            self.assertEqual(ObjectStore(root).list_keys("gold/"), [])

    def test_unparseable_published_at_is_quarantined(self):
        from data_lake.silver import normalize_news_row

        row = {
            "source_record_id": "Matichon|1",
            "payload_json": json.dumps(_article("Matichon", 1, published_at="yesterday")),
        }
        _, errors = normalize_news_row(row, silver_run_id="silver-x")
        self.assertEqual(errors, ["unparseable published_at"])


    def test_valid_published_at_is_written_as_utc_iso_that_duckdb_can_cast(self):
        from data_lake.silver import normalize_news_row

        for raw, expected in (
            ("20261109T02:00:00Z", "2026-11-09T02:00:00Z"),
            ("2026-11-09T09:00:00,123+07:00", "2026-11-09T02:00:00.123000Z"),
            ("2026-11-09 02:00:00", "2026-11-09T02:00:00Z"),  # no offset: UTC
        ):
            row = {"source_record_id": "Matichon|1", "payload_json": json.dumps(_article("Matichon", 1, published_at=raw))}
            item, errors = normalize_news_row(row, silver_run_id="silver-x")
            self.assertEqual((errors, item["published_at"]), ([], expected), raw)

    def test_model_fingerprint_follows_the_dbt_project_settings(self):
        from unittest import mock

        from data_lake import news_marts

        before = news_marts._model_fingerprint()
        real = Path.read_bytes

        def patched(path):
            data = real(path)
            if path.name == "dbt_project.yml":
                return data.replace(b"Asia/Bangkok", b"UTC")
            return data

        with mock.patch.object(Path, "read_bytes", patched):
            self.assertNotEqual(news_marts._model_fingerprint(), before)

    def test_article_urls_keep_their_query_but_drop_tracking(self):
        from data_lake.silver import normalize_news_row

        for raw, expected in (
            ("https://www.matichon.co.th/?p=123", "https://www.matichon.co.th/?p=123"),
            ("https://WWW.Example.com/a/1?id=9&utm_source=rss&utm_medium=x&fbclid=abc#top",
             "https://www.example.com/a/1?id=9"),
            ("https://example.com/news/1/", "https://example.com/news/1"),
        ):
            row = {"source_record_id": "Matichon|1",
                   "payload_json": json.dumps(_article("Matichon", 1, canonical_url=raw))}
            item, errors = normalize_news_row(row, silver_run_id="silver-x")
            self.assertEqual((errors, item["canonical_url"]), ([], expected), raw)

if __name__ == "__main__":
    unittest.main()
