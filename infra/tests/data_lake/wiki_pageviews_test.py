#!/usr/bin/env python3
"""Offline tests for the Wikipedia top-articles capture (spec: docs/specs/wiki-pageviews.md)."""

from __future__ import annotations

import contextlib
import io
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.wiki_pageviews import (
    API,
    TOP_N,
    WikimediaError,
    capture,
    collect,
    http_fetch,
    main,
)  # noqa: E402

DAY = date(2026, 11, 10)


def ranking(n: int):
    return {
        "items": [
            {
                "articles": [
                    {
                        "article": f"Page_{i}" if i != 2 else "Special:Search",
                        "views": 1000 - i,
                        "rank": i,
                    }
                    for i in range(1, n + 1)
                ]
            }
        ]
    }


class FakeApi:
    def __init__(self, n: int = 250, fail: bool = False):
        self.n, self.fail, self.calls = n, fail, []

    def __call__(self, url: str):
        self.calls.append(url)
        if self.fail:
            raise WikimediaError("HTTP 404")
        return ranking(self.n)


class CollectTests(unittest.TestCase):
    def test_top_n_per_project_with_flags(self):
        fake = FakeApi()
        rows = collect(fake, DAY, pause=0)
        self.assertEqual(len(rows), 2 * TOP_N)
        self.assertEqual(fake.calls[0], f"{API}/th.wikipedia/all-access/2026/11/10")
        first = rows[0]
        self.assertEqual(first["id"], "th.wikipedia:Page_1@2026-11-10")
        self.assertFalse(first["namespace_page"])
        self.assertTrue(rows[1]["namespace_page"])
        self.assertEqual(len({r["id"] for r in rows}), len(rows))

    def test_empty_or_bad_shape_is_an_error(self):
        with self.assertRaises(WikimediaError):
            collect(lambda url: ranking(0), DAY, pause=0)
        with self.assertRaises(WikimediaError):
            collect(lambda url: {"nope": 1}, DAY, pause=0)

    def test_fetch_refuses_other_hosts(self):
        fetch = http_fetch()
        for url in ("file:///etc/passwd", "https://evil.example/api"):
            with self.assertRaises(WikimediaError):
                fetch(url)


class CaptureTests(unittest.TestCase):
    def test_rerun_same_day_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = capture(FakeApi(), tmp, day=DAY, pause=0)
            self.assertTrue(first["new"])
            self.assertTrue(
                any(
                    "dataset=wiki_top_articles" in str(p)
                    for p in Path(tmp).rglob("*.parquet")
                )
            )
            self.assertFalse(capture(FakeApi(), tmp, day=DAY, pause=0)["new"])

    def test_errors_exit_2(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(out):
            self.assertEqual(
                main(
                    ["--data-lake-uri", tmp, "--date", "2026-11-10"],
                    fetch=FakeApi(fail=True),
                    pause=0,
                ),
                2,
            )
            self.assertEqual(main(["--data-lake-uri", ""], fetch=FakeApi(), pause=0), 2)
            self.assertEqual(
                main(
                    ["--data-lake-uri", tmp, "--require-cloud"],
                    fetch=FakeApi(),
                    pause=0,
                ),
                2,
            )
            self.assertEqual(
                main(
                    ["--data-lake-uri", tmp, "--date", "10/11/2026"],
                    fetch=FakeApi(),
                    pause=0,
                ),
                2,
            )
        self.assertIn("WikimediaError", out.getvalue())


if __name__ == "__main__":
    unittest.main()
