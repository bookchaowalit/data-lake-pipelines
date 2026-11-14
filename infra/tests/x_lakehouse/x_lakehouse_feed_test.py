#!/usr/bin/env python3
"""Raw X Feed facts (feed.py) and their three readers: the serving tab, the digest, the site.

Offline: the lake is a LocalStore in a temp dir; no Postgres, Sheet, gateway or Telegram.
Spec: docs/specs/x-feed-serving.md.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import bi_site, digest, feed, serve_sheet  # noqa: E402
from x_lakehouse.append import append_rows  # noqa: E402
from x_lakehouse.lake import LocalStore  # noqa: E402
from x_lakehouse.sheet import assert_writable  # noqa: E402

UTC = timezone.utc
DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
START = datetime(2026, 11, 13, 0, 0, tzinfo=UTC)
END = datetime(2026, 11, 14, 0, 0, tzinfo=UTC)


def row(
    post_id: str,
    at: datetime,
    *,
    likes: int = 0,
    reposts: int = 0,
    replies: int = 0,
    views: int = 0,
    watch: tuple[str, str] = ("keyword", "duckdb"),
    text: str = "hello",
    author: str = "a",
) -> dict:
    return {
        "post_id": post_id,
        "collected_at": at,
        "requesting_chief": "Chief Data",
        "watch_type": watch[0],
        "watch_value": watch[1],
        "author_handle": author,
        "posted_at": at - timedelta(hours=1),
        "post_url": f"https://x.com/{author}/status/{post_id}",
        "likes": likes,
        "reposts": reposts,
        "replies": replies,
        "views": views,
        "tag": "",
        "query_id": "Q1",
        "text": text,
    }


class SummariseTests(unittest.TestCase):
    def test_one_row_per_post_newest_counts_and_every_watch_counted(self):
        t = START + timedelta(hours=2)
        rows = [
            row("1", t, likes=1, watch=("keyword", "K07")),
            row(
                "1", t + timedelta(hours=8), likes=9, watch=("account", "A96")
            ),  # seen again, fresher
            row("2", t, likes=3, reposts=1),
            row("3", t, likes=100, reposts=0, replies=0),
            row("4", START - timedelta(minutes=1), likes=999),  # before the window
            row("5", END, likes=999),  # the end is exclusive
            row("", t, likes=999),
        ]  # no post id
        w = feed.summarise(rows, START, END)
        self.assertEqual([p["post_id"] for p in w.posts], ["3", "1", "2"])
        one = next(p for p in w.posts if p["post_id"] == "1")
        self.assertEqual(
            (one["likes"], one["watches"], one["watch_value"]), (9, 2, "A96")
        )
        self.assertEqual(w.observations, 4)
        self.assertEqual(
            dict(w.watches), {"keyword:duckdb": 2, "keyword:K07": 1, "account:A96": 1}
        )

    def test_engagement_weights_reposts_and_views_break_ties(self):
        t = START + timedelta(hours=1)
        w = feed.summarise(
            [
                row("a", t, likes=4),
                row("b", t, likes=2, reposts=1, views=5),
                row("c", t, likes=2, reposts=1, views=50),
            ],
            START,
            END,
        )
        self.assertEqual(
            [(p["post_id"], p["engagement"]) for p in w.posts],
            [("c", 4), ("b", 4), ("a", 4)],
        )

    @unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres (append_rows writes curated too)")
    def test_load_reads_the_raw_lake(self):
        from x_lakehouse import curated as cur

        conn = cur.connect(DB)
        self.addCleanup(conn.close)
        with tempfile.TemporaryDirectory() as tmp:
            store = LocalStore(Path(tmp))
            t = "2026-11-13 10:00"
            base = {
                "requesting_chief": "Chief Data",
                "watch_type": "keyword",
                "watch_value": "duckdb",
                "author_handle": "a",
                "posted_at": "2026-11-13 09:00",
                "text": "x",
                "replies": 0,
                "views": 1,
                "tag": "",
                "query_id": "Q1",
                "score": "",
                "top": "",
            }
            append_rows(
                {
                    "x_feed": [
                        {
                            **base,
                            "collected_at": t,
                            "post_id": "11",
                            "post_url": "https://x.com/a/status/11",
                            "likes": 5,
                            "reposts": 0,
                        },
                        {
                            **base,
                            "collected_at": t,
                            "post_id": "12",
                            "post_url": "https://x.com/a/status/12",
                            "likes": 1,
                            "reposts": 0,
                        },
                    ]
                },
                store=store,
                prefix="",
                conn=conn,
                now=END,
            )
            w = feed.load(store, "", START, END)
        self.assertEqual([p["post_id"] for p in w.posts], ["11", "12"])


class ServeTests(unittest.TestCase):
    def test_feed_tab_is_writable_and_rows_follow_the_header(self):
        assert_writable(serve_sheet.SERVE_TABS["x_feed"])
        w = feed.summarise([row("1", START + timedelta(hours=1), likes=2)], START, END)
        [cells] = serve_sheet.feed_rows(w)
        self.assertEqual(len(cells), len(serve_sheet.FEED_COLUMNS))
        got = dict(zip(serve_sheet.FEED_COLUMNS, cells))
        self.assertEqual(
            (got["engagement"], got["collected_at"], got["post_id"]),
            (2, "2026-11-13 08:00", "1"),
        )

    def test_feed_tab_keeps_the_top_of_the_window(self):
        t = START + timedelta(hours=1)
        w = feed.summarise(
            [row(str(i), t, likes=i) for i in range(serve_sheet.FEED_ROWS + 5)],
            START,
            END,
        )
        rows = serve_sheet.feed_rows(w)
        self.assertEqual(len(rows), serve_sheet.FEED_ROWS)
        self.assertEqual(
            rows[0][serve_sheet.FEED_COLUMNS.index("likes")], serve_sheet.FEED_ROWS + 4
        )


class DigestTests(unittest.TestCase):
    def facts(self, posts):
        win = digest.Window("midday", START, END)
        f = digest.Facts(
            win,
            totals={
                "signals": 0,
                "alerts": 0,
                "urgent": 0,
                "opportunities": 0,
                "strong_sources": 0,
            },
        )
        digest.add_feed(f, feed.summarise(posts, START, END), ("acme corp",))
        return f

    def test_feed_facts_are_screened_ranked_and_their_links_allowed(self):
        t = START + timedelta(hours=1)
        f = self.facts(
            [
                row("1", t, likes=50, text="deal with ACME Corp"),
                row("2", t, likes=5),
                row("3", t, likes=1, watch=("keyword", "acme corp")),
            ]
        )
        self.assertEqual(f.withheld, 2)
        self.assertEqual([p["link"] for p in f.feed_top], ["https://x.com/a/status/2"])
        # Watch counts come from the screened posts (post 1's keyword:duckdb sighting is not counted).
        self.assertEqual((f.feed_watches, f.feed_totals["watches"]), ([{"watch": "keyword:duckdb", "posts": 1}], 1))
        self.assertTrue(f.has_data)
        self.assertIn("https://x.com/a/status/2", f.links)
        self.assertNotIn("acme corp", str(f.as_prompt()).lower())
        answer = digest.validate_answer(
            {
                "headline_th": "หัวข้อ",
                "summary_th": ["สรุป"],
                "watch": [],
                "alerts": [],
                "opportunities": [
                    {"title": "t", "why": "w", "link": "https://x.com/a/status/2"}
                ],
            },
            f,
            ("acme corp",),
        )
        self.assertEqual(answer["opportunities"][0]["link"], "https://x.com/a/status/2")

    def test_a_client_term_outside_the_shown_columns_still_withholds_the_post(self):
        # Codex P1 2026-11-14: _extra / _coerce_errors / another watch's value must be screened too.
        t = START + timedelta(hours=1)
        f = self.facts([{**row("1", t, likes=9), "_extra": '{"note": "for Acme Corp"}'},
                        row("2", t, likes=5), row("2", t + timedelta(hours=1), watch=("account", "AcmeCorp")),  # skeleton match
                        {**row("2", t + timedelta(hours=2)), "watch_value": "duckdb"},
                        row("3", t, likes=1), {**row("3", t + timedelta(minutes=5)), "_coerce_errors": "ACME CORP"}])
        self.assertEqual(f.feed_top, [])
        self.assertEqual(f.withheld, 3)  # 1: _extra, 2: an older observation's watch, 3: _coerce_errors

    def test_render_without_signals_shows_the_feed(self):
        f = self.facts(
            [row("2", START + timedelta(hours=1), likes=5, text="duckdb 2.0 <b>")]
        )
        text = digest.render(f, None)
        self.assertIn("โพสต์ดิบ 1", text)
        self.assertIn("โพสต์ที่คนสนใจมากสุด", text)
        self.assertIn('href="https://x.com/a/status/2"', text)
        self.assertNotIn("<b>⁠", text.replace("&lt;b&gt;", ""))  # post text is escaped
        self.assertNotIn("ยังไม่มีสัญญาณใหม่", text)

    def test_empty_feed_and_no_signals_is_the_short_note(self):
        self.assertIn("ยังไม่มีสัญญาณใหม่", digest.render(self.facts([]), None))


class ServeFailureTests(unittest.TestCase):
    def test_a_lake_failure_skips_the_feed_tab_and_fails_the_refresh(self):
        from unittest import mock

        import io
        from contextlib import redirect_stderr, redirect_stdout

        fake = mock.MagicMock()
        with mock.patch.object(serve_sheet.cur, "connect", return_value=fake), \
                mock.patch.object(serve_sheet, "feed_window", side_effect=OSError("r2 down")), \
                mock.patch.object(serve_sheet, "fetch", return_value={}) as fetch, \
                mock.patch.dict("os.environ", {"X_LAKE_DB_URL": "x"}):
            err, out = io.StringIO(), io.StringIO()
            with redirect_stderr(err), redirect_stdout(out):
                code = serve_sheet.main([])
        self.assertEqual(fetch.call_args.args[1], None)
        self.assertIn("lake read failed (OSError)", err.getvalue())
        self.assertEqual(code, 0)  # dry run: nothing written, nothing stale


class SiteTests(unittest.TestCase):
    def test_top_posts_page_is_screened_and_escaped(self):
        from x_lakehouse_bi_test import snapshot  # the site test's base snapshot

        t = START + timedelta(hours=1)
        snap = snapshot()
        bi_site.add_feed(
            snap,
            feed.summarise(
                [
                    row("1", t, likes=9, text="<script>alert(1)</script>"),
                    row("2", t, likes=3, text="ACME Corp news"),
                ],
                START,
                END,
            ),
            ("acme corp",),
        )
        html = bi_site.render_feed(snap)
        self.assertIn("Top posts", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn("<script>", html)
        self.assertNotIn("ACME", html)
        self.assertIn("1 post(s) naming a client are left out", html)
        self.assertIn("2026-11-13 08:00", html)  # Bangkok time
        with tempfile.TemporaryDirectory() as tmp:
            info = bi_site.build_site(snap, Path(tmp) / "site")
            self.assertTrue((Path(tmp) / "site" / "feed" / "index.html").exists())
        self.assertEqual(info["posts"], 1)


class SiteFeedPagesTests(unittest.TestCase):
    """2026-11-14 step 3: the overview, Signals and Claims pages on the raw feed."""

    def snap(self):
        from x_lakehouse_bi_test import snapshot

        t = START + timedelta(hours=1)
        rows = [row("1", t, likes=9), row("1", t + timedelta(hours=2), watch=("account", "AcmeCorp")),
                row("2", t, likes=3, author="b"), row("3", t - timedelta(days=3), likes=1, author="c")]
        snap = snapshot()
        bi_site.add_feed(snap, feed.summarise(rows, START, END), ("acme corp",), rows,
                         (START - timedelta(days=13), END))
        return snap

    def test_daily_counts_distinct_posts_per_watch_and_ict_day(self):
        t = START + timedelta(hours=1)  # 08:00 ICT on 13 Nov
        out = feed.daily([row("1", t), row("1", t + timedelta(hours=1)), row("2", t),
                          row("3", START - timedelta(hours=8))], START - timedelta(days=1), END)  # 12 Nov 23:00 ICT
        self.assertEqual([(str(r["day"]), r["watch"], r["posts"]) for r in out],
                         [("2026-11-12", "keyword:duckdb", 1), ("2026-11-13", "keyword:duckdb", 2)])

    def test_overview_counts_posts_and_masks_client_watches(self):
        snap = self.snap()
        html = bi_site.render_overview(snap)
        self.assertIn('data-kpi="posts">1<', html)  # post 1 is withheld (a client watch saw it)
        self.assertIn('data-kpi="authors">1<', html)
        self.assertIn('data-kpi="watches">1<', html)  # keyword:duckdb only: post 1's watches are not counted
        # The chart counts screened posts only and drops client-named watches (no "(withheld)" series).
        self.assertEqual([(str(r["day"]), r["watch"], r["posts"]) for r in snap.feed_daily],
                         [("2026-11-10", "keyword", 1), ("2026-11-13", "keyword", 1)])  # by watch type
        self.assertNotIn("(withheld)", html)
        self.assertIn("Raw posts to 2026-11-14 07:00 Bangkok time", html)  # footer names the feed window
        self.assertIn("Posts per day by watch", html)
        self.assertNotIn("Acme", html)
        self.assertIn('href="/feed/"', html)
        self.assertNotIn('href="/signals/"', html)  # the nav drops Top signals

    def test_signals_page_points_to_top_posts_and_claims_are_marked_frozen(self):
        snap = self.snap()
        sig = bi_site.render_signals(snap)
        self.assertIn('<a href="/feed/">Top posts</a>', sig)
        self.assertNotIn("grant open", sig)  # no stale signal rows
        self.assertIn("No new claims since 13 November 2026", bi_site.render_claims(snap))
        with tempfile.TemporaryDirectory() as tmp:
            info = bi_site.build_site(snap, Path(tmp) / "site")
        self.assertEqual(info["pages"], 5)


if __name__ == "__main__":
    unittest.main()
