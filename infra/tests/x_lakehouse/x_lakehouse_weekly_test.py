#!/usr/bin/env python3
"""Tests for the weekly analyses and triage audit (spec docs/specs/x-weekly-analyses.md, W1..W6).

Postgres tests run only with X_LAKE_TEST_DB_URL (throwaway database; they drop schema x_intel).
The gateway is a fake (httpx.MockTransport): nothing leaves the machine.
"""

from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import datetime
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import curated as cur  # noqa: E402
from x_lakehouse import weekly  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402
from x_lakehouse.inbox import ensure_inbox  # noqa: E402

DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
ICT = weekly.TZ
MONDAY = datetime(2026, 11, 16, 8, 20, tzinfo=ICT)  # the run after week 2026-W46 (Nov 9..15)


class WeekTests(unittest.TestCase):
    def test_w1_the_monday_run_targets_the_week_that_just_ended(self):
        w = weekly.last_complete_week(MONDAY)
        self.assertEqual(w.label, "2026-W46")
        self.assertEqual(w.start, datetime(2026, 11, 9, tzinfo=ICT))
        self.assertEqual(w.end, datetime(2026, 11, 16, tzinfo=ICT))
        self.assertEqual(w.prev.label, "2026-W45")
        self.assertEqual(weekly.last_complete_week(datetime(2026, 11, 15, 23, 59, tzinfo=ICT)).label, "2026-W45")

    def test_confusions_use_the_bot_format(self):
        self.assertEqual(weekly._confusions([("a", "b"), ("a", "b"), ("c", "c"), ("c", "d")]), "a->b x2; c->d x1")


class FakeGateway:
    def __init__(self, label="fact_report", route="store", status=200):
        self.label, self.route, self.status, self.prompts = label, route, status, []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        posts = json.loads(body["messages"][1]["content"])
        self.prompts.append(body["messages"][1]["content"])
        if self.status != 200:
            return httpx.Response(self.status, json={"code": "x"})
        items = [{"i": p["i"], "label": self.label, "route": self.route} for p in posts]
        return httpx.Response(200, json={"content": json.dumps({"items": items})})

    def client(self):
        return httpx.Client(transport=httpx.MockTransport(self))


class FeedAnalysesTests(unittest.TestCase):
    """Raw X Feed twins of the signal analyses (2026-11-14): no Postgres, no AI labels."""

    def test_feed_analyses_follow_their_rules(self):
        from datetime import timedelta, timezone
        from unittest import mock

        from x_lakehouse import feed

        def r(pid, when, text="post", likes=0, reposts=0, watch=("keyword", "ai")):
            at = datetime.strptime(when, "%Y-%m-%d %H:%M").replace(tzinfo=ICT).astimezone(timezone.utc)
            return {"post_id": pid, "collected_at": at, "watch_type": watch[0], "watch_value": watch[1],
                    "author_handle": "a", "post_url": f"https://x.com/a/status/{pid}", "text": text,
                    "likes": likes, "reposts": reposts, "replies": 0, "views": 0}

        rows = [r(f"a{i}", "2026-11-10 09:00", likes=1) for i in range(25)]  # W46: 25 posts on keyword:ai
        rows += [r(f"b{i}", "2026-11-03 09:00") for i in range(22)]  # W45: 22
        rows += [r("t1", "2026-11-11 10:00", "ข่าวไทย วันนี้", likes=50),
                 r("t1", "2026-11-12 10:00", "ข่าวไทย วันนี้", likes=80, watch=("account", "news")),  # seen again
                 r("d1", "2026-11-12 10:00", "a thread", likes=100, reposts=40),
                 r("p1", "2026-11-12 11:00", "oil up 3% today", likes=5),
                 r("n1", "2026-11-17 10:00", "next week", likes=999)]
        import tempfile

        from x_lakehouse.lake import LocalStore

        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(feed, "load_rows", return_value=rows) as load:
            out = weekly.analyses(mock.MagicMock(), weekly.last_complete_week(MONDAY), store=LocalStore(Path(tmp)),
                                  prefix="")
        self.assertEqual(load.call_args.args[2], weekly.last_complete_week(MONDAY).start - timedelta(weeks=5))
        by = {}
        for x in out:
            by.setdefault(x["analysis"], []).append(x)
        top = by["watch_sustained"][0]
        self.assertEqual((top["item"], top["value"]), ("keyword:ai", "28"))
        self.assertTrue(top["metric"].startswith("W46:28 W45:22"), top["metric"])
        self.assertTrue(top["note"].startswith("2 week(s) in a row at 20+"))
        self.assertEqual([(x["post_id"], x["geo"]) for x in by["feed_local_discovery"]], [("t1", "TH")])
        self.assertEqual(by["feed_local_discovery"][0]["metric"], "engagement 80")  # newest observation
        self.assertEqual(by["feed_community_knowledge"][0]["post_id"], "d1")
        self.assertEqual(by["feed_community_knowledge"][0]["metric"], "reposts/like 0.40")
        self.assertEqual((by["feed_price_reported"][0]["post_id"], by["feed_price_reported"][0]["value"]),
                         ("p1", "3%"))
        self.assertNotIn("n1", {x["post_id"] for x in out})  # next week stays out


@unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class WeeklyPostgresTests(unittest.TestCase):
    def setUp(self):
        self.conn = cur.connect(DB)
        with self.conn.cursor() as c:
            c.execute("drop schema if exists x_intel cascade")
        self.conn.commit()
        ensure_inbox(self.conn)
        self.cfg = Config(env={"CLIENT_DENYLIST": "acme corp", "AI_GATEWAY_TOKEN": "t"})
        rows = []
        for i in range(25):  # topic AI: 25 this week (W46), 22 the week before
            rows.append((f"a{i}", "2026-11-10 09:00", "AI", "fact_report", "store", "News", f"ข่าว AI {i}", 10, 1, None))
            if i < 22:
                rows.append((f"b{i}", "2026-11-03 09:00", "AI", "fact_report", "store", "News", "old", 1, 0, None))
        rows += [
            ("p1", "2026-11-11 10:00", "Energy", "fact_report", "alert_digest", "News", "oil up 3% today", 5, 0, None),
            ("d1", "2026-11-12 10:00", "Tips", "opinion", "store", "Discussion", "a thread", 100, 40, "Career & Labour"),
            ("e1", "2026-11-12 11:00", "Jobs", "evidence", "store", "News", "labour data", 1, 0,
             "Career & Labour; Economics"),
            ("x1", "2026-11-12 12:00", "Deals", "fact_report", "store", "News", "deal with Acme Corp", 1, 0, None),
            ("o1", "2026-11-17 10:00", "AI", "fact_report", "store", "News", "next week", 1, 0, None),
        ]
        with self.conn.cursor() as c:
            for pid, ts, topic, label, route, kind, text, likes, reposts, life in rows:
                c.execute("insert into x_intel.x_signals (post_id, collected_at, topic, domain, content_label, route, "
                          "signal_type, post_text, post_url, likes, reposts, life_cat, cred_tier) values "
                          "(%s, %s::timestamp at time zone 'Asia/Bangkok', %s, 'D1', %s, %s, %s, %s, %s, %s, %s, %s, 'T3')",
                          (pid, ts, topic, label, route, kind, text, f"https://x.com/a/status/{pid}", likes, reposts, life))
        self.conn.commit()

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def q(self, sql, *args):
        with self.conn.cursor() as c:
            c.execute(sql, args)
            rows = c.fetchall()
        self.conn.commit()
        return rows

    def run_weekly(self, mode="write", gw=None, now=MONDAY):
        gw = gw or FakeGateway()
        with gw.client() as http:
            return weekly.run(self.conn, self.cfg, mode=mode, now=now, http=http), gw

    def test_w2_each_analysis_follows_its_rule(self):
        weekly.ensure_tables(self.conn)
        rows = weekly.analyses(self.conn, weekly.last_complete_week(MONDAY))
        self.conn.commit()
        by = {}
        for r in rows:
            by.setdefault(r["analysis"], []).append(r)
        fad = by["fad_sustained"][0]
        self.assertEqual((fad["item"], fad["value"]), ("AI", "25"))
        self.assertTrue(fad["metric"].startswith("W46:25 W45:22"))
        self.assertEqual(fad["note"], "2 week(s) in a row at 20+")
        self.assertEqual(by["debate"][0]["item"], "(no open or new claims this week)")
        self.assertTrue(all(r["geo"] == "TH" for r in by["local_discovery"]))
        self.assertEqual(by["community_knowledge"][0]["post_id"], "d1")
        self.assertEqual(by["community_knowledge"][0]["metric"], "reposts/like 0.40")
        self.assertEqual([r["item"] for r in by["life_path"]], ["Career & Labour", "Economics"])  # d1 is an opinion
        self.assertEqual((by["price_reported"][0]["post_id"], by["price_reported"][0]["value"]), ("p1", "3%"))
        self.assertNotIn("o1", {r["post_id"] for r in rows})  # next week stays out

    def test_w3_write_is_idempotent_and_w5_audit_counts_agreement(self):
        rep, _ = self.run_weekly()
        self.assertEqual(rep["week"], "2026-W46")
        self.assertEqual(rep["triage"]["n"], 28)  # 29 posts this week, 1 withheld
        n = self.q("select count(*) from x_intel.lh_analyses where report_week = '2026-W46'")[0][0]
        self.assertGreater(n, 0)
        audit = self.q("select n, label_agree, route_confusions, rule_version from x_intel.lh_triage_audit")
        self.assertEqual(audit[0][0], 28)
        self.assertLess(audit[0][1], 1.0)  # the opinion and the evidence post disagree with "fact_report"
        self.assertIn("->store", audit[0][2])
        self.assertEqual(audit[0][3], "lh-v1")
        rep2, gw2 = self.run_weekly()
        self.assertEqual((rep2["analyses"], rep2["triage"]), ("already_written", "already_written"))
        self.assertEqual(gw2.prompts, [])
        self.assertEqual(self.q("select count(*) from x_intel.lh_analyses")[0][0], n)

    def test_w5_denylisted_rows_never_reach_the_prompt(self):
        _, gw = self.run_weekly()
        self.assertTrue(gw.prompts)
        self.assertNotIn("acme", gw.prompts[0].lower())

    def test_w5_a_gateway_error_leaves_no_row_and_the_next_run_retries(self):
        rep, _ = self.run_weekly(gw=FakeGateway(status=502))
        self.assertTrue(rep["triage"].startswith("error"))
        self.assertEqual(self.q("select count(*) from x_intel.lh_triage_audit")[0][0], 0)
        rep2, gw2 = self.run_weekly()
        self.assertEqual(rep2["analyses"], "already_written")
        self.assertEqual(rep2["triage"]["n"], 28)
        self.assertEqual(len(gw2.prompts), 1)

    def test_w4_dry_writes_nothing_and_never_calls_the_gateway(self):
        rep, gw = self.run_weekly(mode="dry")
        self.assertEqual(rep["triage"], "skipped_in_dry")
        self.assertGreater(rep["analyses"]["fad_sustained"], 0)
        self.assertEqual(gw.prompts, [])
        self.assertEqual(self.q("select count(*) from x_intel.lh_analyses")[0][0], 0)

    def test_w5_no_denylist_means_no_audit(self):
        self.cfg = Config(env={"AI_GATEWAY_TOKEN": "t"})
        with self.assertRaises(Exception):
            self.run_weekly()
        self.assertEqual(self.q("select count(*) from x_intel.lh_triage_audit")[0][0], 0)


class BatchTests(unittest.TestCase):
    def test_review8_thai_posts_go_in_batches_that_fit_the_gateway_budget(self):
        posts = [{"post_text": "ข่าว" * 200, "cred_tier": "T3", "domain": "D1"} for _ in range(30)]
        groups = weekly.batches(posts)
        self.assertGreater(len(groups), 1)
        self.assertEqual(sorted(i for g in groups for i in g), list(range(30)))
        for g in groups:
            size = len(json.dumps([weekly._item(i, posts[i]) for i in g], ensure_ascii=False).encode())
            self.assertLessEqual(len(weekly.SYSTEM.encode()) + size + weekly.AUDIT_OUTPUT + 2048, weekly.MAX_TOKEN_BUDGET)

    def test_review8_every_batch_is_asked_and_answers_map_back(self):
        posts = [{"post_text": "ข่าว" * 200, "cred_tier": "T3", "domain": "D1"} for _ in range(30)]
        gw = FakeGateway(label="opinion")
        with gw.client() as http:
            answers = weekly.ask_audit(http, "t", "x-digest", posts)
        self.assertEqual(len(gw.prompts), len(weekly.batches(posts)))
        self.assertTrue(all(a.get("label") == "opinion" for a in answers))


class PriceVerifiedTests(unittest.TestCase):
    def test_w2_only_price_sources_count_as_verified_prices(self):
        import pyarrow as pa
        from unittest import mock

        w = weekly.last_complete_week(MONDAY)
        t = datetime(2026, 11, 12, 9, tzinfo=ICT)
        tbl = pa.Table.from_pylist([
            {"pulled_at_ict": t, "source": "energy_food_health", "sub_source": "fao_ffpi", "title": "FAO index",
             "metric_1": "136.0", "url": "https://fao.org"},
            {"pulled_at_ict": t, "source": "github_trending", "sub_source": "python", "title": "repo",
             "metric_1": "75", "url": "https://github.com"},
            {"pulled_at_ict": t, "source": "energy_food_health", "sub_source": "who_don", "title": "outbreak",
             "metric_1": "2026-DON624", "url": "https://who.int"},
            {"pulled_at_ict": datetime(2026, 11, 17, 9, tzinfo=ICT), "source": "energy_food_health",
             "sub_source": "fao_ffpi", "title": "next week", "metric_1": "1", "url": "https://eia.gov"},
        ])
        with mock.patch("x_lakehouse.lake.read_dataset", return_value=tbl):
            rows = weekly._price_verified(w, object(), "")
        self.assertEqual([(r["item"], r["value"], r["note"]) for r in rows],
                         [("FAO index", "136.0", "Alpha Feed energy_food_health/fao_ffpi")])


class OffTests(unittest.TestCase):
    def test_w6_off_does_nothing(self):
        self.assertEqual(weekly.run(object(), Config(env={}), mode="off"), {})


if __name__ == "__main__":
    unittest.main()
