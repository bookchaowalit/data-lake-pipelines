#!/usr/bin/env python3
"""Tests for the X insights site (spec docs/specs/x-insights.md, B1..B7).

Render tests use an in-memory Snapshot; the Pages API is a fake (httpx.MockTransport). The Postgres test
runs only with X_LAKE_TEST_DB_URL (throwaway database; it drops schema x_intel).
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

import httpx

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import bi_site, pages_deploy, scheduled  # noqa: E402
from x_lakehouse import curated as cur  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402
from x_lakehouse.inbox import ensure_inbox  # noqa: E402

DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
DENY = ("acme corp",)


def signal(pid, text="plain news", url=None, domain="D2 Economy & Money", **kw):
    row = {"post_id": pid, "collected_ict": datetime(2026, 11, 10, 9, 0), "domain": domain, "signal_type": "News",
           "alert_tier": "none", "cred_tier": "T3", "verification_status": None, "suggested_action": None,
           "opportunity_score": 1, "author_handle": "a", "post_url": url or f"https://x.com/a/status/{pid}",
           "post_text": text, "likes": 1, "reposts": 0, "views": 10, "priority": 20}
    row.update(kw)
    return row


def snapshot(**kw):
    base = dict(
        as_of=date(2026, 11, 10), newest=datetime(2026, 11, 10, 9, 0),
        daily=[{"day": date(2026, 11, 10), "domain": "D2 Economy & Money", "signals": 3, "alerts": 1, "urgent": 0,
                "opportunities": 1},
               {"day": date(2026, 11, 9), "domain": "Acme Corp watch", "signals": 2, "alerts": 0, "urgent": 0,
                "opportunities": 0},
               {"day": date(2026, 10, 1), "domain": "D1 World & Politics", "signals": 9, "alerts": 0, "urgent": 0,
                "opportunities": 0}],
        signals=[signal("1", "grant open <script>alert(1)</script>"), signal("2", "deal with Acme Corp"),
                 signal("3", "ข่าวไทย", url="javascript:alert(1)")],
        claims=[{"claim_id": "c1", "claim": "rates fall", "verification_status": "unverified", "queue_state": "open",
                 "priority": 1.0, "next_check": datetime(2026, 11, 11, tzinfo=timezone.utc), "last_checked": None,
                 "first_seen_link": "https://example.com/c1", "checked_by": None},
                {"claim_id": "c2", "claim": "ACME CORP merger", "verification_status": None, "queue_state": "open",
                 "priority": 2.0, "next_check": None, "last_checked": None, "first_seen_link": None,
                 "checked_by": None}],
        digests=[{"digest_id": "midday:2026-11-10", "kind": "midday", "status": "sent", "signals": 189,
                  "cost_usd": 0.0012, "ai": True, "updated_at": datetime(2026, 11, 10, 9, 28)}],
    )
    base.update(kw)
    return bi_site.screen(bi_site.Snapshot(**base), DENY)


def build(snap):
    tmp = tempfile.TemporaryDirectory()
    out = Path(tmp.name) / "site"
    info = bi_site.build_site(snap, out)
    return tmp, out, info


class RenderTests(unittest.TestCase):
    def test_b1_pages_and_kpis(self):
        tmp, out, info = build(snapshot())
        self.addCleanup(tmp.cleanup)
        self.assertEqual(info["pages"], 4)
        for rel in ("index.html", "signals/index.html", "claims/index.html", "404.html", "_headers",
                    "assets/site.css"):
            self.assertTrue((out / rel).is_file(), rel)
        html = (out / "index.html").read_text()
        self.assertIn('data-kpi="signals">5<', html)  # 3 + 2 inside the 7-day window; the October row is not
        self.assertIn('data-kpi="claims">1<', html)
        self.assertIn("midday:2026-11-10", html)
        self.assertIn("AI summary", html)

    def test_b2_denylist_withholds_rows_and_masks_domains(self):
        snap = snapshot()
        self.assertEqual([r["post_id"] for r in snap.signals], ["1", "3"])
        self.assertEqual([r["claim_id"] for r in snap.claims], ["c1"])
        self.assertEqual(snap.withheld, {"signals": 1, "claims": 1, "domains": 1})
        tmp, out, _ = build(snap)
        self.addCleanup(tmp.cleanup)
        every = "".join(p.read_text() for p in out.rglob("*.html")).casefold()
        self.assertNotIn("acme", every)
        self.assertIn(bi_site.WITHHELD, every)

    def test_b2_empty_denylist_fails_closed(self):
        with self.assertRaises(ValueError):
            bi_site.load_snapshot(object(), ())

    def test_b3_escaping_links_and_thai(self):
        tmp, out, _ = build(snapshot())
        self.addCleanup(tmp.cleanup)
        html = (out / "signals" / "index.html").read_text()
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertNotIn("javascript:", html)
        self.assertIn('<span lang="th">ข่าวไทย</span>', html)
        self.assertIn('href="https://x.com/a/status/1"', html)

    def test_b3_long_text_is_cut(self):
        snap = snapshot(signals=[signal("9", "word " * 200)])
        tmp, out, _ = build(snap)
        self.addCleanup(tmp.cleanup)
        html = (out / "signals" / "index.html").read_text()
        self.assertIn("…", html)
        self.assertNotIn("word " * 60, html)

    def test_b4_no_script_csp(self):
        tmp, out, _ = build(snapshot())
        self.addCleanup(tmp.cleanup)
        headers = (out / "_headers").read_text()
        self.assertIn("script-src 'none'", headers)
        self.assertFalse((out / "assets" / "filter.js").exists())
        self.assertNotIn("<script", "".join(p.read_text() for p in out.rglob("*.html")))

    def test_b5_same_rows_same_mark(self):
        a = build(snapshot())
        b = build(snapshot())
        self.addCleanup(a[0].cleanup)
        self.addCleanup(b[0].cleanup)
        self.assertEqual(a[2]["mark"], b[2]["mark"])
        c = build(snapshot(digests=[]))
        self.addCleanup(c[0].cleanup)
        self.assertNotEqual(a[2]["mark"], c[2]["mark"])

    def test_empty_snapshot_renders(self):
        snap = bi_site.screen(bi_site.Snapshot(None, None, [], [], [], []), DENY)
        tmp, out, _ = build(snap)
        self.addCleanup(tmp.cleanup)
        self.assertIn("No signals yet", (out / "index.html").read_text())


class FakePages:
    def __init__(self, live="", missing=None):
        self.live, self.missing, self.calls, self.uploaded, self.deployment = live, missing, [], [], None

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append(path)
        ok = lambda result: httpx.Response(200, json={"success": True, "errors": [], "result": result})  # noqa: E731
        if path.endswith("/upload-token"):
            return ok({"jwt": "jwt"})
        if path.endswith("/check-missing"):
            hashes = json.loads(request.content)["hashes"]
            return ok(hashes if self.missing is None else [h for h in hashes if h in self.missing])
        if path.endswith("/assets/upload"):
            self.uploaded += [i["key"] for i in json.loads(request.content)]
            return ok(None)
        if path.endswith("/upsert-hashes"):
            return ok(True)
        if path.endswith("/deployments"):
            self.deployment = request.content
            return ok({"id": "dep1"})
        if path.endswith("/projects/x-insights"):
            return ok({"canonical_deployment": {"deployment_trigger": {"metadata": {"commit_message": self.live}}}})
        return httpx.Response(404, json={"success": False, "errors": [{"code": 8000007}]})


class DeployTests(unittest.TestCase):
    def setUp(self):
        self.tmp, self.out, self.info = build(snapshot())
        self.addCleanup(self.tmp.cleanup)

    def run_deploy(self, fake, **kw):
        with httpx.Client(transport=httpx.MockTransport(fake)) as http:
            return pages_deploy.deploy(self.out, account="acct", project="x-insights", token="tok",
                                       mark=self.info["mark"], http=http, **kw)

    def test_b5_live_mark_skips_upload(self):
        fake = FakePages(live=self.info["mark"])
        self.assertEqual(self.run_deploy(fake)["status"], "unchanged")
        self.assertEqual(len(fake.calls), 1)

    def test_b5_latest_in_progress_deployment_counts_unless_failed(self):
        def project(stage):
            def handler(request):
                dep = {"deployment_trigger": {"metadata": {"commit_message": self.info["mark"]}},
                       "latest_stage": {"status": stage}}
                return httpx.Response(200, json={"success": True, "result": {
                    "latest_deployment": dep, "canonical_deployment": {"deployment_trigger": {"metadata": {}}}}})
            return handler
        with httpx.Client(transport=httpx.MockTransport(project("active"))) as http:
            self.assertEqual(pages_deploy.live_mark(http, "a", "x-insights", "t"), self.info["mark"])
        with httpx.Client(transport=httpx.MockTransport(project("failure"))) as http:
            self.assertEqual(pages_deploy.live_mark(http, "a", "x-insights", "t"), "")

    def test_uploads_missing_assets_and_creates_deployment(self):
        assets, special = pages_deploy.collect(self.out)
        one = next(iter(assets.values()))[0]
        fake = FakePages(missing={one})
        res = self.run_deploy(fake)
        self.assertEqual(res["status"], "deployed")
        self.assertEqual(res["deployment_id"], "dep1")
        self.assertEqual(fake.uploaded, [one])
        body = fake.deployment.decode()
        self.assertIn('"/index.html"', body)
        self.assertIn(self.info["mark"], body)
        self.assertIn('name="_headers"', body)
        self.assertNotIn("/_headers", json.dumps(sorted(assets)))
        self.assertEqual(set(special), {"_headers"})

    def test_api_error_raises_without_token_text(self):
        def fail(request):
            return httpx.Response(403, json={"success": False, "errors": [{"code": 10000, "message": "tok"}]})
        with self.assertRaises(pages_deploy.PagesError) as cm:
            self.run_deploy(fail)
        self.assertNotIn("tok", str(cm.exception).replace("upload-token", ""))
        self.assertIn("403", str(cm.exception))


class ScheduledHookTests(unittest.TestCase):
    def run_bi(self, env):
        buf = io.StringIO()
        with redirect_stdout(buf):
            scheduled._bi(Config(env=env))
        return [json.loads(line) for line in buf.getvalue().splitlines() if line]

    def test_b6_off_by_default(self):
        self.assertEqual(self.run_bi({}), [])

    def test_b6_failure_is_logged_not_raised(self):
        logs = self.run_bi({"X_LAKE_BI": "build"})  # no denylist → fails closed, logged
        self.assertEqual(logs[0]["status"], "error")
        self.assertEqual(logs[0]["event"], "bi")

    def test_b6_build_logs_counts_only(self):
        with mock.patch.object(cur, "connect", return_value=mock.MagicMock()), \
                mock.patch.object(bi_site, "load_snapshot", return_value=snapshot()):
            logs = self.run_bi({"X_LAKE_BI": "build", "CLIENT_DENYLIST": "acme corp", "X_LAKE_DB_URL": "x"})
        self.assertEqual(logs[0]["status"], "built")
        self.assertTrue(logs[0]["mark"].startswith("x-insights@"))
        self.assertNotIn("grant open", json.dumps(logs))


@unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class BiPostgresTests(unittest.TestCase):
    def setUp(self):
        self.conn = cur.connect(DB)
        with self.conn.cursor() as c:
            c.execute("drop schema if exists x_intel cascade")
        self.conn.commit()
        ensure_inbox(self.conn)
        from x_lakehouse.marts import ensure_marts
        ensure_marts(self.conn)
        rows = [("1", "2026-11-10 09:00", "D5 Technology", "urgent_candidate", "server outage"),
                ("2", "2026-11-10 10:00", "D2 Economy & Money", "digest", "grant open"),
                ("3", "2026-11-09 11:00", "D2 Economy & Money", "none", "deal with Acme Corp"),
                ("4", "2026-10-01 06:00", "D1 World & Politics", "none", "old")]
        with self.conn.cursor() as c:
            for pid, ts, dom, alert, text in rows:
                c.execute("insert into x_intel.x_signals (post_id, collected_at, domain, alert_tier, post_text, post_url) "
                          "values (%s, %s::timestamp at time zone 'Asia/Bangkok', %s, %s, %s, %s)",
                          (pid, ts, dom, alert, text, f"https://x.com/a/status/{pid}"))
        self.conn.commit()

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def test_load_snapshot_windows_rank_and_screen(self):
        snap = bi_site.load_snapshot(self.conn, DENY)
        self.assertEqual(snap.as_of, date(2026, 11, 10))
        self.assertEqual([r["post_id"] for r in snap.signals], ["1", "2"])  # 4 is out of the window, 3 withheld
        self.assertEqual(snap.withheld["signals"], 1)
        self.assertNotIn(date(2026, 10, 1), {r["day"] for r in snap.daily})
        tmp, out, _ = build(snap)
        self.addCleanup(tmp.cleanup)
        self.assertIn('data-kpi="urgent">1<', (out / "index.html").read_text())


if __name__ == "__main__":
    unittest.main()
