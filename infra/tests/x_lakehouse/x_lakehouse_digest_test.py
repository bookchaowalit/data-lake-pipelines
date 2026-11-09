#!/usr/bin/env python3
"""Tests for the X digest (spec docs/specs/x-digest.md, D1..D7).

Postgres tests run only with X_LAKE_TEST_DB_URL (throwaway database; they drop schema x_intel).
The gateway and Telegram are fakes (httpx.MockTransport): nothing leaves the machine.
"""

from __future__ import annotations

import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import curated as cur  # noqa: E402
from x_lakehouse import digest  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402
from x_lakehouse.inbox import ensure_inbox  # noqa: E402

DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
ICT = digest.TZ


def at(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=ICT)


class WindowTests(unittest.TestCase):
    def test_d1_kind_and_window_follow_the_run_hour(self):
        cases = {"2026-11-10 08:20": ("morning", "2026-11-09 22:00"),
                 "2026-11-10 16:20": ("midday", "2026-11-10 08:00"),
                 "2026-11-10 22:20": ("daily", "2026-11-10 00:00")}
        for run, (kind, start) in cases.items():
            [w] = digest.windows(at(run))
            self.assertEqual((w.kind, w.start.astimezone(ICT).strftime("%Y-%m-%d %H:%M")), (kind, start), run)
            self.assertEqual(w.digest_id, f"{kind}:{run[:10]}")

    def test_d1_monday_morning_adds_the_weekly_digest(self):
        kinds = [w.kind for w in digest.windows(at("2026-11-09 08:20"))]  # 2026-11-09 is a Monday
        self.assertEqual(kinds, ["morning", "weekly"])
        self.assertEqual([w.kind for w in digest.windows(at("2026-11-09 16:20"))], ["midday"])


class RenderTests(unittest.TestCase):
    def _facts(self, **kw):
        w = digest.windows(at("2026-11-10 16:20"))[0]
        f = digest.Facts(w, totals={"signals": 3, "alerts": 1, "urgent": 0, "opportunities": 1, "strong_sources": 1})
        f.top = [{"domain": "D5 <Tech>", "text": "<script>alert(1)</script> & co", "link": "https://x.com/a/status/1",
                  "alert": "none"}]
        for k, v in kw.items():
            setattr(f, k, v)
        return f

    def test_d6_x_text_is_escaped(self):
        text = digest.render(self._facts(), None)
        self.assertNotIn("<script>", text)
        self.assertIn("&lt;script&gt;", text)
        self.assertIn("D5 &lt;Tech&gt;", text)

    def test_d6_long_messages_split_under_the_telegram_limit(self):
        text = "\n".join(f"• line {i} " + "ก" * 90 for i in range(200))
        parts = digest.split_message(text)
        self.assertGreater(len(parts), 1)
        self.assertTrue(all(len(p) <= digest.MAX_MESSAGE for p in parts))
        self.assertEqual("\n".join(parts), text)

    def test_d5_answer_links_must_come_from_the_facts(self):
        f = self._facts()
        ok = {"headline_th": "h", "summary_th": ["a"], "alerts": [{"title": "t", "why": "w",
                                                                    "link": "https://x.com/a/status/1"}]}
        self.assertEqual(digest.validate_answer(ok, f, ["zzz"])["alerts"][0]["link"], "https://x.com/a/status/1")
        bad = ok | {"alerts": [{"title": "t", "why": "w", "link": "https://evil.example/x"}]}
        with self.assertRaises(digest.DigestError):
            digest.validate_answer(bad, f, ["zzz"])
        with self.assertRaises(digest.DigestError):
            digest.validate_answer(ok | {"summary_th": ["ดู https://evil.example/y"]}, f, ["zzz"])
        with self.assertRaises(digest.DigestError):
            digest.validate_answer({"summary_th": []}, f, ["zzz"])
        # Links or bare hosts hidden in any model-written field are refused (Telegram links bare hosts).
        for poisoned in (ok | {"alerts": [{"title": "ดู evil.example/login", "why": "w", "link": ""}]},
                         ok | {"opportunities": [{"title": "t", "why": "ไปที่ https://evil.example", "link": ""}]},
                         ok | {"watch": ["phish.example.co"]},
                         ok | {"headline_th": "สรุป bit.ly/abc"}):
            with self.assertRaises(digest.DigestError, msg=str(poisoned)):
                digest.validate_answer(poisoned, f, ["zzz"])
        # Hosts of the facts' own links, version numbers and Thai text stay allowed.
        fine = ok | {"summary_th": ["โพสต์บน x.com เพิ่มขึ้น 1.5 เท่า", "Python 3.12 ออกแล้ว"]}
        self.assertEqual(digest.validate_answer(fine, f, ["zzz"])["summary_th"][0], "โพสต์บน x.com เพิ่มขึ้น 1.5 เท่า")

    def test_review_malformed_answer_fields_raise_digest_error(self):
        f = self._facts()
        base = {"headline_th": "h", "summary_th": ["a"]}
        for bad in (base | {"watch": 2}, base | {"opportunities": 2}, base | {"alerts": [{"link": 5}]},
                    base | {"summary_th": [3]}, base | {"headline_th": None}, ["not", "an", "object"]):
            with self.assertRaises(digest.DigestError, msg=str(bad)):
                digest.validate_answer(bad, f, ["zzz"])

    def test_review_hosts_are_checked_after_normalising_and_with_their_path(self):
        f = self._facts()
        ok = {"headline_th": "h", "summary_th": ["a"]}
        for poisoned in ("ดู evil\uff0eexample", "x.com/evil", "ไป 10.0.0.1 ด่วน", "ดู ตัวอย่าง.ไทย", "เว็บ(evil.example)"):
            with self.assertRaises(digest.DigestError, msg=poisoned):
                digest.validate_answer(ok | {"summary_th": [poisoned]}, f, ["zzz"])
        self.assertTrue(digest.validate_answer(ok | {"summary_th": ["x.com/a/status/1 ok"]}, f, ["zzz"]))

    def test_review_free_text_cannot_become_a_link(self):
        text = digest.render(self._facts(top=[{"domain": "D5", "text": "visit evil.example now",
                                                  "link": "https://x.com/a/status/1", "alert": "none"}]), None)
        self.assertNotIn("evil.example", text)
        self.assertIn("evil.\u2060example", text)
        self.assertIn('href="https://x.com/a/status/1"', text)  # real links stay intact

    def test_review_overlong_lines_split_into_valid_html(self):
        import re as _re
        line = "<b>" + "&amp;" * 3000 + "</b>"
        parts = digest.split_message("head\n" + line + "\ntail")
        self.assertTrue(all(len(p) <= digest.MAX_MESSAGE for p in parts))
        for p in parts:
            self.assertEqual(len(_re.findall(r"<b>", p)), len(_re.findall(r"</b>", p)))
            self.assertIsNone(_re.search(r"&[a-z]*$", p))  # no entity cut in half

    def test_d4_denylist_is_required_and_matches_robustly(self):
        with self.assertRaises(digest.DigestError):
            digest.load_denylist(Config(env={}))
        terms = digest.load_denylist(Config(env={"CLIENT_DENYLIST": "# comment\nAcme Corp\n"}))
        self.assertTrue(digest.contains_term("news about ACME​corp today", terms))
        self.assertFalse(digest.contains_term("nothing here", terms))


class FakeServices:
    """Gateway + Telegram behind one MockTransport; records what was sent."""

    def __init__(self, answer=None, gateway_status=200):
        self.answer, self.gateway_status = answer, gateway_status
        self.prompts, self.messages = [], []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == "api.telegram.org":
            self.messages.append(json.loads(request.content)["text"])
            return httpx.Response(200, json={"ok": True, "result": {"message_id": len(self.messages)}})
        body = json.loads(request.content)
        self.prompts.append(body["messages"][1]["content"])
        if self.gateway_status != 200:
            return httpx.Response(self.gateway_status, json={"code": "monthly_budget_exceeded"})
        return httpx.Response(200, json={"content": json.dumps(self.answer, ensure_ascii=False),
                                         "usage": {"input_tokens": 900, "output_tokens": 300},
                                         "estimated_cost_usd": 0.001})

    def client(self):
        return httpx.Client(transport=httpx.MockTransport(self))


@unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class DigestPostgresTests(unittest.TestCase):
    NOW = at("2026-11-10 16:20")

    def setUp(self):
        self.conn = cur.connect(DB)
        with self.conn.cursor() as c:
            c.execute("drop schema if exists x_intel cascade")
        self.conn.commit()
        ensure_inbox(self.conn)
        self.cfg = Config(env={"CLIENT_DENYLIST": "acme corp", "AI_GATEWAY_TOKEN": "t",
                               "TELEGRAM_NOTI_BOT_TOKEN": "b", "TELEGRAM_NOTI_CHAT_ID": "1"})
        rows = [
            ("1", "2026-11-10 09:00", "D5 Technology", "urgent_candidate", 2, "T1", "Warning", "server outage"),
            ("2", "2026-11-10 10:00", "D2 Economy & Money", "digest", 4, "T3", "Opportunity", "grant open"),
            ("3", "2026-11-10 11:00", "D2 Economy & Money", "none", 0, "T4", "News", "plain news"),
            ("4", "2026-11-10 12:00", "D2 Economy & Money", "none", 5, "T2", "Opportunity", "deal with Acme Corp"),
            ("5", "2026-11-10 06:00", "D1 World & Politics", "none", 1, "T4", "News", "before the window"),
        ]
        with self.conn.cursor() as c:
            for pid, ts, dom, alert, score, cred, kind, text in rows:
                c.execute("insert into x_intel.x_signals (post_id, collected_at, domain, alert_tier, opportunity_score, "
                          "cred_tier, signal_type, post_text, post_url, author_handle, likes) "
                          "values (%s, %s::timestamp at time zone 'Asia/Bangkok', %s, %s, %s, %s, %s, %s, %s, 'a', 3)",
                          (pid, ts, dom, alert, score, cred, kind, text, f"https://x.com/a/status/{pid}"))
        self.conn.commit()

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def run_digest(self, svc, mode="send", now=None):
        with svc.client() as http:
            return digest.run(self.conn, self.cfg, mode=mode, now=now or self.NOW, http=http)

    def test_d2_priority_and_hourly_mart(self):
        from x_lakehouse.marts import ensure_marts
        ensure_marts(self.conn)
        with self.conn.cursor() as c:
            c.execute("select post_id from x_intel.mart_x_signal_priority order by priority desc")
            order = [r[0] for r in c.fetchall()]
            c.execute("select sum(signals), sum(alerts), sum(urgent) from x_intel.mart_x_domain_hourly")
            totals = c.fetchone()
        self.conn.commit()
        self.assertEqual(order[:2], ["1", "2"])  # urgent first, then the digest alert
        self.assertEqual(totals, (5, 2, 1))

    def test_review_urgent_always_outranks_a_strong_digest_row(self):
        from x_lakehouse.marts import ensure_marts
        ensure_marts(self.conn)
        with self.conn.cursor() as c:
            c.execute("delete from x_intel.x_signals")
            c.execute("insert into x_intel.x_signals (post_id, collected_at, alert_tier, opportunity_score, cred_tier, "
                      "signal_type) values ('u', now(), 'urgent_candidate', 1, 'T4', 'News'), "
                      "('d', now(), 'digest', 5, 'T1', 'Opportunity')")
            c.execute("select post_id from x_intel.mart_x_signal_priority order by priority desc")
            self.assertEqual([r[0] for r in c.fetchall()], ["u", "d"])
        self.conn.commit()

    def test_review_every_outgoing_field_is_screened(self):
        with self.conn.cursor() as c:
            c.execute("update x_intel.x_signals set suggested_action = 'Contact ACME Corp' where post_id = '2'")
        self.conn.commit()
        svc = FakeServices({"headline_th": "h", "summary_th": ["a"]})
        [rep] = self.run_digest(svc)
        self.assertEqual(rep["withheld"], 2)
        self.assertNotIn("acme", svc.prompts[0].lower())

    def test_review_a_digest_id_is_reserved_before_sending(self):
        w = digest.windows(self.NOW)[0]
        from x_lakehouse.marts import ensure_marts
        ensure_marts(self.conn)
        other = cur.connect(DB)
        try:
            self.assertTrue(digest._reserve(other, w))  # another process is sending it
        finally:
            other.close()
        svc = FakeServices({"headline_th": "h", "summary_th": ["a"]})
        [rep] = self.run_digest(svc)
        self.assertEqual(rep["status"], "already_sent_or_sending")
        self.assertEqual(svc.messages, [])
        with self.conn.cursor() as c:  # a claim older than 15 minutes counts as a crashed run
            c.execute("update x_intel.digest_log set updated_at = now() - interval '20 minutes'")
        self.conn.commit()
        self.assertEqual(self.run_digest(svc)[0]["status"], "sent")

    def test_d4_matching_rows_never_reach_the_gateway_and_d3_no_double_send(self):
        answer = {"headline_th": "สรุป", "summary_th": ["ข้อ 1"],
                  "alerts": [{"title": "outage", "why": "ด่วน", "link": "https://x.com/a/status/1"}],
                  "opportunities": [], "watch": []}
        svc = FakeServices(answer)
        [rep] = self.run_digest(svc)
        self.assertEqual((rep["status"], rep["ai"], rep["signals"], rep["withheld"]), ("sent", True, 4, 1))
        self.assertNotIn("Acme", svc.prompts[0])
        self.assertNotIn("before the window", svc.prompts[0])
        self.assertIn("ซ่อน 1 รายการ", svc.messages[0])
        [again] = self.run_digest(svc)
        self.assertEqual(again["status"], "already_sent_or_sending")
        self.assertEqual(len(svc.messages), 1)
        with self.conn.cursor() as c:
            c.execute("select status, signals, sent_messages, tokens_in from x_intel.digest_log")
            self.assertEqual(c.fetchall(), [("sent", 4, 1, 900)])

    def test_d4_a_client_term_in_the_answer_falls_back_and_never_leaks(self):
        svc = FakeServices({"headline_th": "about acme corp", "summary_th": ["x"]})
        [rep] = self.run_digest(svc)
        self.assertEqual((rep["status"], rep["ai"]), ("sent", False))
        self.assertIn("client term", rep["fallback_reason"])
        self.assertNotIn("acme", svc.messages[0].lower())

    def test_d5_gateway_failure_sends_a_facts_only_message(self):
        svc = FakeServices(gateway_status=429)
        [rep] = self.run_digest(svc)
        self.assertEqual((rep["status"], rep["ai"]), ("sent", False))
        self.assertIn("ข้อมูลดิบ", svc.messages[0])
        self.assertIn("https://x.com/a/status/1", svc.messages[0])

    def test_d7_dry_and_off_never_call_out(self):
        svc = FakeServices({"headline_th": "h", "summary_th": []})
        [rep] = self.run_digest(svc, mode="dry")
        self.assertEqual((rep["status"], rep["signals"]), ("dry", 4))
        self.assertEqual(self.run_digest(svc, mode="off"), [])
        self.assertEqual((svc.prompts, svc.messages), ([], []))

    def test_empty_window_sends_a_short_note_without_the_gateway(self):
        svc = FakeServices({"headline_th": "h", "summary_th": []})
        [rep] = self.run_digest(svc, now=at("2026-11-11 16:20"))
        self.assertEqual((rep["status"], rep["signals"]), ("sent", 0))
        self.assertEqual(svc.prompts, [])
        self.assertIn("ยังไม่มีสัญญาณใหม่", svc.messages[0])

    def test_scheduled_run_logs_digest_and_never_fails_on_it(self):
        import io
        from unittest.mock import patch
        from x_lakehouse import scheduled
        out = io.StringIO()
        env = {"X_LAKE_DIGEST": "send", "X_LAKE_DB_URL": DB}  # no denylist: the digest must fail closed
        with patch.dict(os.environ, env), patch.object(scheduled, "_serve_counts", return_value={}), \
                patch.object(scheduled, "_heartbeat") as beat, patch("sys.stdout", out):
            code = scheduled._finish(True, datetime.now(timezone.utc), scheduled.Config())
        self.assertEqual(code, 0)
        beat.assert_called_once_with(True)
        ev = [json.loads(line) for line in out.getvalue().splitlines() if '"digest"' in line][0]
        self.assertEqual(ev["status"], "error")
        self.assertIn("denylist", ev["error"])


if __name__ == "__main__":
    unittest.main()
