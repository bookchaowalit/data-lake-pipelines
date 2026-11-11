#!/usr/bin/env python3
"""Tests for the cron's hook problem alert (scheduled._issues / _ops_alert). Telegram is mocked."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import scheduled  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402

TG = {"TELEGRAM_NOTI_BOT_TOKEN": "b", "TELEGRAM_NOTI_CHAT_ID": "1"}


class IssueTests(unittest.TestCase):
    def test_every_hook_problem_becomes_one_short_line(self):
        events = [
            {"event": "drain", "rows": 5},
            {"event": "digest", "mode": "send", "reports": [
                {"digest_id": "midday:2026-11-10", "status": "sent", "ai": False,
                 "fallback_reason": "answer names a host that is not in the facts"},
                {"digest_id": "daily:2026-11-10", "status": "unknown"},
                {"digest_id": "morning:2026-11-11", "status": "sent", "ai": True, "fallback_reason": None},
                {"digest_id": "weekly:2026-11-09", "status": "already_sent_or_sending"}]},
            {"event": "weekly", "week": "2026-W45", "triage": "error: gateway http 502"},
            {"event": "gold", "ok": False, "errors": {"03_topic_count_spikes": "IOException"}},
            {"event": "bi", "mode": "deploy", "status": "error", "error": "PagesError"},
            {"event": "digest", "mode": "send", "status": "error",
             "error": "OperationalError: connection to server at db.secret.host failed"},
        ]
        self.assertEqual(scheduled._issues(events), [
            "digest midday:2026-11-10: facts-only (answer names a host that is not in the facts)",
            "digest daily:2026-11-10: unknown",
            "weekly 2026-W45: triage error: gateway http 502",
            "gold: failed ['03_topic_count_spikes']",
            "bi: error PagesError",
            "digest: error OperationalError",  # the type only, never the library text
        ])

    def test_a_clean_run_has_no_issues(self):
        events = [{"event": "digest", "reports": [{"digest_id": "m", "status": "sent", "ai": True}]},
                  {"event": "weekly", "week": "W", "analyses": "already_written", "triage": "already_written"},
                  {"event": "gold", "ok": True, "errors": {}}, {"event": "bi", "status": "deployed"}]
        self.assertEqual(scheduled._issues(events), [])


class AlertTests(unittest.TestCase):
    def setUp(self):
        scheduled._EVENTS.clear()
        self.addCleanup(scheduled._EVENTS.clear)

    def test_one_message_per_run_with_the_issue_lines(self):
        scheduled._EVENTS.append({"event": "bi", "status": "error", "error": "PagesError"})
        with mock.patch.object(scheduled.httpx, "post", return_value=mock.Mock(status_code=200)) as post, \
                mock.patch("builtins.print"):
            scheduled._ops_alert(Config(env=TG))
        self.assertEqual(post.call_count, 1)
        self.assertIn("• bi: error PagesError", post.call_args.kwargs["json"]["text"])

    def test_nothing_is_sent_without_issues_when_off_or_without_telegram(self):
        with mock.patch.object(scheduled.httpx, "post") as post, mock.patch("builtins.print"):
            scheduled._ops_alert(Config(env=TG))  # no issues
            scheduled._EVENTS.append({"event": "bi", "status": "error", "error": "X"})
            scheduled._ops_alert(Config(env=TG | {"X_LAKE_OPS_ALERT": "off"}))
            scheduled._ops_alert(Config(env={}))
        post.assert_not_called()
        self.assertEqual(scheduled._EVENTS[-1], {"event": "ops_alert", "status": "skipped", "issues": 1})

    def test_a_telegram_failure_is_logged_without_the_url(self):
        scheduled._EVENTS.append({"event": "bi", "status": "error", "error": "X"})
        with mock.patch.object(scheduled.httpx, "post", side_effect=scheduled.httpx.ConnectError("x")), \
                mock.patch("builtins.print"):
            scheduled._ops_alert(Config(env=TG))
        self.assertEqual(scheduled._EVENTS[-1], {"event": "ops_alert", "status": "error", "error": "ConnectError",
                                                 "issues": 1})


if __name__ == "__main__":
    unittest.main()
