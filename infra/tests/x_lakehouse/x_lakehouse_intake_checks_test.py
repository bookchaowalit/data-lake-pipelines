#!/usr/bin/env python3
"""Intake sanity checks (intake_checks.py): sender clock, missing or thin bot runs, zone-less stamps.

Offline: rows are tuples as the inbox query returns them. Spec: docs/specs/x-intake-checks.md.
"""

from __future__ import annotations

import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import intake_checks as ic  # noqa: E402
from x_lakehouse import scheduled  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402

ICT = timezone(timedelta(hours=7))


def at(day: int, h: int, m: int = 0) -> datetime:
    return datetime(2026, 11, day, h, m, tzinfo=ICT)


def feed(run: str, n: int, stamp: str, received: datetime, dataset: str = "x_feed") -> list[tuple]:
    return [(run, dataset, stamp, received)] * n


class FutureStampTests(unittest.TestCase):
    def test_a_run_stamped_after_its_arrival_is_reported_once_per_run_and_dataset(self):
        # run1125e reached the inbox at 19:01 ICT and stamped 21:00 (UTC+9 clock).
        rows = feed("run1125e", 166, "2026-11-25 21:00", at(25, 19, 1))
        out = ic.evaluate(rows, at(25, 22, 22))
        self.assertEqual(out["issues"][0], "intake run1125e x_feed: 166 rows stamped up to 119 min after arrival "
                                           "(sender clock?)")
        self.assertEqual(out["future_rows"], 166)

    def test_stamps_before_arrival_or_within_the_tolerance_pass(self):
        rows = (feed("run1125m", 85, "2026-11-25 08:00", at(25, 8, 2))
                + feed("run1125m", 3, "2026-11-25T08:05:00+07:00", at(25, 8, 2), dataset="x_news"))
        self.assertEqual(ic.evaluate(rows, at(25, 8, 23))["future_rows"], 0)

    def test_rows_received_before_the_lookback_are_not_reported_again(self):
        rows = feed("run1125e", 166, "2026-11-25 21:00", at(25, 19, 1))
        self.assertEqual(ic.evaluate(rows, at(26, 8, 23))["future_rows"], 0)

    def test_zone_offsets_are_honoured(self):
        rows = feed("r", 1, "2026-11-25T12:00:00Z", at(25, 19, 1))  # 19:00 ICT: before arrival
        self.assertEqual(ic.evaluate(rows, at(25, 22, 0))["future_rows"], 0)


class MissingRunTests(unittest.TestCase):
    def test_the_thin_morning_run_of_2026_11_24_is_reported(self):
        rows = feed("run1124m", 84, "2026-11-24 07:58", at(24, 7, 59))
        out = ic.evaluate(rows, at(24, 8, 24))
        self.assertIn("intake 2026-11-24 06:00-08:15: x_feed 84 rows (< 100)", out["issues"])

    def test_a_missing_evening_run_is_reported_at_the_night_cron(self):
        out = ic.evaluate([], at(25, 22, 22))
        self.assertEqual(out["issues"], ["intake 2026-11-25 18:00-20:30: no x_feed run"])

    def test_a_normal_day_has_no_issue_and_each_window_is_checked_once(self):
        rows = feed("run1125m", 386, "2026-11-25 08:00", at(25, 8, 2))
        self.assertEqual(ic.evaluate(rows, at(25, 8, 23))["issues"], [])
        self.assertEqual(ic.evaluate([], at(25, 16, 26))["windows"], [])  # 08:15 closed 8 h ago: not again
        self.assertEqual([w["window"] for w in ic.evaluate([], at(26, 8, 23))["windows"]], ["2026-11-26 06:00-08:15"])

    def test_windows_and_floor_come_from_the_config(self):
        cfg = Config(env={"X_LAKE_INTAKE_WINDOWS": "07:00-07:30", "X_LAKE_INTAKE_MIN_ROWS": "10"})
        s = ic.Settings.from_cfg(cfg)
        out = ic.evaluate(feed("r", 9, "2026-11-25 07:10", at(25, 7, 10)), at(25, 8, 0), s)
        self.assertEqual(out["issues"], ["intake 2026-11-25 07:00-07:30: x_feed 9 rows (< 10)"])


class NaiveStampTests(unittest.TestCase):
    def test_zone_less_stamps_are_counted_not_alerted(self):
        rows = (feed("r", 100, "2026-11-25 08:00", at(25, 8, 2))
                + feed("r", 1, "2026-11-25T07:40:26+07:00", at(25, 8, 2), dataset="x_news"))
        out = ic.evaluate(rows, at(25, 12, 0))
        self.assertEqual(out["naive"], {"x_feed": 100})
        self.assertEqual(out["issues"], [])


class CronHookTests(unittest.TestCase):
    def test_issues_reach_the_ops_alert(self):
        lines = scheduled._issues([{"event": "intake_checks", "issues": ["intake r x_feed: 1 rows stamped up to 9 "
                                                                         "min after arrival (sender clock?)"]}])
        self.assertEqual(lines, ["intake r x_feed: 1 rows stamped up to 9 min after arrival (sender clock?)"])

    def test_a_failing_check_is_one_error_line_and_off_skips_it(self):
        self.assertEqual(scheduled._issues([{"event": "intake_checks", "status": "error", "error": "OperationalError: x"}]),
                         ["intake_checks: error OperationalError"])

        class Boom:
            def cursor(self):
                raise RuntimeError("no db")

        scheduled._EVENTS.clear()
        scheduled._intake_checks(Config(env={}), Boom(), at(25, 8, 23))
        self.assertEqual(scheduled._EVENTS[-1]["status"], "error")
        scheduled._EVENTS.clear()
        scheduled._intake_checks(Config(env={"X_LAKE_INTAKE_CHECKS": "off"}), Boom(), at(25, 8, 23))
        self.assertEqual(scheduled._EVENTS, [])


if __name__ == "__main__":
    unittest.main()
