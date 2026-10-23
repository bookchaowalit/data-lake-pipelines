#!/usr/bin/env python3
"""Opt-in Sentry for the scheduled capture runners: off without a DSN, allowlist scrubber."""

from __future__ import annotations

import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import observability  # noqa: E402
from data_lake.observability import run_with_reporting, scrub_event  # noqa: E402


class FakeSdk(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("sentry_sdk")
        self.init_kwargs: dict[str, object] = {}
        self.messages: list[tuple[str, str]] = []
        self.exceptions: list[BaseException] = []
        self.tags: dict[str, str] = {}

    def init(self, **kwargs: object) -> None:
        self.init_kwargs = kwargs

    def set_tag(self, key: str, value: str) -> None:
        self.tags[key] = value

    def capture_message(self, message: str, level: str = "info") -> None:
        self.messages.append((message, level))

    def capture_exception(self, error: BaseException) -> None:
        self.exceptions.append(error)

    def flush(self, timeout: float = 0) -> None:
        return None


class ScrubEventTests(unittest.TestCase):
    def test_keeps_exception_type_and_frames_only(self) -> None:
        event = {
            "level": "error",
            "tags": {"runner": "jobs"},
            "exception": {
                "values": [
                    {
                        "type": "HTTPError",
                        "value": "GET https://example.com/jobs?token=abc failed for 'somchai@example.com'",
                        "stacktrace": {"frames": [{"filename": "run.py", "function": "main", "lineno": 7, "vars": {"k": "v"}}]},
                    }
                ]
            },
            "user": {"ip_address": "203.0.113.9"},
            "request": {"url": "https://example.com", "headers": {"Authorization": "Bearer x"}},
            "breadcrumbs": {"values": [{"message": "secret"}]},
            "extra": {"payload": "private"},
        }
        clean = scrub_event(event)
        value = clean["exception"]["values"][0]
        self.assertEqual(value["type"], "HTTPError")
        self.assertNotIn("value", value)
        self.assertEqual(value["stacktrace"]["frames"], [{"filename": "run.py", "function": "main", "lineno": 7}])
        for dropped in ("user", "request", "breadcrumbs", "extra"):
            self.assertNotIn(dropped, clean)
        flat = repr(clean)
        for leaked in ("token=abc", "somchai", "203.0.113.9", "Bearer", "private"):
            self.assertNotIn(leaked, flat)

    def test_message_passes_only_when_allowlisted(self) -> None:
        allowed = scrub_event({"message": "capture run failed", "level": "error"})
        other = scrub_event({"message": "failed to fetch 'https://x.test/?key=1'", "level": "error"})
        logentry = scrub_event({"logentry": {"message": "user somchai", "params": ["a"]}})
        self.assertEqual(allowed["message"], "capture run failed")
        self.assertEqual(other["message"], "[redacted]")
        self.assertEqual(logentry["message"], "[redacted]")
        self.assertNotIn("logentry", logentry)


class RunWithReportingTests(unittest.TestCase):
    def test_off_without_dsn_returns_exit_code_untouched(self) -> None:
        fake = FakeSdk()
        with patch.dict(os.environ, {"SENTRY_DSN": ""}), patch.dict(sys.modules, {"sentry_sdk": fake}):
            code = run_with_reporting("jobs", lambda: 1)
        self.assertEqual(code, 1)
        self.assertEqual(fake.init_kwargs, {})
        self.assertEqual(fake.messages, [])

    def test_failed_run_sends_one_static_message(self) -> None:
        fake = FakeSdk()
        with patch.dict(os.environ, {"SENTRY_DSN": "https://public@example.invalid/1"}), patch.dict(
            sys.modules, {"sentry_sdk": fake}
        ):
            code = run_with_reporting("market", lambda: 1)
        self.assertEqual(code, 1)
        self.assertIs(fake.init_kwargs["before_send"], scrub_event)
        self.assertEqual(fake.init_kwargs["send_default_pii"], False)
        self.assertEqual(fake.messages, [("capture run failed", "error")])
        self.assertEqual(fake.tags, {"runner": "market", "exit_code": "1"})

    def test_success_and_lock_contention_are_not_reported(self) -> None:
        fake = FakeSdk()
        with patch.dict(os.environ, {"SENTRY_DSN": "https://public@example.invalid/1"}), patch.dict(
            sys.modules, {"sentry_sdk": fake}
        ):
            self.assertEqual(run_with_reporting("seo", lambda: 0), 0)
            self.assertEqual(run_with_reporting("seo", lambda: 2), 2)
        self.assertEqual(fake.messages, [])

    def test_uncaught_exception_is_captured_and_reraised(self) -> None:
        fake = FakeSdk()

        def boom() -> int:
            raise RuntimeError("disk /home/user/lake full")

        with patch.dict(os.environ, {"SENTRY_DSN": "https://public@example.invalid/1"}), patch.dict(
            sys.modules, {"sentry_sdk": fake}
        ):
            with self.assertRaises(RuntimeError):
                run_with_reporting("news", boom)
        self.assertEqual(len(fake.exceptions), 1)

    def test_missing_sdk_is_inert(self) -> None:
        with patch.dict(os.environ, {"SENTRY_DSN": "https://public@example.invalid/1"}), patch.object(
            observability, "import_module", side_effect=ImportError
        ):
            self.assertEqual(run_with_reporting("jobs", lambda: 1), 1)


if __name__ == "__main__":
    unittest.main()
