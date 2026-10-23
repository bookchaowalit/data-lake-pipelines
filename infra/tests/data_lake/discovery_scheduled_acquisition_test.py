"""Offline contract checks for the local Discovery acquisition runner."""

from __future__ import annotations

import importlib.util
import logging
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "infra/scripts/discovery/run_scheduled_acquisition.py"
SPEC = importlib.util.spec_from_file_location("run_scheduled_acquisition", RUNNER)
assert SPEC and SPEC.loader
scheduled = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = scheduled
SPEC.loader.exec_module(scheduled)


class DiscoveryScheduledAcquisitionTests(unittest.TestCase):
    def test_summary_keeps_counts_and_failed_source_ids_without_payloads(self) -> None:
        payload = {
            "contract": "discovery.portfolio.v1",
            "command": "acquire",
            "partial": True,
            "sources": [
                {"source_id": "first-source", "status": "captured", "count": 4},
                {"source_id": "second-source", "status": "captured", "count": 2},
                {
                    "source_id": "third-source",
                    "status": "unavailable",
                    "error": "PRIVATE_RESPONSE_BODY_SHOULD_NOT_BE_LOGGED",
                },
            ],
        }

        summary = scheduled.summarize_result(payload)

        self.assertIn("sources=3", summary)
        self.assertIn("captured=2", summary)
        self.assertIn("unavailable=1", summary)
        self.assertIn("third-source", summary)
        self.assertNotIn("PRIVATE_RESPONSE_BODY_SHOULD_NOT_BE_LOGGED", summary)

    def test_summary_handles_empty_source_set(self) -> None:
        self.assertIn("sources=0", scheduled.summarize_result({"sources": []}))

    def test_log_timestamps_are_utc_as_the_z_suffix_claims(self) -> None:
        # The format string ends the timestamp with "Z"; the converter must be set on the
        # Formatter (a handler attribute is ignored), or local time is written as UTC.
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            scheduled, "LOG_FILE", Path(tmp) / "logs" / "scheduled.log"
        ):
            logger = scheduled.configure_logger()
            try:
                formatter = logger.handlers[0].formatter
                assert formatter is not None
                record = logging.LogRecord("t", logging.INFO, __file__, 1, "msg", None, None)
                record.created = 0.0
                record.msecs = 0.0
                self.assertTrue(formatter.format(record).startswith("1970-01-01 00:00:00,000Z"))
            finally:
                for handler in list(logger.handlers):
                    handler.close()
                logger.handlers.clear()


if __name__ == "__main__":
    unittest.main()
