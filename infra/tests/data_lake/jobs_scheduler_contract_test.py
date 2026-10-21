#!/usr/bin/env python3
"""Offline contract checks for the gated jobs scheduler."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scheduler_contract_utils import require_bash


ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "infra/scripts/data_lake/run_jobs_capture.py"
SETUP = ROOT / "infra/scripts/data_lake/setup_jobs_cron.sh"


class JobsSchedulerContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.runner = RUNNER.read_text(encoding="utf-8")
        cls.setup = SETUP.read_text(encoding="utf-8")

    def test_runner_uses_only_bounded_free_api_slice(self) -> None:
        for value in (
            "scrape_job_postings.py",
            "filter_job_matches.py",
            "remotive,remoteok-api,himalayas,jobicy",
            "python developer",
            "ingest_job_captures.py",
            "job_data_readiness.py",
            "FREE_ONLY",
            "FIRECRAWL_API_KEY",
            "TELEGRAM_BOT_TOKEN",
            "jobs-feeds.lock",
            "dependencies_missing",
        ):
            self.assertIn(value, self.runner)
        self.assertIn("SOLO_EMPIRE_JOB_SOURCE_COLLECTION_APPROVED", self.setup)
        self.assertIn("book-scraping-scheduler", self.setup)

    def test_plan_is_read_only_and_shows_install_gate(self) -> None:
        env = os.environ.copy()
        env["SOLO_EMPIRE_DATA_LAKE_URI"] = "/tmp/jobs-lake-plan"
        env.pop("SOLO_EMPIRE_JOB_SOURCE_COLLECTION_APPROVED", None)
        completed = subprocess.run(
            [require_bash(), SETUP.as_posix(), "plan"],
            cwd=ROOT,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("Jobs scheduler plan (not installed)", completed.stdout)
        self.assertIn("every 6h", completed.stdout)
        self.assertIn("SOLO_EMPIRE_JOB_SOURCE_COLLECTION_APPROVED=true", completed.stdout)
        self.assertIn("run_jobs_capture.py", completed.stdout)

    def test_install_requires_explicit_approval_and_lake(self) -> None:
        env = os.environ.copy()
        env.pop("SOLO_EMPIRE_DATA_LAKE_URI", None)
        env["SOLO_EMPIRE_JOB_SOURCE_COLLECTION_APPROVED"] = "true"
        with tempfile.TemporaryDirectory(prefix="jobs-cron-test-") as home:
            env["HOME"] = home
            completed = subprocess.run(
                [require_bash(), SETUP.as_posix(), "install"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("set SOLO_EMPIRE_DATA_LAKE_URI", completed.stderr)

    def test_install_fails_without_operator_approval(self) -> None:
        env = os.environ.copy()
        env.pop("SOLO_EMPIRE_JOB_SOURCE_COLLECTION_APPROVED", None)
        with tempfile.TemporaryDirectory(prefix="jobs-cron-test-") as home, tempfile.TemporaryDirectory(
            prefix="jobs-lake-test-"
        ) as lake:
            env["HOME"] = home
            env["SOLO_EMPIRE_DATA_LAKE_URI"] = lake
            completed = subprocess.run(
                [require_bash(), SETUP.as_posix(), "install"],
                cwd=ROOT,
                env=env,
                capture_output=True,
                text=True,
            )
        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("SOLO_EMPIRE_JOB_SOURCE_COLLECTION_APPROVED", completed.stderr)

    def test_runner_help_is_local(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(RUNNER), "--help"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("--lake-uri", completed.stdout)
        self.assertIn("--attempts", completed.stdout)


if __name__ == "__main__":
    unittest.main()
