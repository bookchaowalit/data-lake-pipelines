#!/usr/bin/env python3
"""Offline tests for the hosted registry-export cron wrapper."""

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import scheduled_registry_export as job  # noqa: E402

FIXTURES = ROOT / "infra" / "tests" / "fixtures" / "registry_lake_export"
FIXTURE_DIRS = {"registries": FIXTURES / "registries", "catalog": FIXTURES / "metadata-catalog"}


def fixture_fetch(repo: str, path: str, ref: str, token: str) -> bytes:
    for name, (source_repo, directory, _files) in job.SOURCES.items():
        if repo == source_repo and path.startswith(directory + "/"):
            return (FIXTURE_DIRS[name] / path.rsplit("/", 1)[1]).read_bytes()
    raise AssertionError(f"unexpected fetch {repo}:{path}")


class ScheduledRegistryExportTests(unittest.TestCase):
    def setUp(self):
        self.beats: list[bool] = []

    def run_job(self, env: dict[str, str], fetch=fixture_fetch, argv=None) -> int:
        return job.run(argv or [], env=env, fetch=fetch, heartbeat=self.beats.append)

    def test_exports_and_reports_ok(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            lake = Path(temp_dir) / "lake"
            code = self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": lake.as_uri()})
            self.assertEqual(code, 0)
            self.assertEqual(self.beats, [True])
            self.assertEqual(len(list((lake / "control" / "manifests").rglob("*.json"))), 4)

    def test_fetches_only_the_four_source_files(self):
        seen: list[tuple[str, str]] = []

        def recording_fetch(repo, path, ref, token):
            seen.append((repo, path))
            return fixture_fetch(repo, path, ref, token)

        self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://unused"}, recording_fetch, ["--dry-run"])
        self.assertEqual(sorted(seen), [
            ("architecture-docs", "systems/ai-data/metadata-catalog/catalog.yaml"),
            ("architecture-docs", "systems/ai-data/metadata-catalog/lineage.yaml"),
            ("solo-empire", "repository-catalog/registries/data-sources.yaml"),
            ("solo-empire", "repository-catalog/registries/online-systems.yaml"),
        ])

    def test_dry_run_sends_no_heartbeat(self):
        code = self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://unused"}, argv=["--dry-run"])
        self.assertEqual(code, 0)
        self.assertEqual(self.beats, [])

    def test_missing_settings_fail_with_heartbeat(self):
        self.assertEqual(self.run_job({"SOLO_EMPIRE_DATA_LAKE_URI": "s3://x"}), 1)
        self.assertEqual(self.run_job({"GITHUB_TOKEN": "t"}), 1)
        self.assertEqual(self.beats, [False, False])

    def test_fetch_error_fails_with_heartbeat(self):
        def broken_fetch(repo, path, ref, token):
            raise job.FetchError("HTTP 404")

        code = self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://x"}, broken_fetch)
        self.assertEqual(code, 1)
        self.assertEqual(self.beats, [False])

    def test_local_fixture_dirs_skip_github(self):
        code = self.run_job(
            {},
            fetch=lambda *args: self.fail("fetch called"),
            argv=["--dry-run", "--registries", str(FIXTURE_DIRS["registries"]), "--catalog", str(FIXTURE_DIRS["catalog"])],
        )
        self.assertEqual(code, 0)


if __name__ == "__main__":
    unittest.main()
