#!/usr/bin/env python3
"""Offline tests for the hosted registry-export cron wrapper."""

from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
import base64
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake import scheduled_registry_export as job  # noqa: E402
from data_lake.registry_lake_export import DATASETS  # noqa: E402

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
        self.pages: list[str] = []

    def publish(self, text, ref, env):
        self.pages.append(text)
        return "committed"

    def run_job(self, env: dict[str, str], fetch=fixture_fetch, argv=None) -> int:
        return job.run(argv or [], env=env, fetch=fetch, heartbeat=self.beats.append, publish=self.publish)

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
            self.assertEqual(len(list((lake / "control" / "manifests").rglob("*.json"))), len(DATASETS))
            self.assertEqual(len(self.pages), 1)
            self.assertIn("# Solo Empire — Generated System Landscape", self.pages[0])

    def test_fetches_only_the_five_source_files(self):
        seen: list[tuple[str, str]] = []

        def recording_fetch(repo, path, ref, token):
            seen.append((repo, path))
            return fixture_fetch(repo, path, ref, token)

        self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://unused"}, recording_fetch, ["--dry-run"])
        self.assertEqual(sorted(seen), [
            ("architecture-docs", "systems/ai-data/metadata-catalog/catalog.yaml"),
            ("architecture-docs", "systems/ai-data/metadata-catalog/lineage.yaml"),
            ("solo-empire", "repository-catalog/registries/business-capabilities.yaml"),
            ("solo-empire", "repository-catalog/registries/data-sources.yaml"),
            ("solo-empire", "repository-catalog/registries/online-systems.yaml"),
        ])

    def test_dry_run_sends_no_heartbeat(self):
        code = self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://unused"}, argv=["--dry-run"])
        self.assertEqual(code, 0)
        self.assertEqual(self.beats, [])
        self.assertEqual(self.pages, [])

    def test_publish_failure_fails_the_run(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")

        def broken_publish(text, ref, env):
            raise job.FetchError("commit HTTP 403")

        with tempfile.TemporaryDirectory() as temp_dir:
            env = {"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": (Path(temp_dir) / "lake").as_uri()}
            code = job.run([], env=env, fetch=fixture_fetch, heartbeat=self.beats.append, publish=broken_publish)
        self.assertEqual(code, 1)
        self.assertEqual(self.beats, [False])

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


class Response:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body or {}

    def json(self):
        return self._body


def existing(text):
    return Response(200, {"sha": "abc", "content": base64.b64encode(text.encode()).decode()})


class GithubPublishTests(unittest.TestCase):
    ENV = {"GITHUB_TOKEN": "read", "LANDSCAPE_COMMIT_TOKEN": "write"}

    def test_unchanged_page_is_not_committed(self):
        with mock.patch.object(job.httpx, "get", return_value=existing("same")), \
                mock.patch.object(job.httpx, "put") as put:
            self.assertEqual(job.github_publish("same", "main", self.ENV), "unchanged")
        put.assert_not_called()

    def test_changed_page_commits_with_sha_and_write_token(self):
        with mock.patch.object(job.httpx, "get", return_value=existing("old")), \
                mock.patch.object(job.httpx, "put", return_value=Response(200)) as put:
            self.assertEqual(job.github_publish("new", "main", self.ENV), "committed")
        payload = put.call_args.kwargs["json"]
        self.assertEqual(payload["sha"], "abc")
        self.assertEqual(payload["branch"], "main")
        self.assertEqual(base64.b64decode(payload["content"]).decode(), "new")
        self.assertEqual(put.call_args.kwargs["headers"]["Authorization"], "Bearer write")

    def test_new_file_commits_without_sha(self):
        with mock.patch.object(job.httpx, "get", return_value=Response(404)), \
                mock.patch.object(job.httpx, "put", return_value=Response(201)) as put:
            self.assertEqual(job.github_publish("new", "main", self.ENV), "committed")
        self.assertNotIn("sha", put.call_args.kwargs["json"])

    def test_no_write_token_skips(self):
        with mock.patch.object(job.httpx, "get", return_value=existing("old")), \
                mock.patch.object(job.httpx, "put") as put:
            self.assertEqual(job.github_publish("new", "main", {"GITHUB_TOKEN": "read"}), "skipped")
        put.assert_not_called()

    def test_commit_error_raises(self):
        with mock.patch.object(job.httpx, "get", return_value=existing("old")), \
                mock.patch.object(job.httpx, "put", return_value=Response(409)):
            with self.assertRaisesRegex(job.FetchError, "HTTP 409"):
                job.github_publish("new", "main", self.ENV)


class ImageFileSetTests(unittest.TestCase):
    def test_dry_run_works_with_only_the_files_the_dockerfile_copies(self):
        dockerfile = ROOT / "deploy" / "registry-export" / "Dockerfile"
        copies = [line.split()[1:3] for line in dockerfile.read_text(encoding="utf-8").splitlines()
                  if line.startswith("COPY infra/scripts/")]
        self.assertTrue(copies)
        with tempfile.TemporaryDirectory() as temp_dir:
            app = Path(temp_dir)
            for source, target in copies:
                destination = app / target
                if (ROOT / source).is_dir():
                    shutil.copytree(ROOT / source, destination)
                else:
                    destination.mkdir(parents=True, exist_ok=True)
                    shutil.copy(ROOT / source, destination)
            result = subprocess.run(
                [sys.executable, str(app / "infra/scripts/data_lake/scheduled_registry_export.py"), "--dry-run",
                 "--registries", str(FIXTURE_DIRS["registries"]), "--catalog", str(FIXTURE_DIRS["catalog"])],
                capture_output=True, text=True, cwd=temp_dir,
            )
            self.assertEqual(result.returncode, 0, result.stderr[-1000:])
            self.assertIn('"status": "dry-run"', result.stdout)


if __name__ == "__main__":
    unittest.main()
