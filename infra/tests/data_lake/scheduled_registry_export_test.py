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

import httpx  # noqa: E402

from data_lake import knowledge_store  # noqa: E402
from data_lake import scheduled_registry_export as job  # noqa: E402
from data_lake.registry_lake_export import DATASETS  # noqa: E402

FIXTURES = ROOT / "infra" / "tests" / "fixtures" / "registry_lake_export"
FIXTURE_DIRS = {"registries": FIXTURES / "registries", "catalog": FIXTURES / "metadata-catalog"}


def fixture_fetch(repo: str, path: str, ref: str, token: str) -> bytes:
    for name, (source_repo, directory, _files) in job.SOURCES.items():
        if repo == source_repo and path.startswith(directory + "/"):
            return (FIXTURE_DIRS[name] / path.rsplit("/", 1)[1]).read_bytes()
    raise AssertionError(f"unexpected fetch {repo}:{path}")


STORE_ENV = {"EMPIRE_KNOWLEDGE_API": "https://store.test", "SOLO_WEB_PUBLIC_ORIGIN": "https://issuer.test",
             "KNOWLEDGE_STORE_KEY": "pem", "KNOWLEDGE_STORE_SUBJECT": "owner"}


def fixture_store(env):
    def fetch(path):
        return fixture_fetch(job.STORE, path, "main", "")
    return fetch


class ScheduledRegistryExportTests(unittest.TestCase):
    def setUp(self):
        self.beats: list[bool] = []
        self.pages: list[str] = []

    def publish(self, text, ref, env):
        self.pages.append(text)
        return "committed"

    def run_job(self, env: dict[str, str], fetch=fixture_fetch, argv=None, store=fixture_store) -> int:
        return job.run(argv or [], env=env, fetch=fetch, heartbeat=self.beats.append, publish=self.publish,
                       fetch_store=store)

    def test_exports_and_reports_ok(self):
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow not installed")
        with tempfile.TemporaryDirectory() as temp_dir:
            lake = Path(temp_dir) / "lake"
            code = self.run_job({**STORE_ENV, "GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": lake.as_uri()})
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

        def recording_store(env):
            def fetch(path):
                seen.append((job.STORE, path))
                return fixture_fetch(job.STORE, path, "main", "")
            return fetch

        self.run_job({**STORE_ENV, "GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://unused"}, recording_fetch,
                     ["--dry-run"], store=recording_store)
        self.assertEqual(sorted(seen), [
            ("solo-empire", "repository-catalog/registries/business-capabilities.yaml"),
            ("solo-empire", "repository-catalog/registries/data-sources.yaml"),
            ("solo-empire", "repository-catalog/registries/online-systems.yaml"),
            ("store:architecture-docs", "systems/ai-data/metadata-catalog/catalog.yaml"),
            ("store:architecture-docs", "systems/ai-data/metadata-catalog/lineage.yaml"),
        ])
        self.assertTrue(seen)

    def test_dry_run_sends_no_heartbeat(self):
        code = self.run_job({**STORE_ENV, "GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://unused"}, argv=["--dry-run"])
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
            env = {**STORE_ENV, "GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": (Path(temp_dir) / "lake").as_uri()}
            code = job.run([], env=env, fetch=fixture_fetch, heartbeat=self.beats.append, publish=broken_publish,
                           fetch_store=fixture_store)
        self.assertEqual(code, 1)
        self.assertEqual(self.beats, [False])

    def test_missing_settings_fail_with_heartbeat(self):
        self.assertEqual(self.run_job({**STORE_ENV, "SOLO_EMPIRE_DATA_LAKE_URI": "s3://x"}), 1)
        self.assertEqual(self.run_job({**STORE_ENV, "GITHUB_TOKEN": "t"}), 1)
        # Without the store settings the catalog source and the landscape page are unreachable.
        self.assertEqual(self.run_job({"GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://x"}), 1)
        self.assertEqual(self.beats, [False, False, False])

    def test_store_fetch_error_fails_with_heartbeat(self):
        def broken_store(env):
            def fetch(path):
                raise job.FetchError("store:architecture-docs not found")
            return fetch

        code = self.run_job({**STORE_ENV, "GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://x"}, store=broken_store)
        self.assertEqual(code, 1)
        self.assertEqual(self.beats, [False])

    def test_fetch_error_fails_with_heartbeat(self):
        def broken_fetch(repo, path, ref, token):
            raise job.FetchError("HTTP 404")

        code = self.run_job({**STORE_ENV, "GITHUB_TOKEN": "t", "SOLO_EMPIRE_DATA_LAKE_URI": "s3://x"}, broken_fetch)
        self.assertEqual(code, 1)
        self.assertEqual(self.beats, [False])

    def test_local_fixture_dirs_skip_github(self):
        code = self.run_job(
            {},
            fetch=lambda *args: self.fail("fetch called"),
            argv=["--dry-run", "--registries", str(FIXTURE_DIRS["registries"]), "--catalog", str(FIXTURE_DIRS["catalog"])],
        )
        self.assertEqual(code, 0)


class FakeStore:
    """The store API's document endpoint, in memory: GET / PUT with if_version."""

    def __init__(self, docs=None, *, fail=None):
        self.docs = dict(docs or {})  # path -> (bytes, version)
        self.fail = fail
        self.calls = []

    def __call__(self, method, url, *, params, json=None, headers=None, timeout=None):
        self.calls.append((method, params["path"], headers["Authorization"].split()[0]))
        request = httpx.Request(method, url)
        if self.fail:
            return httpx.Response(self.fail, json={"error": {"code": "boom"}}, request=request)
        path = params["path"]
        if method == "GET":
            if path not in self.docs:
                return httpx.Response(404, json={"error": {"code": "not_found"}}, request=request)
            body, version = self.docs[path]
            return httpx.Response(200, request=request, json={"document": {
                "encoding": "utf-8", "content": body.decode(), "version": version, "sha256": "x"}})
        current = self.docs.get(path, (b"", 0))[1]
        if json["if_version"] != current:
            return httpx.Response(409, json={"error": {"code": "version_conflict"}}, request=request)
        self.docs[path] = (base64.b64decode(json["content_base64"]), current + 1)
        return httpx.Response(200, request=request, json={"document": {"version": current + 1, "sha256": "x"}})


def rsa_env():
    try:
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        import jwt  # noqa: F401
    except ImportError:
        raise unittest.SkipTest("extra 'store' (pyjwt[crypto]) not installed")
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption()).decode()
    return {**STORE_ENV, "KNOWLEDGE_STORE_KEY": pem}, key.public_key()


class StorePublishTests(unittest.TestCase):
    def setUp(self):
        self.env, self.public_key = rsa_env()

    def publish(self, store, text):
        with mock.patch.object(knowledge_store.httpx, "request", store):
            return job.store_publish(text, "main", self.env)

    def test_unchanged_page_is_not_written(self):
        store = FakeStore({job.LANDSCAPE_PATH: (b"same", 3)})
        self.assertEqual(self.publish(store, "same"), "unchanged")
        self.assertEqual([c[0] for c in store.calls], ["GET"])

    def test_changed_page_is_written_with_if_version(self):
        store = FakeStore({job.LANDSCAPE_PATH: (b"old", 3)})
        self.assertEqual(self.publish(store, "new"), "written")
        self.assertEqual(store.docs[job.LANDSCAPE_PATH], (b"new", 4))

    def test_new_page_is_written_with_version_zero(self):
        store = FakeStore()
        self.assertEqual(self.publish(store, "new"), "written")
        self.assertEqual(store.docs[job.LANDSCAPE_PATH], (b"new", 1))

    def test_a_concurrent_write_is_refused_not_overwritten(self):
        store = FakeStore({job.LANDSCAPE_PATH: (b"old", 3)})
        real = store.__call__

        def racing(method, url, **kwargs):
            if method == "PUT":
                store.docs[job.LANDSCAPE_PATH] = (b"someone else", 4)
            return real(method, url, **kwargs)

        with self.assertRaisesRegex(job.FetchError, "409 version_conflict"):
            self.publish(racing, "new")
        self.assertEqual(store.docs[job.LANDSCAPE_PATH], (b"someone else", 4))

    def test_store_errors_name_status_and_code_only(self):
        with self.assertRaisesRegex(job.FetchError, "HTTP 503 boom"):
            self.publish(FakeStore(fail=503), "new")

    def test_tokens_are_short_lived_scoped_rs256(self):
        import jwt

        token = knowledge_store.mint(self.env, knowledge_store.WRITE_SCOPE, now=1000)
        claims = jwt.decode(token, self.public_key, algorithms=["RS256"], audience="knowledge-api",
                            options={"verify_exp": False, "verify_nbf": False, "verify_iat": False})
        self.assertEqual((claims["iss"], claims["sub"], claims["scope"]),
                         ("https://issuer.test", "owner", "knowledge:write"))
        self.assertEqual(claims["exp"] - claims["iat"], 60)

    def test_fetch_reads_from_space_architecture_docs(self):
        store = FakeStore({"systems/ai-data/metadata-catalog/catalog.yaml": (b"x: 1\n", 2)})
        with mock.patch.object(knowledge_store.httpx, "request", store):
            fetch = job.store_fetch(self.env)
            self.assertEqual(fetch("systems/ai-data/metadata-catalog/catalog.yaml"), b"x: 1\n")
            with self.assertRaisesRegex(job.FetchError, "not found"):
                fetch("systems/ai-data/metadata-catalog/lineage.yaml")


class KnowledgeStoreConfigTests(unittest.TestCase):
    def test_missing_or_unsafe_settings_are_refused(self):
        with self.assertRaisesRegex(knowledge_store.StoreError, "missing KNOWLEDGE_STORE_KEY"):
            knowledge_store.KnowledgeStore({k: v for k, v in STORE_ENV.items() if k != "KNOWLEDGE_STORE_KEY"})
        with self.assertRaisesRegex(knowledge_store.StoreError, "https"):
            knowledge_store.KnowledgeStore({**STORE_ENV, "EMPIRE_KNOWLEDGE_API": "http://store.test"})

    def test_a_bad_key_never_shows_its_text(self):
        try:
            import jwt  # noqa: F401
        except ImportError:
            self.skipTest("extra 'store' not installed")
        with self.assertRaises(knowledge_store.StoreError) as caught:
            knowledge_store.mint({**STORE_ENV, "KNOWLEDGE_STORE_KEY": "not-a-secret-key"}, "knowledge:read")
        self.assertNotIn("not-a-secret-key", str(caught.exception))


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
