#!/usr/bin/env python3
"""Offline tests for GitHub App installation tokens (data_lake/github_app.py) and their use in
the registry-export cron. HTTP is mocked; keys are throwaway RSA keys generated per test run."""

from __future__ import annotations

import json
import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(ROOT / "infra" / "tests" / "data_lake"))

import httpx  # noqa: E402

from data_lake import github_app  # noqa: E402
from data_lake import scheduled_registry_export as job  # noqa: E402

try:  # PyJWT[crypto] is the "store" extra; without it the tests skip (CI requires it)
    import jwt
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
except ImportError:
    if os.environ.get("SOLO_EMPIRE_CI") == "1":
        raise RuntimeError("extra 'store' (pyjwt[crypto]) not installed; SOLO_EMPIRE_CI=1 requires it") from None
    HAVE_CRYPTO = False
    KEY = PUBLIC = None
    PKCS1 = PKCS8 = ""
else:
    HAVE_CRYPTO = True
    KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    PUBLIC = KEY.public_key()
    PKCS1 = KEY.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.TraditionalOpenSSL,
                              serialization.NoEncryption()).decode()
    PKCS8 = KEY.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                              serialization.NoEncryption()).decode()
SKIP = unittest.skipUnless(HAVE_CRYPTO, "extra 'store' (pyjwt[crypto]) not installed")
NOW = 1_800_000_000.0
SECRET_TOKEN = "installation-token-value"  # placeholder, deliberately not a ghs_ shape


def response(status: int, body: object) -> httpx.Response:
    return httpx.Response(status, json=body, request=httpx.Request("GET", "https://api.github.com"))


class FakeGitHub:
    """Records calls; answers the installation lookups and the token mint."""

    def __init__(self, *, org: int = 200, user: int = 200, mint: int = 201, expires: str = "2027-01-15T08:00:00Z"):
        self.calls: list[tuple[str, str, dict, object]] = []
        self.org, self.user, self.mint, self.expires = org, user, mint, expires

    def __call__(self, method, url, *, headers, json=None, timeout=None):
        self.calls.append((method, url, headers, json))
        if url.endswith("/orgs/bookchaowalit/installation"):
            return response(self.org, {"id": 11} if self.org == 200 else {"message": "Not Found"})
        if url.endswith("/users/bookchaowalit/installation"):
            return response(self.user, {"id": 22} if self.user == 200 else {"message": "Not Found"})
        if url.endswith("/access_tokens"):
            body = {"token": SECRET_TOKEN, "expires_at": self.expires} if self.mint == 201 else {"message": "no"}
            return response(self.mint, body)
        raise AssertionError(f"unexpected {method} {url}")


def app(fake: FakeGitHub, key: str = PKCS1, clock=lambda: NOW) -> github_app.GitHubApp:
    env = {"GITHUB_APP_ID": "123456", "GITHUB_APP_PRIVATE_KEY": key}
    return github_app.GitHubApp(env, user_agent="registry-export/1", request=fake, clock=clock)


@SKIP
class GitHubAppTests(unittest.TestCase):
    def test_jwt_claims(self):
        token = github_app.app_jwt("123456", KEY, NOW)
        claims = jwt.decode(token, PUBLIC, algorithms=["RS256"], options={"verify_exp": False, "verify_iat": False})
        self.assertEqual(claims, {"iat": int(NOW) - 60, "exp": int(NOW) + 540, "iss": "123456"})
        self.assertEqual(jwt.get_unverified_header(token)["alg"], "RS256")

    def test_loads_pkcs1_pkcs8_and_literal_newlines(self):
        for pem in (PKCS1, PKCS8, PKCS1.replace("\n", "\\n"), PKCS8.replace("\n", "\\n")):
            key = github_app.load_private_key(pem)
            self.assertEqual(key.public_key().public_numbers(), PUBLIC.public_numbers())

    def test_bad_key_error_names_no_key_text(self):
        with self.assertRaises(github_app.GitHubAppError) as caught:
            # Built from parts so the fixture is not a PEM-shaped literal for secret scanners.
            armour = "-" * 5
            github_app.load_private_key(f"{armour}BEGIN {'PRIVATE'} KEY{armour}\nnotakey\n{armour}END {'PRIVATE'} KEY{armour}")
        self.assertNotIn("notakey", str(caught.exception))
        self.assertIn("GITHUB_APP_PRIVATE_KEY", str(caught.exception))

    def test_configured_states(self):
        self.assertFalse(github_app.app_configured({}))
        self.assertFalse(github_app.app_configured({"GITHUB_APP_ID": " ", "GITHUB_APP_PRIVATE_KEY": ""}))
        self.assertTrue(github_app.app_configured({"GITHUB_APP_ID": "1", "GITHUB_APP_PRIVATE_KEY": PKCS1}))
        with self.assertRaises(github_app.GitHubAppError) as caught:
            github_app.app_configured({"GITHUB_APP_ID": "1"})
        self.assertIn("GITHUB_APP_PRIVATE_KEY", str(caught.exception))

    def test_non_numeric_app_id_is_refused(self):
        with self.assertRaises(github_app.GitHubAppError):
            github_app.GitHubApp({"GITHUB_APP_ID": "Iv1.abc", "GITHUB_APP_PRIVATE_KEY": PKCS1}, user_agent="x")

    def test_org_installation_and_down_scoped_body(self):
        fake = FakeGitHub()
        value = app(fake).token("bookchaowalit", permissions={"contents": "read"}, repositories=["bookchaowalit-empire"])
        self.assertEqual(value, SECRET_TOKEN)
        self.assertEqual([(m, u.rsplit("api.github.com", 1)[1]) for m, u, _h, _b in fake.calls], [
            ("GET", "/orgs/bookchaowalit/installation"),
            ("POST", "/app/installations/11/access_tokens"),
        ])
        self.assertEqual(fake.calls[1][3], {"permissions": {"contents": "read"}, "repositories": ["bookchaowalit-empire"]})
        headers = fake.calls[1][2]
        self.assertEqual(headers["Accept"], "application/vnd.github+json")
        self.assertEqual(headers["X-GitHub-Api-Version"], "2022-11-28")
        self.assertEqual(headers["User-Agent"], "registry-export/1")
        claims = jwt.decode(headers["Authorization"].removeprefix("Bearer "), PUBLIC, algorithms=["RS256"],
                            options={"verify_exp": False, "verify_iat": False})
        self.assertEqual(claims["iss"], "123456")

    def test_user_installation_fallback(self):
        fake = FakeGitHub(org=404)
        app(fake, PKCS8).token("bookchaowalit", permissions={"contents": "read"})
        self.assertTrue(fake.calls[1][1].endswith("/users/bookchaowalit/installation"))
        self.assertTrue(fake.calls[2][1].endswith("/app/installations/22/access_tokens"))
        self.assertEqual(fake.calls[2][3], {"permissions": {"contents": "read"}})

    def test_caches_installation_and_token_then_re_mints_near_expiry(self):
        now = [github_app.dt.datetime(2027, 1, 15, 7, 0, tzinfo=github_app.dt.timezone.utc).timestamp()]
        fake = FakeGitHub()
        client = app(fake, clock=lambda: now[0])
        scope = {"permissions": {"contents": "read"}, "repositories": ["bookchaowalit-empire"]}
        client.token("bookchaowalit", **scope)
        client.token("bookchaowalit", **scope)
        self.assertEqual(len(fake.calls), 2)  # one lookup, one mint
        now[0] += 56 * 60  # four minutes left on the one-hour token
        client.token("bookchaowalit", **scope)
        self.assertEqual([m for m, *_ in fake.calls], ["GET", "POST", "POST"])

    def test_errors_carry_status_and_owner_only(self):
        cases = [FakeGitHub(org=404, user=404), FakeGitHub(org=403), FakeGitHub(mint=422)]
        for fake in cases:
            with self.assertRaises(github_app.GitHubAppError) as caught:
                app(fake).token("bookchaowalit", permissions={"contents": "read"})
            text = str(caught.exception)
            self.assertIn("bookchaowalit", text)
            self.assertIn("HTTP", text)
            for secret in (SECRET_TOKEN, "BEGIN", "eyJ"):
                self.assertNotIn(secret, text)


@SKIP
class RegistryExportAppTests(unittest.TestCase):
    def run_job(self, env, *, mint, fetch=None):
        from scheduled_registry_export_test import STORE_ENV, fixture_fetch, fixture_store

        self.beats: list[bool] = []
        self.tokens: list[str] = []

        def recording_fetch(repo, path, ref, token):
            self.tokens.append(token)
            return fixture_fetch(repo, path, ref, token)

        return job.run(["--dry-run"], env={**STORE_ENV, "SOLO_EMPIRE_DATA_LAKE_URI": "s3://unused", **env},
                       fetch=fetch or recording_fetch, heartbeat=self.beats.append,
                       publish=lambda *a: "unused", fetch_store=fixture_store, mint_token=mint)

    def test_read_scope_is_contents_read_on_the_registry_repository(self):
        self.assertEqual(job.READ_PERMISSIONS, {"contents": "read"})
        self.assertEqual(job.READ_REPOSITORIES, ("bookchaowalit-empire",))

    def test_app_vars_win_over_the_pat(self):
        minted: list[dict] = []
        env = {"GITHUB_APP_ID": "1", "GITHUB_APP_PRIVATE_KEY": PKCS1, "GITHUB_TOKEN": "pat"}
        code = self.run_job(env, mint=lambda e: minted.append(dict(e)) or "app-token")
        self.assertEqual(code, 0)
        self.assertEqual(len(minted), 1)
        self.assertEqual(set(self.tokens), {"app-token"})

    def test_app_without_pat_is_enough(self):
        env = {"GITHUB_APP_ID": "1", "GITHUB_APP_PRIVATE_KEY": PKCS1}
        self.assertEqual(self.run_job(env, mint=lambda e: "app-token"), 0)

    def test_pat_fallback_when_app_unset(self):
        def no_mint(env):
            raise AssertionError("must not mint without the App vars")

        self.assertEqual(self.run_job({"GITHUB_TOKEN": "pat"}, mint=no_mint), 0)
        self.assertEqual(set(self.tokens), {"pat"})

    def test_half_configured_app_fails_with_heartbeat(self):
        from scheduled_registry_export_test import STORE_ENV

        beats: list[bool] = []
        env = {**STORE_ENV, "SOLO_EMPIRE_DATA_LAKE_URI": "s3://unused", "GITHUB_APP_ID": "1", "GITHUB_TOKEN": "pat"}
        code = job.run([], env=env, heartbeat=beats.append, mint_token=lambda e: self.fail("no mint when half set"))
        self.assertEqual(code, 1)
        self.assertEqual(beats, [False])

    def test_local_files_ignore_a_half_configured_app(self):
        from scheduled_registry_export_test import FIXTURE_DIRS

        argv = ["--dry-run", "--registries", str(FIXTURE_DIRS["registries"]), "--catalog", str(FIXTURE_DIRS["catalog"])]
        code = job.run(argv, env={"GITHUB_APP_ID": "1"}, heartbeat=lambda ok: None,
                       mint_token=lambda e: self.fail("no mint for local files"))
        self.assertEqual(code, 0)

    def test_mint_failure_fails_without_leaking(self):
        def failing(env):
            raise github_app.GitHubAppError("GitHub App token for bookchaowalit: HTTP 403", status=403)

        import contextlib
        import io

        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = self.run_job({"GITHUB_APP_ID": "1", "GITHUB_APP_PRIVATE_KEY": PKCS1}, mint=failing)
        self.assertEqual(code, 1)
        report = json.loads(out.getvalue().strip().splitlines()[-1])
        self.assertEqual(report["error"], "GitHubAppError")
        self.assertNotIn("BEGIN", out.getvalue())

    def test_child_processes_do_not_inherit_the_secrets(self):
        import subprocess
        from unittest import mock

        fake_environ = {"GITHUB_APP_ID": "1", "GITHUB_APP_PRIVATE_KEY": PKCS1, "GITHUB_TOKEN": "pat",
                        "KNOWLEDGE_STORE_KEY": "store-pem", "CATALOG_EXPORT_TOKEN": "cat",
                        "AWS_ACCESS_KEY_ID": "r2", "PATH": os.environ.get("PATH", "")}
        with mock.patch.dict(os.environ, fake_environ, clear=True):
            env = job.private_env(os.environ)
            child = subprocess.run([sys.executable, "-c", "import json, os; print(json.dumps(sorted(os.environ)))"],
                                   capture_output=True, text=True, check=True)
        self.assertEqual(env["GITHUB_APP_PRIVATE_KEY"], PKCS1)  # run() still gets everything
        inherited = set(json.loads(child.stdout))
        self.assertFalse(inherited & set(job.CHILD_DENYLIST))
        self.assertIn("AWS_ACCESS_KEY_ID", inherited)  # the aws CLI keeps its own credentials

    def test_default_mint_uses_the_app_end_to_end(self):
        fake = FakeGitHub()
        original = github_app.httpx.request
        github_app.httpx.request = fake  # type: ignore[assignment]
        try:
            token = job.app_read_token({"GITHUB_APP_ID": "123456", "GITHUB_APP_PRIVATE_KEY": PKCS8})
        finally:
            github_app.httpx.request = original  # type: ignore[assignment]
        self.assertEqual(token, SECRET_TOKEN)
        self.assertEqual(fake.calls[-1][3], {"permissions": {"contents": "read"}, "repositories": ["bookchaowalit-empire"]})


if __name__ == "__main__":
    unittest.main()
