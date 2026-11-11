#!/usr/bin/env python3
"""Offline tests for the GitHub activity capture (spec: docs/specs/github-activity.md)."""

from __future__ import annotations

import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.github_activity import (  # noqa: E402
    API,
    MAX_EVENT_PAGES,
    GitHubError,
    _next_link,
    capture,
    collect,
    main,
)

REPO = {
    "id": 2, "full_name": "o/b", "name": "b", "private": False, "fork": False, "archived": False,
    "language": "Python", "topics": ["z", "a"], "stargazers_count": 1, "forks_count": 0,
    "open_issues_count": 3, "size": 10, "default_branch": "main", "created_at": "2026-01-01T00:00:00Z",
    "updated_at": "2026-11-01T00:00:00Z", "pushed_at": "2026-11-11T00:00:00Z", "html_url": "https://github.com/o/b",
    "owner": {"login": "o", "email": "secret@example.com"},
}
PRIVATE_REPO = dict(REPO, id=3, full_name="o/private", private=True)
PUSH = {
    "id": "101", "type": "PushEvent", "public": True, "created_at": "2026-11-11T01:00:00Z",
    "repo": {"name": "o/b"}, "actor": {"login": "o"},
    "payload": {"ref": "refs/heads/main", "size": 2, "commits": [
        {"message": "private message", "author": {"name": "Owner", "email": "secret@example.com"}},
        {"message": "another", "author": {"name": "Owner", "email": "secret@example.com"}},
    ]},
}
PR = {
    "id": "102", "type": "PullRequestEvent", "public": True, "created_at": "2026-11-11T02:00:00Z",
    "repo": {"name": "o/b"}, "actor": {"login": "o"},
    "payload": {"action": "opened", "pull_request": {"number": 7, "body": "secret body"}},
}


class FakeGitHub:
    def __init__(self, pages: dict[str, list[tuple[list, str]]] | None = None, fail: bool = False):
        self.calls: list[str] = []
        self.fail = fail
        self.pages = pages

    def __call__(self, url: str):
        self.calls.append(url)
        if self.fail:
            raise GitHubError("HTTP 500")
        if self.pages is not None:
            for prefix, responses in self.pages.items():
                if url.startswith(prefix):
                    index = sum(1 for c in self.calls if c.startswith(prefix)) - 1
                    return responses[min(index, len(responses) - 1)]
        if "/repos?" in url:
            return [REPO, PRIVATE_REPO], ""
        return [PUSH, PR], ""


class TrimTests(unittest.TestCase):
    def test_no_pii_or_private_repos_reach_records(self):
        records = collect(FakeGitHub(), "o")
        blob = json.dumps(records)
        for secret in ("secret@example.com", "private message", "secret body", "o/private"):
            self.assertNotIn(secret, blob)
        repo = records["github_repos"][0]
        self.assertEqual(repo["topics"], ["a", "z"])
        events = {e["id"]: e for e in records["github_events"]}
        self.assertEqual(events["101"]["push_size"], 2)
        self.assertEqual(events["101"]["ref"], "refs/heads/main")
        self.assertEqual(events["102"]["number"], 7)
        self.assertEqual(events["102"]["action"], "opened")

    def test_owner_is_validated(self):
        with self.assertRaises(ValueError):
            collect(FakeGitHub(), "o/../x")


class PagingTests(unittest.TestCase):
    def test_follows_next_link_within_bound(self):
        next_link = f'<{API}/users/o/events/public?page=2>; rel="next"'
        pages = {f"{API}/users/o/events/public": [([PUSH], next_link)] * 10, f"{API}/users/o/repos": [([REPO], "")]}
        fake = FakeGitHub(pages)
        collect(fake, "o")
        self.assertEqual(sum("/events/public" in c for c in fake.calls), MAX_EVENT_PAGES)

    def test_never_follows_off_host_links(self):
        self.assertIsNone(_next_link('<https://evil.example/x>; rel="next"'))
        self.assertEqual(_next_link(f'<{API}/a?page=2>; rel="next", <{API}/a?page=9>; rel="last"'), f"{API}/a?page=2")


class CaptureTests(unittest.TestCase):
    def test_capture_writes_once_then_nothing_new(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = capture(FakeGitHub(), "o", tmp)
            self.assertEqual(first["status"], "captured")
            self.assertEqual(first["datasets"]["github_repos"]["records"], 1)
            self.assertEqual(first["datasets"]["github_events"]["records"], 2)
            self.assertTrue(all(d["new"] for d in first["datasets"].values()))
            bronze = list(Path(tmp).rglob("*.parquet"))
            self.assertTrue(any("domain=engineering" in str(p) and "dataset=github_events" in str(p) for p in bronze))
            second = capture(FakeGitHub(), "o", tmp)
            self.assertFalse(any(d["new"] for d in second["datasets"].values()))

    def test_api_error_exits_2_with_type_only(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(out):
            code = main(["--owner", "o", "--data-lake-uri", tmp], fetch=FakeGitHub(fail=True))
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(out.getvalue()), {"status": "error", "error": "GitHubError"})

    def test_empty_or_local_lake_uri_is_refused(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(main(["--owner", "o", "--data-lake-uri", ""], fetch=FakeGitHub()), 2)
            self.assertEqual(main(["--owner", "o", "--data-lake-uri", "/tmp/x", "--require-cloud"], fetch=FakeGitHub()), 2)
        self.assertIn("LakeUriNotConfigured", out.getvalue())

    def test_dry_run_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            report = capture(FakeGitHub(), "o", tmp, dry_run=True)
            self.assertEqual(report["status"], "dry-run")
            self.assertEqual(list(Path(tmp).rglob("*")), [])


if __name__ == "__main__":
    unittest.main()
