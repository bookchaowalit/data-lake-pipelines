#!/usr/bin/env python3
"""Offline tests for the Hugging Face trending snapshot (spec: docs/specs/hf-trending.md)."""

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

from data_lake.hf_trending import API, HubError, capture, collect, http_fetch, main  # noqa: E402

MODEL = {
    "id": "org/model-a",
    "modelId": "org/model-a",
    "trendingScore": 900,
    "likes": 10,
    "downloads": 5,
    "pipeline_tag": "text-generation",
    "library_name": "transformers",
    "tags": [f"t{i}" for i in range(40)],
    "createdAt": "2026-11-01T00:00:00Z",
    "private": False,
}
DATASET = {
    "id": "org/data-b",
    "author": "org",
    "trendingScore": 50,
    "likes": 2,
    "downloads": 9,
    "description": "long free text that must not land",
    "cardData": {"x": 1},
    "gated": False,
    "lastModified": "2026-11-10T00:00:00Z",
    "private": False,
}
SPACE = {
    "id": "user/space-c",
    "trendingScore": 7,
    "likes": 1,
    "sdk": "gradio",
    "private": False,
}
PRIVATE = dict(MODEL, id="org/hidden", private=True)


class FakeHub:
    def __init__(self, fail: bool = False):
        self.fail = fail
        self.calls: list[str] = []

    def __call__(self, url: str):
        self.calls.append(url)
        if self.fail:
            raise HubError("HTTP 503")
        if "/models?" in url:
            return [MODEL, PRIVATE]
        if "/datasets?" in url:
            return [DATASET]
        return [SPACE]


class CollectTests(unittest.TestCase):
    def test_rows_are_trimmed_and_ranked(self):
        rows = collect(FakeHub(), "2026-11-12")
        self.assertEqual(
            set(rows),
            {"hf_trending_models", "hf_trending_datasets", "hf_trending_spaces"},
        )
        model = rows["hf_trending_models"]
        self.assertEqual(len(model), 1)  # private dropped
        self.assertEqual(model[0]["id"], "org/model-a@2026-11-12")
        self.assertEqual(model[0]["rank"], 1)
        self.assertEqual(model[0]["author"], "org")
        self.assertEqual(len(model[0]["tags"]), 20)
        blob = json.dumps(rows)
        self.assertNotIn("long free text", blob)
        self.assertNotIn("cardData", blob)
        self.assertEqual(rows["hf_trending_spaces"][0]["sdk"], "gradio")

    def test_fetch_refuses_other_hosts(self):
        fetch = http_fetch()
        for url in ("file:///etc/passwd", "https://evil.example/api/models"):
            with self.assertRaises(HubError):
                fetch(url)
        self.assertTrue(API.startswith("https://huggingface.co/"))


class CaptureTests(unittest.TestCase):
    def test_capture_then_same_day_rerun_writes_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = capture(FakeHub(), tmp, snapshot_date="2026-11-12")
            self.assertTrue(all(d["new"] for d in first["datasets"].values()))
            self.assertTrue(
                any(
                    "domain=market-intelligence" in str(p)
                    for p in Path(tmp).rglob("*.parquet")
                )
            )
            second = capture(FakeHub(), tmp, snapshot_date="2026-11-12")
            self.assertFalse(any(d["new"] for d in second["datasets"].values()))
            third = capture(FakeHub(), tmp, snapshot_date="2026-11-13")
            self.assertTrue(all(d["new"] for d in third["datasets"].values()))

    def test_errors_exit_2(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(out):
            self.assertEqual(
                main(["--data-lake-uri", tmp], fetch=FakeHub(fail=True)), 2
            )
            self.assertEqual(main(["--data-lake-uri", ""], fetch=FakeHub()), 2)
            self.assertEqual(
                main(["--data-lake-uri", tmp, "--require-cloud"], fetch=FakeHub()), 2
            )
            self.assertEqual(
                main(
                    ["--data-lake-uri", tmp, "--snapshot-date", "12-11-2026"],
                    fetch=FakeHub(),
                ),
                2,
            )
        self.assertIn("HubError", out.getvalue())
        self.assertIn("LakeUriNotConfigured", out.getvalue())


if __name__ == "__main__":
    unittest.main()
