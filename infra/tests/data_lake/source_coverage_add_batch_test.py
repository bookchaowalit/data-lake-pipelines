#!/usr/bin/env python3
"""Tests for source_coverage_add_batch.py: appends a batch and refuses unsafe rewrites."""

from __future__ import annotations

import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts" / "utils"))

from source_coverage_add_batch import main  # noqa: E402

REGISTRY = """schema_version: x
current_batch: 1
batches:
  1: {checked_at: '2026-11-12', report: docs/b1.md}
sources:
  - id: a
    name: A
    batch: 1
next_batch:
  number: 2
  sources:
    [b]
"""
ROWS = {
    "batch": 2,
    "checked_at": "2026-11-13",
    "sources": [{"id": "b", "name": "B", "verdict": "skip"}],
    "next_batch": ["c", "d"],
}


class AddBatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.registry = self.dir / "reg.yaml"
        self.rows = self.dir / "rows.yaml"
        self.rows.write_text(yaml.safe_dump(ROWS), encoding="utf-8")

    def tearDown(self):
        self.tmp.cleanup()

    def _run(self, registry_text: str) -> int:
        self.registry.write_text(registry_text, encoding="utf-8")
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            return main(
                [
                    str(self.rows),
                    "--report",
                    "docs/b2.md",
                    "--registry",
                    str(self.registry),
                ]
            )

    def test_appends_and_moves_the_batch(self):
        self.assertEqual(self._run(REGISTRY), 0)
        data = yaml.safe_load(self.registry.read_text(encoding="utf-8"))
        self.assertEqual(data["current_batch"], 2)
        self.assertEqual(data["batches"][2]["report"], "docs/b2.md")
        self.assertEqual([s["id"] for s in data["sources"]], ["a", "b"])
        self.assertEqual(data["sources"][1]["batch"], 2)
        self.assertEqual(data["sources"][1]["checked_at"], "2026-11-13")
        self.assertEqual(data["next_batch"], {"number": 3, "sources": ["c", "d"]})

    def test_refuses_keys_after_next_batch(self):
        text = REGISTRY + "extra: keep-me\n"
        self.assertEqual(self._run(text), 1)
        self.assertEqual(self.registry.read_text(encoding="utf-8"), text)

    def test_refuses_a_batch_out_of_order(self):
        ROWS_BAD = dict(ROWS, batch=3)
        self.rows.write_text(yaml.safe_dump(ROWS_BAD), encoding="utf-8")
        self.assertEqual(self._run(REGISTRY), 1)


if __name__ == "__main__":
    unittest.main()
