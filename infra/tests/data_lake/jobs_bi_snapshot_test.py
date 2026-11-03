#!/usr/bin/env python3
"""Tests for the jobs Gold → BI snapshot (the DuckDB file the Evidence site reads)."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_lake.storage import ObjectStore  # noqa: E402
import jobs_marts_test  # noqa: E402
from jobs_marts_test import FIXTURE_JOBS, NOW, _ingest  # noqa: E402


class JobsBiSnapshotTests(unittest.TestCase):
    # Same runtime checks as the marts tests (duckdb, pyarrow, dbt).
    setUp = jobs_marts_test.JobsMartsTests.setUp

    def _gold(self, root):
        from data_lake.jobs_marts import run_jobs_marts

        _ingest(root, FIXTURE_JOBS)
        return run_jobs_marts(data_lake_uri=root, max_invalid_ratio=0.25, now=NOW)

    def test_snapshot_loads_every_mart_and_the_gold_meta_row(self):
        import duckdb

        from data_lake.jobs_bi_snapshot import build_bi_snapshot

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = Path(out) / "jobs.duckdb"
            result = build_bi_snapshot(gold["manifest_key"], target, data_lake_uri=root)
            self.assertEqual(result["gold_run_id"], gold["gold_run_id"])
            con = duckdb.connect(str(target), read_only=True)
            try:
                for mart, info in gold["marts"].items():
                    rows = con.sql(f"select count(*) from {mart}").fetchall()[0][0]
                    self.assertEqual(rows, info["rows"], mart)
                meta = con.sql(
                    "select gold_run_id, silver_run_id, cast(as_of_date as varchar), "
                    "cast(max_seen_at as varchar) from gold_meta"
                ).fetchall()
            finally:
                con.close()
            self.assertEqual(
                meta,
                [(gold["gold_run_id"], gold["silver_run_id"], "2026-10-22", "2026-10-22 06:00:00")],
            )

    def test_tampered_mart_is_rejected_and_nothing_is_written(self):
        from data_lake.jobs_bi_snapshot import BiSnapshotError, build_bi_snapshot

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            store = ObjectStore(root)
            manifest = json.loads(store.get_bytes(gold["manifest_key"]))
            manifest["marts"][0]["sha256"] = "0" * 64
            fake_key = gold["manifest_key"].replace("gold_run_id=", "gold_run_id=tampered-")
            store.put_bytes(fake_key, json.dumps(manifest).encode("utf-8"))
            target = Path(out) / "jobs.duckdb"
            with self.assertRaisesRegex(BiSnapshotError, "sha256"):
                build_bi_snapshot(fake_key, target, data_lake_uri=root)
            self.assertFalse(target.exists())

    def test_only_jobs_gold_manifests_are_accepted(self):
        from data_lake.jobs_bi_snapshot import BiSnapshotError, build_bi_snapshot

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = Path(out) / "jobs.duckdb"
            for key in (gold["silver"]["key"], "control/manifests/gold/source=other/x.json"):
                with self.assertRaisesRegex(BiSnapshotError, "not a jobs Gold manifest"):
                    build_bi_snapshot(key, target, data_lake_uri=root)

    def test_rebuild_replaces_the_previous_snapshot(self):
        import duckdb

        from data_lake.jobs_bi_snapshot import build_bi_snapshot

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = Path(out) / "jobs.duckdb"
            build_bi_snapshot(gold["manifest_key"], target, data_lake_uri=root)
            build_bi_snapshot(gold["manifest_key"], target, data_lake_uri=root)
            self.assertEqual(sorted(p.name for p in Path(out).iterdir()), ["jobs.duckdb"])
            con = duckdb.connect(str(target), read_only=True)
            try:
                self.assertEqual(con.sql("select count(*) from gold_meta").fetchall()[0][0], 1)
            finally:
                con.close()


if __name__ == "__main__":
    unittest.main()
