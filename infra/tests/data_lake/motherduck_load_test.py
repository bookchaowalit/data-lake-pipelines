#!/usr/bin/env python3
"""Jobs Gold → warehouse load (ADR-0016 P2), against a local DuckDB file instead of MotherDuck."""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from data_lake.storage import ObjectStore  # noqa: E402
import jobs_marts_test  # noqa: E402
from jobs_marts_test import FIXTURE_JOBS, NOW, _ingest  # noqa: E402


class MotherDuckLoadTests(unittest.TestCase):
    # Same runtime checks as the marts tests (duckdb, pyarrow, dbt).
    setUp = jobs_marts_test.JobsMartsTests.setUp

    def _gold(self, root):
        from data_lake.jobs_marts import run_jobs_marts

        _ingest(root, FIXTURE_JOBS)
        return run_jobs_marts(data_lake_uri=root, max_invalid_ratio=0.25, now=NOW)

    def _counts(self, target, marts):
        import duckdb

        con = duckdb.connect(str(target), read_only=True)
        try:
            return {m: con.sql(f"select count(*) from jobs.{m}").fetchall()[0][0] for m in marts}
        finally:
            con.close()

    def test_load_matches_manifest_counts_then_rerun_is_unchanged(self):
        from data_lake.motherduck_load import load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = str(Path(out) / "warehouse.duckdb")
            first = load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(first["status"], "loaded")
            expected = {m: info["rows"] for m, info in gold["marts"].items()}
            self.assertEqual(first["loaded_rows"], expected)
            self.assertEqual(self._counts(target, expected), expected)

            again = load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(again["status"], "unchanged")

    def test_tampered_part_is_rejected_and_tables_stay_as_they_were(self):
        from data_lake.motherduck_load import WarehouseLoadError, load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = str(Path(out) / "warehouse.duckdb")
            load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            store = ObjectStore(root)
            manifest = json.loads(store.get_bytes(gold["manifest_key"]))
            manifest["gold_run_id"] = "gold-tampered"
            for mart in manifest["marts"]:
                data = store.get_bytes(mart["key"])
                mart["key"] = mart["key"].replace(gold["gold_run_id"], "gold-tampered")
                store.put_bytes(mart["key"], data)
            manifest["marts"][0]["sha256"] = "0" * 64
            fake_key = gold["manifest_key"].replace(gold["gold_run_id"], "gold-tampered")
            store.put_bytes(fake_key, json.dumps(manifest).encode("utf-8"))
            with self.assertRaisesRegex(WarehouseLoadError, "sha256"):
                load_jobs_gold(fake_key, target, data_lake_uri=root, now=NOW)
            expected = {m: info["rows"] for m, info in gold["marts"].items()}
            self.assertEqual(self._counts(target, expected), expected)

    def test_only_allow_listed_jobs_gold_leaves_r2(self):
        from data_lake.motherduck_load import WarehouseLoadError, load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = str(Path(out) / "warehouse.duckdb")
            for key in (gold["silver"]["key"], "control/manifests/gold/source=other/x.json"):
                with self.assertRaisesRegex(WarehouseLoadError, "not a jobs Gold manifest"):
                    load_jobs_gold(key, target, data_lake_uri=root)
            store = ObjectStore(root)
            manifest = json.loads(store.get_bytes(gold["manifest_key"]))
            manifest["marts"].append({"dataset": "dim_customer", "key": "gold/x", "sha256": "", "rows": 0})
            extra_key = gold["manifest_key"].replace("gold_run_id=", "gold_run_id=extra-")
            store.put_bytes(extra_key, json.dumps(manifest).encode("utf-8"))
            with self.assertRaisesRegex(WarehouseLoadError, "allow-listed"):
                load_jobs_gold(extra_key, target, data_lake_uri=root)

    def test_cli_skips_without_a_token_and_loads_the_newest_pointer(self):
        from data_lake import motherduck_load

        env = {k: v for k, v in os.environ.items() if k not in ("MOTHERDUCK_TOKEN", "JOBS_WAREHOUSE_TARGET")}
        with mock.patch.dict(os.environ, env, clear=True):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(motherduck_load.main(["--json"]), 0)
            self.assertEqual(json.loads(out.getvalue())["status"], "skipped_no_token")

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as tmp:
            gold = self._gold(root)
            target = str(Path(tmp) / "warehouse.duckdb")
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = motherduck_load.main(["--json", "--target", target, "--data-lake-uri", root])
            result = json.loads(out.getvalue())
            self.assertEqual(code, 0)
            self.assertEqual(result["status"], "loaded")
            self.assertEqual(result["gold_run_id"], gold["gold_run_id"])

    def test_part_of_another_mart_is_refused(self):
        from data_lake.motherduck_load import WarehouseLoadError, load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            store = ObjectStore(root)
            manifest = json.loads(store.get_bytes(gold["manifest_key"]))
            first, second = manifest["marts"][0], manifest["marts"][1]
            first.update(key=second["key"], sha256=second["sha256"], rows=second["rows"])
            swap_key = gold["manifest_key"].replace("gold_run_id=", "gold_run_id=swap-")
            store.put_bytes(swap_key, json.dumps(manifest).encode("utf-8"))
            with self.assertRaisesRegex(WarehouseLoadError, "part key"):
                load_jobs_gold(swap_key, str(Path(out) / "w.duckdb"), data_lake_uri=root)

    def test_log_in_another_attached_database_is_ignored(self):
        import duckdb

        from data_lake.motherduck_load import load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            other = Path(out) / "other.duckdb"
            con = duckdb.connect(str(other))
            con.execute("CREATE SCHEMA jobs; CREATE TABLE jobs._gold_loads (seq BIGINT, gold_run_id VARCHAR)")
            con.close()
            target = Path(out) / "w.duckdb"
            real_connect = duckdb.connect

            def connect_with_other(path, *a, **k):
                c = real_connect(path, *a, **k)
                c.execute(f"ATTACH '{other}' AS other")
                return c

            with mock.patch("duckdb.connect", connect_with_other):
                result = load_jobs_gold(gold["manifest_key"], str(target), data_lake_uri=root, now=NOW)
            self.assertEqual(result["status"], "loaded")

    def test_same_timestamp_reload_of_an_older_run_is_not_unchanged(self):
        from data_lake.motherduck_load import load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            store = ObjectStore(root)
            manifest = json.loads(store.get_bytes(gold["manifest_key"]))
            # A second Gold run with the same parts and counts, under its own part keys.
            manifest["gold_run_id"] = "gold-second"
            for mart in manifest["marts"]:
                data = store.get_bytes(mart["key"])
                mart["key"] = mart["key"].replace(gold["gold_run_id"], "gold-second")
                store.put_bytes(mart["key"], data)
            second_key = gold["manifest_key"].replace(gold["gold_run_id"], "gold-second")
            store.put_bytes(second_key, json.dumps(manifest).encode("utf-8"))
            target = str(Path(out) / "w.duckdb")
            load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            load_jobs_gold(second_key, target, data_lake_uri=root, now=NOW)
            again = load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(again["status"], "loaded")

    def test_require_latest_skips_an_older_manifest(self):
        from data_lake import motherduck_load

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as tmp:
            self._gold(root)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = motherduck_load.main([
                    "--json", "--target", str(Path(tmp) / "w.duckdb"), "--data-lake-uri", root,
                    "--require-latest", "--manifest-key",
                    "control/manifests/gold/source=book-job-data/domain=jobs/gold_run_id=gold-old.json",
                ])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(out.getvalue())["status"], "skipped_not_latest")


if __name__ == "__main__":
    unittest.main()
