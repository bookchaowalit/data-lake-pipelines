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
            with self.assertRaisesRegex(WarehouseLoadError, "published jobs marts"):
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

    def test_file_named_jobs_does_not_shadow_the_schema(self):
        from data_lake.motherduck_load import load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = str(Path(out) / "jobs.duckdb")  # catalog "jobs" == schema "jobs"
            first = load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(first["status"], "loaded")
            again = load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(again["status"], "unchanged")

    def test_restore_by_manifest_key_does_not_need_a_pointer(self):
        from data_lake import motherduck_load

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as tmp:
            gold = self._gold(root)
            with mock.patch.object(
                motherduck_load, "latest_pointer", side_effect=AssertionError("pointer read")
            ):
                out = io.StringIO()
                with contextlib.redirect_stdout(out):
                    code = motherduck_load.main([
                        "--json", "--target", str(Path(tmp) / "w.duckdb"), "--data-lake-uri", root,
                        "--manifest-key", gold["manifest_key"],
                    ])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(out.getvalue())["status"], "loaded")

    def test_load_of_an_older_publish_is_refused_inside_the_transaction(self):
        from data_lake.motherduck_load import load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            store = ObjectStore(root)
            manifest = json.loads(store.get_bytes(gold["manifest_key"]))
            manifest["gold_run_id"] = "gold-second"
            for mart in manifest["marts"]:
                data = store.get_bytes(mart["key"])
                mart["key"] = mart["key"].replace(gold["gold_run_id"], "gold-second")
                store.put_bytes(mart["key"], data)
            second_key = gold["manifest_key"].replace(gold["gold_run_id"], "gold-second")
            store.put_bytes(second_key, json.dumps(manifest).encode("utf-8"))
            target = str(Path(out) / "w.duckdb")
            # The newer publish (B) lands first; the older one (A) arrives late.
            newer = load_jobs_gold(second_key, target, data_lake_uri=root, published_key="p/published_at=2", now=NOW)
            self.assertEqual(newer["status"], "loaded")
            older = load_jobs_gold(
                gold["manifest_key"], target, data_lake_uri=root, published_key="p/published_at=1", now=NOW
            )
            self.assertEqual(older["status"], "skipped_not_latest")
            import duckdb

            con = duckdb.connect(target, read_only=True)
            try:
                held = con.sql("select gold_run_id from jobs._gold_loads order by seq desc limit 1").fetchall()
            finally:
                con.close()
            self.assertEqual(held, [("gold-second",)])

    def test_mart_not_export_approved_is_reported_not_loaded(self):
        from data_lake import motherduck_load

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            approved = {k: v for k, v in motherduck_load.EXPORT_APPROVED.items() if k != "jobs_company_hiring"}
            target = str(Path(out) / "w.duckdb")
            with mock.patch.object(motherduck_load, "EXPORT_APPROVED", approved):
                result = motherduck_load.load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(result["status"], "loaded")
            self.assertEqual(result["not_exported"], ["jobs_company_hiring"])
            self.assertNotIn("jobs_company_hiring", result["loaded_rows"])
            import duckdb

            con = duckdb.connect(target, read_only=True)
            try:
                tables = {r[0] for r in con.sql("select table_name from information_schema.tables").fetchall()}
            finally:
                con.close()
            self.assertNotIn("jobs_company_hiring", tables)

    def test_duplicate_mart_is_refused(self):
        from data_lake.motherduck_load import WarehouseLoadError, load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            store = ObjectStore(root)
            manifest = json.loads(store.get_bytes(gold["manifest_key"]))
            manifest["marts"].append(dict(manifest["marts"][0]))
            dup_key = gold["manifest_key"].replace("gold_run_id=", "gold_run_id=dup-")
            store.put_bytes(dup_key, json.dumps(manifest).encode("utf-8"))
            with self.assertRaisesRegex(WarehouseLoadError, "more than once"):
                load_jobs_gold(dup_key, str(Path(out) / "w.duckdb"), data_lake_uri=root)

    def test_pointer_failing_the_reader_contract_fails_the_load(self):
        from data_lake import motherduck_load
        from data_lake.jobs_marts import GOLD_POINTER_PREFIX

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as tmp:
            gold = self._gold(root)
            ObjectStore(root).put_bytes(
                f"{GOLD_POINTER_PREFIX}published_at=99991231T000000Z-x.json",
                json.dumps({"pointer_version": "2", "layer": "gold", "manifest_key": gold["manifest_key"]}).encode(),
            )
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                code = motherduck_load.main(["--json", "--target", str(Path(tmp) / "w.duckdb"), "--data-lake-uri", root])
            self.assertEqual(code, 2)
            self.assertIn("reader contract", json.loads(out.getvalue())["error"])

    def test_pointer_chosen_run_records_its_publish_order(self):
        from data_lake import motherduck_load

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as tmp:
            self._gold(root)
            target = str(Path(tmp) / "w.duckdb")
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(motherduck_load.main(["--json", "--target", target, "--data-lake-uri", root]), 0)
            import duckdb

            con = duckdb.connect(target, read_only=True)
            try:
                key = con.sql("select published_key from jobs._gold_loads").fetchall()[0][0]
            finally:
                con.close()
            self.assertTrue(key and key.endswith(".json"))

    def test_motherduck_token_goes_in_the_connection_config(self):
        from data_lake import motherduck_load

        calls = []

        class FakeCon:
            def execute(self, *a, **k):
                return self

        def fake_connect(path, config=None):
            calls.append((path, config))
            return FakeCon()

        with mock.patch.dict(os.environ, {"MOTHERDUCK_TOKEN": "tok-test"}), mock.patch("duckdb.connect", fake_connect):
            motherduck_load._connect("md:empire_gold")
        self.assertEqual(calls, [("md:", {"motherduck_token": "tok-test"})])

    def test_withdrawn_mart_is_dropped_on_the_next_load(self):
        from data_lake import motherduck_load

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = str(Path(out) / "w.duckdb")
            motherduck_load.load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            approved = {k: v for k, v in motherduck_load.EXPORT_APPROVED.items() if k != "jobs_company_hiring"}
            with mock.patch.object(motherduck_load, "EXPORT_APPROVED", approved):
                result = motherduck_load.load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(result["status"], "loaded")
            self.assertEqual(result["dropped"], ["jobs_company_hiring"])

    def test_unapproved_column_is_refused(self):
        from data_lake import motherduck_load

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            approved = dict(motherduck_load.EXPORT_APPROVED)
            approved["jobs_company_hiring"] = tuple(c for c in approved["jobs_company_hiring"] if c != "company")
            with mock.patch.object(motherduck_load, "EXPORT_APPROVED", approved):
                with self.assertRaisesRegex(motherduck_load.WarehouseLoadError, "export-approved columns"):
                    motherduck_load.load_jobs_gold(
                        gold["manifest_key"], str(Path(out) / "w.duckdb"), data_lake_uri=root, now=NOW
                    )

    def test_republish_of_the_held_run_advances_the_watermark(self):
        from data_lake.motherduck_load import load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            store = ObjectStore(root)
            manifest = json.loads(store.get_bytes(gold["manifest_key"]))
            manifest["gold_run_id"] = "gold-second"
            for mart in manifest["marts"]:
                data = store.get_bytes(mart["key"])
                mart["key"] = mart["key"].replace(gold["gold_run_id"], "gold-second")
                store.put_bytes(mart["key"], data)
            second_key = gold["manifest_key"].replace(gold["gold_run_id"], "gold-second")
            store.put_bytes(second_key, json.dumps(manifest).encode("utf-8"))
            target = str(Path(out) / "w.duckdb")
            # A at p1, then A re-published at p3 (unchanged), then a slow B from p2 arrives.
            load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, published_key="p1", now=NOW)
            again = load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, published_key="p3", now=NOW)
            self.assertEqual(again["status"], "unchanged")
            late = load_jobs_gold(second_key, target, data_lake_uri=root, published_key="p2", now=NOW)
            self.assertEqual(late["status"], "skipped_not_latest")

    def test_tables_the_loader_never_wrote_are_never_dropped(self):
        import duckdb

        from data_lake.motherduck_load import load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = str(Path(out) / "w.duckdb")
            load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            con = duckdb.connect(target)
            con.execute("CREATE TABLE jobs.analyst_notes AS SELECT 1 AS x")
            con.close()
            again = load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(again["status"], "unchanged")
            con = duckdb.connect(target, read_only=True)
            try:
                self.assertEqual(con.sql("select x from jobs.analyst_notes").fetchall(), [(1,)])
            finally:
                con.close()

    def test_unchanged_is_not_reported_when_warehouse_columns_drift(self):
        import duckdb

        from data_lake.motherduck_load import load_jobs_gold

        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out:
            gold = self._gold(root)
            target = str(Path(out) / "w.duckdb")
            load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            con = duckdb.connect(target)
            con.execute("ALTER TABLE jobs.jobs_company_hiring ADD COLUMN contact VARCHAR")
            con.close()
            again = load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            self.assertEqual(again["status"], "loaded")
            con = duckdb.connect(target, read_only=True)
            try:
                cols = [r[0] for r in con.sql("describe jobs.jobs_company_hiring").fetchall()]
            finally:
                con.close()
            self.assertNotIn("contact", cols)


if __name__ == "__main__":
    unittest.main()
