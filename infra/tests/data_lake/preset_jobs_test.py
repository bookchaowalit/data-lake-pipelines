#!/usr/bin/env python3
"""ADR-0016 P3: Preset dataset parity with jobs-insights, and the export ZIP check."""

from __future__ import annotations

import io
import sys
import tempfile
import unittest
import zipfile
from unittest import mock
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import jobs_marts_test  # noqa: E402
from jobs_marts_test import FIXTURE_JOBS, NOW, _ingest  # noqa: E402


class PresetParityTests(unittest.TestCase):
    setUp = jobs_marts_test.JobsMartsTests.setUp

    def _loaded(self, root: str, out: str) -> tuple[dict, str]:
        from data_lake.jobs_marts import run_jobs_marts
        from data_lake.motherduck_load import load_jobs_gold

        _ingest(root, FIXTURE_JOBS)
        gold = run_jobs_marts(data_lake_uri=root, max_invalid_ratio=0.25, now=NOW)
        # Named empire_gold so the datasets' three-part names resolve as they do in MotherDuck.
        target = str(Path(out) / "empire_gold.duckdb")
        self.assertEqual(
            load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)[
                "status"
            ],
            "loaded",
        )
        return gold, target

    def test_datasets_match_the_site_and_the_streak_reaches_14_days(self):
        from data_lake.preset_parity import check_parity

        with (
            tempfile.TemporaryDirectory() as root,
            tempfile.TemporaryDirectory() as out,
        ):
            gold, target = self._loaded(root, out)
            first = check_parity(
                gold["manifest_key"], target, data_lake_uri=root, now=NOW
            )
            self.assertEqual(first["status"], "match", first)
            self.assertEqual(first["mismatches"], [])
            self.assertFalse(first["p3_parity_met"])
            later = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW + timedelta(days=14),
            )
            self.assertEqual(later["matching_days"], 14)
            self.assertTrue(later["p3_parity_met"])

    def test_fixture_is_not_trivially_empty(self):
        from data_lake.jobs_bi_site import load_snapshot
        from data_lake.jobs_bi_snapshot import build_bi_snapshot
        from data_lake.preset_parity import site_values

        with (
            tempfile.TemporaryDirectory() as root,
            tempfile.TemporaryDirectory() as out,
        ):
            gold, _ = self._loaded(root, out)
            path = Path(out) / "site.duckdb"
            build_bi_snapshot(gold["manifest_key"], path, data_lake_uri=root)
            site = site_values(load_snapshot(path))
            self.assertGreater(site["kpis"]["postings"], 0)
            self.assertGreater(site["kpis"]["companies"], 0)
            self.assertTrue(site["board_totals_90d"])

    def test_a_changed_number_is_a_mismatch_and_resets_the_streak(self):
        import duckdb

        from data_lake.preset_parity import check_parity, main

        with (
            tempfile.TemporaryDirectory() as root,
            tempfile.TemporaryDirectory() as out,
        ):
            gold, target = self._loaded(root, out)
            check_parity(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
            con = duckdb.connect(target)
            con.execute(
                "update jobs.jobs_board_health set postings_7d = postings_7d + 1"
            )
            con.close()
            bad = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW + timedelta(days=15),
            )
            self.assertEqual(bad["status"], "mismatch")
            self.assertEqual(bad["mismatches"], ["kpis.postings_7d"])
            self.assertFalse(bad["p3_parity_met"])
            con = duckdb.connect(target)
            con.execute(
                "update jobs.jobs_board_health set postings_7d = postings_7d - 1"
            )
            con.close()
            good = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW + timedelta(days=16),
            )
            self.assertEqual((good["status"], good["matching_days"]), ("match", 0))
            out_buf = io.StringIO()
            with mock.patch("sys.stdout", out_buf):
                code = main(
                    [
                        "--manifest-key",
                        gold["manifest_key"],
                        "--target",
                        target,
                        "--data-lake-uri",
                        root,
                    ]
                )
            self.assertEqual(code, 0)

    def test_skills_week_and_order_match_the_site(self):
        import duckdb

        from data_lake.jobs_bi_site import load_snapshot
        from data_lake.jobs_bi_snapshot import build_bi_snapshot
        from data_lake.preset_parity import compare, site_values, warehouse_values

        # as_of 2026-10-22 (Thursday): the last complete week starts 2026-10-12. The fixture
        # Gold has no tags that week, so add the same rows on both sides: 25 tags in that
        # week (only 20 shown, ties on rank ordered by tag), plus the week in progress.
        rows = [("2026-10-12", f"tag{i:02d}", 30 - i, i // 2 + 1) for i in range(25)]
        rows += [("2026-10-19", "current-week", 99, 1)]
        with (
            tempfile.TemporaryDirectory() as root,
            tempfile.TemporaryDirectory() as out,
        ):
            gold, target = self._loaded(root, out)
            site_db = Path(out) / "site.duckdb"
            build_bi_snapshot(gold["manifest_key"], site_db, data_lake_uri=root)
            for path, table in (
                (site_db, "jobs_skills_demand"),
                (Path(target), "jobs.jobs_skills_demand"),
            ):
                con = duckdb.connect(str(path))
                con.executemany(
                    f"insert into {table} values (cast(? as date), ?, ?, ?)", rows
                )
                con.close()
            site = site_values(load_snapshot(site_db))
            con = duckdb.connect(target, read_only=True)
            try:
                warehouse = warehouse_values(con)
            finally:
                con.close()
            self.assertEqual(len(site["skills_last_week"][1]), 20)
            self.assertEqual(compare(site, warehouse), [])

    def test_warehouse_without_this_run_is_skipped(self):
        from data_lake.jobs_marts import run_jobs_marts
        from data_lake.preset_parity import check_parity

        with (
            tempfile.TemporaryDirectory() as root,
            tempfile.TemporaryDirectory() as out,
        ):
            _ingest(root, FIXTURE_JOBS)
            gold = run_jobs_marts(data_lake_uri=root, max_invalid_ratio=0.25, now=NOW)
            target = str(Path(out) / "empire_gold.duckdb")
            result = check_parity(
                gold["manifest_key"], target, data_lake_uri=root, now=NOW
            )
            self.assertEqual(result["status"], "skipped_run_mismatch")


def _zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text)
    return buf.getvalue()


def _export(**overrides: str | None) -> dict[str, str]:
    from data_lake.preset_parity import dataset_sql

    sql = "\n".join("    " + line for line in dataset_sql("jobs_kpis").splitlines())
    files: dict[str, str | None] = {
        "dashboard_export/metadata.yaml": "version: 1.0.0\ntype: Dashboard\n",
        "dashboard_export/databases/MotherDuck.yaml": (
            "database_name: MotherDuck\nsqlalchemy_uri: duckdb:///md:empire_gold\nextra:\n  allows_virtual_table_explore: true\n"
        ),
        "dashboard_export/datasets/MotherDuck/jobs_kpis.yaml": f"table_name: jobs_kpis\nschema: null\nsql: |\n{sql}\n",
        "dashboard_export/datasets/MotherDuck/jobs_board_health.yaml": (
            "table_name: jobs_board_health\nschema: jobs\ncolumns:\n- column_name: board\n"
            "- column_name: postings_total\n- column_name: share\n  expression: postings_total * 1.0\n"
        ),
        "dashboard_export/charts/Postings.yaml": "slice_name: Postings\nviz_type: big_number_total\n",
        "dashboard_export/dashboards/Jobs.yaml": "dashboard_title: Jobs insights\n",
    }
    files.update(overrides)
    return {name: text for name, text in files.items() if text is not None}


class PresetExportCheckTests(unittest.TestCase):
    def _problems(self, files: dict[str, str]) -> list[str]:
        from data_lake.preset_export_check import check_zip

        with tempfile.TemporaryDirectory() as out:
            path = Path(out) / "export.zip"
            path.write_bytes(_zip(files))
            return check_zip(path)

    def test_clean_export_passes(self):
        self.assertEqual(self._problems(_export()), [])

    def test_committed_exports_pass(self):
        from data_lake.preset_export_check import check_zip, committed_exports

        for path in committed_exports():
            self.assertEqual(check_zip(path), [], path.name)

    def test_secrets_are_refused_without_echoing_them(self):
        token = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0dXNlciJ9.c2lnbmF0dXJl"
        db = "dashboard_export/databases/MotherDuck.yaml"
        cases = {
            "token in URI": f"database_name: MotherDuck\nsqlalchemy_uri: duckdb:///md:empire_gold?motherduck_token={token}\n",
            "password": "database_name: MotherDuck\nsqlalchemy_uri: duckdb:///md:empire_gold\npassword: hunter2hunter2\n",
            "encrypted extra": (
                "database_name: MotherDuck\nsqlalchemy_uri: duckdb:///md:empire_gold\n"
                "encrypted_extra: '{\"connect_args\": {}}'\n"
            ),
        }
        for label, text in cases.items():
            with self.subTest(label):
                problems = self._problems(_export(**{db: text}))
                self.assertTrue(problems)
                self.assertFalse(any(token in p or "hunter2" in p for p in problems))
        masked = "database_name: MotherDuck\nsqlalchemy_uri: duckdb:///md:empire_gold\npassword: XXXXXXXXXX\n"
        self.assertEqual(self._problems(_export(**{db: masked})), [])

    def test_data_outside_the_allow_list_is_refused(self):
        ds = "dashboard_export/datasets/MotherDuck/"
        cases = {
            "other database": {
                "dashboard_export/databases/MotherDuck.yaml": "sqlalchemy_uri: duckdb:///md:x_intel\n"
            },
            "other table": {ds + "t.yaml": "table_name: fact_sales\nschema: jobs\n"},
            "other schema": {
                ds + "t.yaml": "table_name: jobs_board_health\nschema: main\n"
            },
            "load log": {ds + "t.yaml": "table_name: _gold_loads\nschema: jobs\n"},
            "unapproved column": {
                ds
                + "t.yaml": "table_name: jobs_company_hiring\nschema: jobs\ncolumns:\n- column_name: email\n"
            },
            "own SQL": {
                ds
                + "t.yaml": "table_name: q\nsql: select * from empire_gold.jobs.jobs_company_hiring\n"
            },
            "not YAML": {"dashboard_export/charts/x.json": "{}"},
            "path traversal": {"../evil.yaml": "a: 1\n"},
            "no dashboard": {"dashboard_export/dashboards/Jobs.yaml": None},
        }
        for label, override in cases.items():
            with self.subTest(label):
                self.assertTrue(self._problems(_export(**override)), label)


if __name__ == "__main__":
    unittest.main()
