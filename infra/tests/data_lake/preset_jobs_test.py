#!/usr/bin/env python3
"""ADR-0016 P3: Preset dataset parity with jobs-insights, and the export ZIP check."""

from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
import zipfile
from datetime import timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import jobs_marts_test  # noqa: E402
from jobs_marts_test import FIXTURE_JOBS, NOW, _ingest  # noqa: E402

TEMP = tempfile.TemporaryDirectory


class PresetParityTests(unittest.TestCase):
    setUp = jobs_marts_test.JobsMartsTests.setUp

    def _loaded(self, root: str, out: str) -> tuple[dict, str]:
        from data_lake.jobs_marts import run_jobs_marts
        from data_lake.motherduck_load import load_jobs_gold

        _ingest(root, FIXTURE_JOBS)
        gold = run_jobs_marts(data_lake_uri=root, max_invalid_ratio=0.25, now=NOW)
        # Named empire_gold so the datasets' three-part names resolve as they do in MotherDuck.
        target = str(Path(out) / "empire_gold.duckdb")
        loaded = load_jobs_gold(
            gold["manifest_key"], target, data_lake_uri=root, now=NOW
        )
        self.assertEqual(loaded["status"], "loaded")
        return gold, target

    def _sql(self, target: str, sql: str) -> None:
        import duckdb

        con = duckdb.connect(target)
        try:
            con.execute(sql)
        finally:
            con.close()

    def test_datasets_match_the_site(self):
        from data_lake.preset_parity import check_parity

        with TEMP() as root, TEMP() as out:
            gold, target = self._loaded(root, out)
            first = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(first["status"], "match", first)
            self.assertEqual(first["mismatches"], [])
            self.assertEqual(first["dashboard_export"], "committed")
            self.assertFalse(first["p3_parity_met"])
            later = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW + timedelta(hours=2),
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(later["dashboard_matching_since"], NOW.isoformat())

    def test_checks_14_days_apart_with_nothing_between_do_not_count(self):
        from data_lake.preset_parity import check_parity

        with TEMP() as root, TEMP() as out:
            gold, target = self._loaded(root, out)
            check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            later = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW + timedelta(days=14),
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(
                (later["dashboard_matching_days"], later["p3_parity_met"]), (0, False)
            )

    def test_streak_counts_unbroken_checks_with_the_same_export(self):
        from data_lake.preset_parity import _streak

        now = NOW + timedelta(days=15)
        every_2h = [
            (now - timedelta(hours=2 * i), True, "a", True) for i in range(15 * 12 + 1)
        ]
        start, days = _streak(every_2h, now, "a")
        self.assertEqual((start, days), (NOW, 15.0))
        # A newer dashboard export restarts the dashboard streak but not the dataset one.
        reexported = [(now, True, "b", True), *every_2h[1:]]
        self.assertEqual(_streak(reexported, now, "b")[1], 0.0)
        self.assertEqual(_streak(reexported, now, None)[1], 15.0)
        # A mismatch or a 7 h gap ends the streak at that point.
        broken = list(every_2h)
        broken[24] = (broken[24][0], False, "a", True)
        self.assertEqual(_streak(broken, now, "a")[1], 2.0 - 1 / 12)
        gap = every_2h[:12] + [
            (r[0] - timedelta(hours=7), *r[1:]) for r in every_2h[12:]
        ]
        self.assertAlmostEqual(_streak(gap, now, "a")[1], 22 / 24)
        # A gap of exactly 3 h breaks it too, and a manual run cannot fill a missed slot.
        exact = every_2h[:12] + [
            (r[0] - timedelta(hours=1), *r[1:]) for r in every_2h[12:]
        ]
        self.assertAlmostEqual(_streak(exact, now, "a")[1], 22 / 24)
        manual_fill = list(every_2h)
        manual_fill[12] = (manual_fill[12][0], True, "a", False)
        self.assertAlmostEqual(_streak(manual_fill, now, "a")[1], 22 / 24)
        # A manual mismatch still breaks it.
        manual_bad = list(every_2h)
        manual_bad[12] = (manual_bad[12][0], False, "a", False)
        self.assertAlmostEqual(_streak(manual_bad, now, "a")[1], 22 / 24)

    def test_no_committed_export_never_meets_p3(self):
        from data_lake.preset_parity import check_parity

        with TEMP() as root, TEMP() as out:
            gold, target = self._loaded(root, out)
            result = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="",
                scheduled=True,
            )
            self.assertEqual(
                (result["dashboard_export"], result["p3_parity_met"]),
                ("missing", False),
            )
            self.assertEqual(result["dashboard_matching_days"], 0)

    def test_fixture_is_not_trivially_empty(self):
        from data_lake.jobs_bi_site import load_snapshot
        from data_lake.jobs_bi_snapshot import build_bi_snapshot
        from data_lake.preset_parity import site_values

        with TEMP() as root, TEMP() as out:
            gold, _ = self._loaded(root, out)
            path = Path(out) / "site.duckdb"
            build_bi_snapshot(gold["manifest_key"], path, data_lake_uri=root)
            site = site_values(load_snapshot(path))
            self.assertGreater(site["kpis"]["postings"], 0)
            self.assertGreater(site["kpis"]["companies"], 0)
            self.assertGreaterEqual(len(site["daily_90d"]), 2)

    def test_a_changed_number_is_a_mismatch_and_resets_the_streak(self):
        from data_lake.preset_parity import check_parity, main

        with TEMP() as root, TEMP() as out:
            gold, target = self._loaded(root, out)
            check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            self._sql(
                target,
                "update jobs.jobs_board_health set postings_7d = postings_7d + 1",
            )
            bad = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW + timedelta(hours=2),
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(bad["status"], "mismatch")
            self.assertEqual(bad["mismatches"], ["kpis.postings_7d", "boards"])
            self.assertEqual(bad["dashboard_matching_days"], 0)
            self._sql(
                target,
                "update jobs.jobs_board_health set postings_7d = postings_7d - 1",
            )
            good = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW + timedelta(hours=4),
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(
                good["dashboard_matching_since"], (NOW + timedelta(hours=4)).isoformat()
            )
            with mock.patch("sys.stdout", io.StringIO()):
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

    def test_postings_moved_between_days_are_a_mismatch(self):
        from data_lake.preset_parity import check_parity

        with TEMP() as root, TEMP() as out:
            gold, target = self._loaded(root, out)
            # Same board, same 90-day and per-board totals: one posting moves to another day.
            self._sql(
                target,
                """
                create temp table pick as
                    select posted_date, board from jobs.jobs_postings_daily
                    where posted_date > (select max(as_of_date) from jobs.jobs_board_health) - 80
                    order by postings desc, posted_date, board limit 1;
                update jobs.jobs_postings_daily d set postings = postings - 1
                    from pick p where d.posted_date = p.posted_date and d.board = p.board;
                insert into jobs.jobs_postings_daily (posted_date, board, postings, companies, with_salary)
                    select posted_date - 1, board, 1, 1, 0 from pick;
                """,
            )
            result = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(result["mismatches"], ["daily_90d"])

    def test_company_boards_and_dates_and_repeated_daily_rows_are_compared(self):
        from data_lake.preset_parity import check_parity

        with TEMP() as root, TEMP() as out:
            gold, target = self._loaded(root, out)
            self._sql(
                target,
                "update jobs.jobs_company_hiring set last_posted_date = last_posted_date - 1",
            )
            dated = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(dated["mismatches"], ["companies"])
            self._sql(
                target,
                "update jobs.jobs_company_hiring set last_posted_date = last_posted_date + 1",
            )
            self._sql(
                target,
                "update jobs.jobs_company_hiring set boards = list_reverse(boards) || ['Extra']",
            )
            boards = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(boards["mismatches"], ["companies"])

    def test_a_repeated_daily_row_is_a_mismatch(self):
        from data_lake.preset_parity import check_parity

        with TEMP() as root, TEMP() as out:
            gold, target = self._loaded(root, out)
            # The chart sums postings per day and board, so a repeated row doubles that point.
            self._sql(
                target,
                "insert into jobs.jobs_postings_daily select * from jobs.jobs_postings_daily "
                "where posted_date <= (select max(as_of_date) from jobs.jobs_board_health) "
                "order by posted_date desc limit 1",
            )
            result = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            self.assertIn("daily_90d", result["mismatches"])

    def test_dataset_columns_are_what_the_sql_returns(self):
        from data_lake.preset_parity import DATASET_COLUMNS, DATASETS, dataset_sql

        with TEMP() as root, TEMP() as out:
            _, target = self._loaded(root, out)
            import duckdb

            con = duckdb.connect(target, read_only=True)
            try:
                for name in DATASETS:
                    columns = tuple(
                        c[0] for c in con.execute(dataset_sql(name)).description
                    )
                    self.assertEqual(columns, DATASET_COLUMNS[name], name)
            finally:
                con.close()

    def test_a_changed_board_row_is_a_mismatch(self):
        from data_lake.preset_parity import check_parity

        with TEMP() as root, TEMP() as out:
            gold, target = self._loaded(root, out)
            self._sql(
                target,
                "update jobs.jobs_board_health set last_seen_at = last_seen_at - interval 1 hour",
            )
            result = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(result["mismatches"], ["boards"])

    def test_jobs_kpis_must_return_one_row(self):
        import duckdb

        from data_lake import preset_parity

        real = preset_parity.dataset_sql

        def doubled(name: str) -> str:
            sql = real(name)
            return (
                f"select * from (\n{sql}\n) union all select * from (\n{sql}\n)"
                if name == "jobs_kpis"
                else sql
            )

        with TEMP() as root, TEMP() as out:
            _, target = self._loaded(root, out)
            con = duckdb.connect(target, read_only=True)
            try:
                with mock.patch.object(preset_parity, "dataset_sql", doubled):
                    with self.assertRaisesRegex(
                        preset_parity.ParityError, "exactly one row"
                    ):
                        preset_parity.warehouse_values(con)
            finally:
                con.close()

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
        with TEMP() as root, TEMP() as out:
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

        with TEMP() as root, TEMP() as out:
            _ingest(root, FIXTURE_JOBS)
            gold = run_jobs_marts(data_lake_uri=root, max_invalid_ratio=0.25, now=NOW)
            target = str(Path(out) / "empire_gold.duckdb")
            result = check_parity(
                gold["manifest_key"],
                target,
                data_lake_uri=root,
                now=NOW,
                export_sha256="a",
                scheduled=True,
            )
            self.assertEqual(result["status"], "skipped_run_mismatch")


def _zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        for name, text in files.items():
            archive.writestr(name, text)
    return buf.getvalue()


DB = "dashboard_export/databases/MotherDuck.yaml"
DS = "dashboard_export/datasets/MotherDuck/"
KPIS = DS + "jobs_kpis.yaml"
HEALTH = DS + "jobs_board_health.yaml"
CHART = "dashboard_export/charts/Postings.yaml"
DASHBOARD = "dashboard_export/dashboards/Jobs.yaml"


def _indented_sql(name: str) -> str:
    from data_lake.preset_parity import dataset_sql

    return "\n".join("    " + line for line in dataset_sql(name).splitlines())


def _sql_dataset(name: str) -> str:
    return f"table_name: {name}\nschema: null\nuuid: u-{name}\ndatabase_uuid: u-db\nsql: |\n{_indented_sql(name)}\n"


def _export(**overrides: str | None) -> dict[str, str]:
    others = (
        "jobs_daily_90d",
        "jobs_skills_last_week",
        "jobs_companies_30d",
        "jobs_boards",
    )
    position = {
        f"CHART-{n}": {"type": "CHART", "meta": {"uuid": f"c-{n}"}}
        for n in ("jobs_kpis", *others)
    }
    files: dict[str, str | None] = {
        "dashboard_export/metadata.yaml": "version: 1.0.0\ntype: Dashboard\n",
        DB: "database_name: MotherDuck\nuuid: u-db\nsqlalchemy_uri: duckdb:///md:empire_gold\nextra:\n  allows_virtual_table_explore: true\n",
        KPIS: (
            _sql_dataset("jobs_kpis")
            + "columns:\n- column_name: postings\n- column_name: boards\n"
            "metrics:\n- metric_name: count\n  expression: COUNT(*)\n"
        ),
        HEALTH: (
            "table_name: jobs_board_health\nschema: jobs\nuuid: u-health\ndatabase_uuid: u-db\ncolumns:\n- column_name: board\n"
            "- column_name: postings_total\n- column_name: share\n  expression: postings_total * 1.0\n"
            "metrics:\n- metric_name: total\n  expression: SUM(postings_total)\n"
        ),
        CHART: (
            "slice_name: Postings\nviz_type: big_number_total\nuuid: c-jobs_kpis\ndataset_uuid: u-jobs_kpis\nparams:\n"
            "  metric:\n    expressionType: SQL\n    sqlExpression: sum(postings) / nullif(boards, 0)\n"
            'query_context: \'{"queries": [{"filters": [{"sqlExpression": "boards > 1"}], '
            '"extras": {"where": "", "having": "sum(postings) > 0"}}]}\'\n'
        ),
        DASHBOARD: f"dashboard_title: Jobs insights\nposition: '{json.dumps(position)}'\n",
    }
    for name in others:
        files[DS + f"{name}.yaml"] = _sql_dataset(name)
        files[f"dashboard_export/charts/{name}.yaml"] = (
            f"slice_name: {name}\nuuid: c-{name}\ndataset_uuid: u-{name}\n"
        )
    files.update(overrides)
    return {name: text for name, text in files.items() if text is not None}


class PresetExportCheckTests(unittest.TestCase):
    def _problems(self, files: dict[str, str]) -> list[str]:
        from data_lake.preset_export_check import check_zip

        with TEMP() as out:
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
        base = "database_name: MotherDuck\nuuid: u-db\nsqlalchemy_uri: duckdb:///md:empire_gold\n"
        cases = {
            "token in URI": {
                DB: f"database_name: MotherDuck\nsqlalchemy_uri: duckdb:///md:empire_gold?motherduck_token={token}\n"
            },
            "password": {DB: base + "password: hunter2hunter2\n"},
            "inline mapping password": {
                DB: base + "extra: {password: hunter2hunter2}\n"
            },
            "inline encrypted extra": {
                DB: base + "extra: {encrypted_extra: hunter2hunter2}\n"
            },
            "token in embedded JSON": {
                DB: base
                + 'extra: \'{"engine_params": {"connect_args": {"config": {"motherduck_token": "hunter2hunter2"}}}}\'\n'
            },
            "encrypted extra": {
                DB: base + "encrypted_extra: '{\"connect_args\": {}}'\n"
            },
            "JWT in chart params": {
                CHART: f'slice_name: x\ndataset_uuid: u-kpis\nparams: \'{{"note": "{token}"}}\'\n'
            },
        }
        for label, override in cases.items():
            with self.subTest(label):
                problems = self._problems(_export(**override))
                self.assertTrue(problems, label)
                self.assertFalse(any(token in p or "hunter2" in p for p in problems))
        masked = base + "password: XXXXXXXXXX\n"
        self.assertEqual(self._problems(_export(**{DB: masked})), [])

    def test_data_outside_the_allow_list_is_refused(self):
        health = _export()[HEALTH]
        cases = {
            "other database": {DB: "sqlalchemy_uri: duckdb:///md:x_intel\n"},
            "URI parameters": {
                DB: "sqlalchemy_uri: duckdb:///md:empire_gold?attach_mode=single\n"
            },
            "other table": {DS + "t.yaml": "table_name: fact_sales\nschema: jobs\n"},
            "other schema": {
                DS + "t.yaml": "table_name: jobs_board_health\nschema: main\n"
            },
            "load log": {DS + "t.yaml": "table_name: _gold_loads\nschema: jobs\n"},
            "unapproved column": {
                DS
                + "t.yaml": "table_name: jobs_company_hiring\nschema: jobs\ncolumns:\n- column_name: email\n"
            },
            "own SQL": {
                DS
                + "t.yaml": "table_name: q\nsql: select * from empire_gold.jobs.jobs_company_hiring\n"
            },
            "calculated column reads another table": {
                HEALTH: health.replace(
                    "postings_total * 1.0", "(select count(*) from x_intel.main.posts)"
                )
            },
            "metric names an unapproved column": {
                HEALTH: health.replace("SUM(postings_total)", "SUM(company_key)")
            },
            "metric with a qualified name": {
                HEALTH: health.replace(
                    "SUM(postings_total)", "SUM(jobs.postings_total)"
                )
            },
            "metric with a comment": {
                HEALTH: health.replace(
                    "SUM(postings_total)", "SUM(postings_total) -- x"
                )
            },
            "chart SQL metric reads another table": {
                CHART: "slice_name: x\ndataset_uuid: u-kpis\nparams:\n  metric:\n    sqlExpression: (select max(text) from posts)\n"
            },
            "chart filter in query_context": {
                CHART: (
                    "slice_name: x\ndataset_uuid: u-kpis\n"
                    'query_context: \'{"queries": [{"filters": [{"sqlExpression": "\\"handle\\" = 1"}]}]}\'\n'
                )
            },
            "chart names a column of another dataset": {
                CHART: "slice_name: x\ndataset_uuid: u-kpis\nparams:\n  metric:\n    sqlExpression: sum(postings_total)\n"
            },
            "chart where clause reads another table": {
                CHART: (
                    "slice_name: x\nuuid: c-jobs_kpis\ndataset_uuid: u-jobs_kpis\n"
                    'query_context: \'{"queries": [{"extras": {"where": "boards in (select 1 from posts)"}}]}\'\n'
                )
            },
            "chart having names another column": {
                CHART: "slice_name: x\nuuid: c-jobs_kpis\ndataset_uuid: u-jobs_kpis\nparams:\n  having: sum(email) > 0\n"
            },
            "dashboard misses a dataset": {
                "dashboard_export/charts/jobs_companies_30d.yaml": None
            },
            "dashboard chart not in the export": {
                DASHBOARD: 'dashboard_title: x\nposition: \'{"C": {"type": "CHART", "meta": {"uuid": "nope"}}}\'\n'
            },
            "SQL dataset with a changed literal": {
                DS + "jobs_companies_30d.yaml": _sql_dataset(
                    "jobs_companies_30d"
                ).replace("', '", "',  '")
            },
            "dataset on another database": {
                DS + "jobs_boards.yaml": _sql_dataset("jobs_boards").replace(
                    "u-db", "u-other"
                )
            },
            "filter options predicate": {
                HEALTH: _export()[HEALTH]
                + "fetch_values_predicate: board in (select handle from posts)\n"
            },
            "SQL dataset lists a column its SQL does not return": {
                KPIS: _sql_dataset("jobs_kpis") + "columns:\n- column_name: email\n"
            },
            "a second dashboard misses datasets": {
                "dashboard_export/dashboards/Other.yaml": "dashboard_title: Other\nposition: '{}'\n"
            },
            "SQL dataset with a block comment": {
                KPIS: _sql_dataset("jobs_kpis").replace(
                    "from as_of a", "from as_of a /* x */"
                )
            },
            # A double-quoted YAML string keeps a bare CR (a block scalar would fold it to \n).
            "SQL dataset hiding code behind a bare carriage return": {
                KPIS: "table_name: jobs_kpis\nschema: null\nuuid: u-jobs_kpis\ndatabase_uuid: u-db\nsql: "
                + json.dumps(
                    _indented_sql("jobs_kpis").replace(
                        "from as_of a",
                        "from as_of a -- note\r union all select 1, 2, 3, 4, 5, 6",
                    )
                )
                + "\n"
            },
            "chart on a dataset outside the export": {
                CHART: "slice_name: x\ndataset_uuid: other\n"
            },
            "not YAML": {"dashboard_export/charts/x.json": "{}"},
            "path traversal": {"../evil.yaml": "a: 1\n"},
            "no dashboard": {"dashboard_export/dashboards/Jobs.yaml": None},
        }
        for label, override in cases.items():
            with self.subTest(label):
                self.assertTrue(self._problems(_export(**override)), label)

    def test_windows_line_endings_still_match(self):
        from data_lake.preset_export_check import APPROVED_SQL, _normal_sql, _plain_sql
        from data_lake.preset_parity import dataset_sql

        crlf = dataset_sql("jobs_kpis").replace("\n", "\r\n")
        self.assertTrue(_plain_sql(crlf))
        self.assertIn(_normal_sql(crlf.replace("\r\n", "\n")), APPROVED_SQL)
        self.assertFalse(_plain_sql("select 1 -- x\rselect 2"))

    def test_problems_do_not_echo_export_values(self):
        problems = self._problems(
            _export(
                **{
                    HEALTH: _export()[HEALTH].replace(
                        "SUM(postings_total)", "SUM(hunter2col)"
                    )
                }
            )
        )
        self.assertTrue(problems)
        self.assertFalse(any("hunter2col" in p for p in problems))

    def test_fingerprint_needs_a_passing_committed_export(self):
        from data_lake import preset_export_check as check

        with TEMP() as out:
            directory = Path(out)
            with mock.patch.object(check, "EXPORT_DIR", directory):
                self.assertIsNone(check.export_fingerprint())
                (directory / "jobs-dashboard.zip").write_bytes(_zip(_export()))
                first = check.export_fingerprint()
                self.assertRegex(first or "", r"^[0-9a-f]{64}$")
                (directory / "jobs-dashboard.zip").write_bytes(
                    _zip(_export(**{DB: "sqlalchemy_uri: x\n"}))
                )
                self.assertIsNone(check.export_fingerprint())


if __name__ == "__main__":
    unittest.main()
