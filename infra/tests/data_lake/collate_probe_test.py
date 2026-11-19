#!/usr/bin/env python3
"""ADR-0016 P4: the Collate PostgreSQL-connector probe.

OpenMetadata's SQL is not in this repository (Collate Community License), so these
tests drive the probe with a synthetic ``queries.py`` written here. Set
COLLATE_PROBE_NETWORK=1 to also dry-run the real, pinned SQL on DuckDB.
"""

from __future__ import annotations

import hashlib
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

import jobs_marts_test  # noqa: E402
from jobs_marts_test import FIXTURE_JOBS, NOW, _ingest  # noqa: E402

TEMP = tempfile.TemporaryDirectory

# Our own stand-ins with the connector's shape: same names, placeholders and binds.
SYNTHETIC = '''
import textwrap
POSTGRES_GET_DATABASE = "select datname from pg_catalog.pg_database"
POSTGRES_GET_SCHEMA_NAMES = """
SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg\\\\_%' ORDER BY nspname
"""
POSTGRES_GET_TABLE_NAMES = """
SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = :schema
"""
POSTGRES_SCHEMA_COMMENTS = "select nspname, NULL from pg_catalog.pg_namespace"
POSTGRES_TABLE_COMMENTS = "select relname, NULL from pg_catalog.pg_class where 1=0"
TEST_COLUMN_METADATA = "select count(*) from pg_catalog.pg_attribute where 1=0"
TEST_TABLE_COMMENTS = "select count(*) from pg_catalog.pg_class where 1=0"
TEST_INFORMATION_SCHEMA_COLUMNS = "select count(*) from information_schema.columns where 1=0"
POSTGRES_TABLE_OWNERS = "select 1"
POSTGRES_VIEW_DEFINITIONS = "select no_such_function()"
POSTGRES_TEST_GET_TAGS = "select * from pg_policy"
POSTGRES_GET_ALL_TABLE_PG_POLICY = "select '{schema_name}', '{database_name}'"
POSTGRES_GET_STORED_PROCEDURES = "select '{schema_name}'"
POSTGRES_GET_FUNCTIONS = "select '{schema_name}'"
POSTGRES_COL_IDENTITY = "no_such_function() as identity_options"
POSTGRES_SQL_COLUMNS = textwrap.dedent("""
    SELECT a.attname, {generated}, {identity}
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = :table_oid AND a.attnum > 0
    ORDER BY a.attnum
""")
POSTGRES_FETCH_FK = "select 1 where :table > 0"
POSTGRES_PARTITION_DETAILS = "select 1 where :table_name = :schema_name"
NOT_A_QUERY = 42
'''


def _queries() -> dict[str, str]:
    from data_lake.collate_probe import extract_queries

    return extract_queries(SYNTHETIC)


class CollateProbeTests(unittest.TestCase):
    setUp = jobs_marts_test.JobsMartsTests.setUp

    def _warehouse(self, root: str, out: str) -> str:
        from data_lake.jobs_marts import run_jobs_marts
        from data_lake.motherduck_load import load_jobs_gold

        _ingest(root, FIXTURE_JOBS)
        gold = run_jobs_marts(data_lake_uri=root, max_invalid_ratio=0.25, now=NOW)
        target = str(Path(out) / "empire_gold.duckdb")
        load_jobs_gold(gold["manifest_key"], target, data_lake_uri=root, now=NOW)
        return target

    def test_extract_reads_strings_only_and_needs_every_query(self):
        from data_lake.collate_probe import ProbeError, extract_queries

        queries = _queries()
        self.assertIn("POSTGRES_SQL_COLUMNS", queries)
        self.assertNotIn("NOT_A_QUERY", queries)
        self.assertNotIn("\n    SELECT", queries["POSTGRES_SQL_COLUMNS"])  # dedented
        with self.assertRaises(ProbeError):
            extract_queries("POSTGRES_GET_DATABASE = 'select 1'")
        # A call that is not textwrap.dedent is never evaluated.
        with self.assertRaises(ProbeError):
            extract_queries(SYNTHETIC.replace('textwrap.dedent("""', 'print("""'))

    def test_download_must_match_the_pinned_sha256(self):
        from data_lake import collate_probe as probe

        data = SYNTHETIC.encode()
        with self.assertRaisesRegex(probe.ProbeError, "sha256"):
            probe.load_queries(fetch=lambda url: data)
        with mock.patch.object(
            probe, "QUERIES_SHA256", hashlib.sha256(data).hexdigest()
        ):
            seen: list[str] = []
            self.assertIn(
                "POSTGRES_SQL_COLUMNS",
                probe.load_queries(fetch=lambda url: seen.append(url) or data),
            )
        self.assertEqual(seen, [probe.QUERIES_URL])
        self.assertIn(probe.QUERIES_COMMIT, probe.QUERIES_URL)

    def test_render_follows_the_server_version(self):
        from data_lake.collate_probe import render

        queries = _queries()
        new = render(queries, "POSTGRES_SQL_COLUMNS", server_version=160002)
        self.assertIn("a.attgenerated as generated", new)
        self.assertIn("no_such_function() as identity_options", new)
        old = render(queries, "POSTGRES_SQL_COLUMNS", server_version=90600)
        self.assertIn("NULL as generated", old)
        self.assertIn("NULL as identity_options", old)
        self.assertIn(
            "'jobs', 'empire_gold'",
            render(queries, "POSTGRES_GET_ALL_TABLE_PG_POLICY", server_version=1),
        )

    def test_server_version_num(self):
        from data_lake.collate_probe import server_version_num

        self.assertEqual(server_version_num((16, 2)), 160002)
        self.assertEqual(server_version_num((9, 6, 24)), 90624)
        self.assertIsNone(server_version_num(None))

    def test_binds_per_driver_and_no_parameters_without_binds(self):
        from data_lake.collate_probe import Runner

        seen: list[tuple] = []
        pg = Runner(lambda sql, params: seen.append((sql, params)) or [], "psycopg")
        pg.rows(
            "select x::regclass from t where a = :schema and b like 'pg\\_%'",
            {"schema": "jobs", "unused": 1},
        )
        self.assertEqual(
            seen[-1],
            (
                "select x::regclass from t where a = %(schema)s and b like 'pg\\_%%'",
                {"schema": "jobs"},
            ),
        )
        pg.rows("select 1 where b like 'pg\\_%'", {"schema": "jobs"})
        self.assertEqual(seen[-1], ("select 1 where b like 'pg\\_%'", None))
        duck = Runner(lambda sql, params: seen.append((sql, params)) or [], "duckdb")
        duck.rows("select :table_name, c::text", {"table_name": "t"})
        self.assertEqual(seen[-1], ("select $table_name, c::text", {"table_name": "t"}))

    def test_catalog_on_duckdb_records_answers_and_judges_columns(self):
        from data_lake.collate_probe import probe_duckdb

        with TEMP() as root, TEMP() as out:
            target = self._warehouse(root, out)
            old = probe_duckdb(target, queries=_queries(), server_version=90600)
            self.assertTrue(old["viable"], old["required_failed"])
            self.assertIn("catalog.view_definitions", old["optional_failed"])
            new = probe_duckdb(target, queries=_queries(), server_version=160002)
            self.assertFalse(new["viable"])
            self.assertIn("catalog.jobs_board_health.columns", new["required_failed"])
            # Errors are exception types only.
            errors = [
                s.get("error", "") for s in new["stages"]["catalog"]["steps"].values()
            ]
            self.assertTrue(all(" " not in e for e in errors if e != "table not found"))

    def test_a_dropped_connection_is_inconclusive_not_a_verdict(self):
        from data_lake.collate_probe import Inconclusive, Runner, run_catalog

        class Dropped(Exception):
            pass

        def execute(sql, params):
            raise Dropped("server closed the connection")

        runner = Runner(execute, "psycopg", fatal=(Dropped,))
        with self.assertRaises(Inconclusive):
            run_catalog(runner, _queries(), server_version=160002)

    def test_dialect_stage_reads_the_version_and_classifies_like_the_connector(self):
        import contextlib

        from data_lake.collate_probe import run_dialect
        from data_lake.motherduck_load import EXPORT_APPROVED

        calls: list[str] = []

        class Inspector:
            unique_error: type[BaseException] | None = None

            def get_table_names(self, schema):
                return list(EXPORT_APPROVED)

            def get_view_names(self, schema):
                raise RuntimeError("no views")

            def get_columns(self, table, schema):
                calls.append("get_columns")
                raise AssertionError("stock get_columns must not be judged")

            def get_pk_constraint(self, table, schema):
                return {}

            def get_unique_constraints(self, table, schema):
                if self.unique_error:
                    raise self.unique_error("x")
                return []

            def get_indexes(self, table, schema):
                raise RuntimeError("no indexes")

        class Dialect:
            server_version_info = (9, 6, 24)
            enums_fail = False

            def _load_domains(self, conn):
                return {}

            def _load_enums(self, conn, schema=None):
                if self.enums_fail:
                    raise RuntimeError("no pg_enum")
                return []

        class Engine:
            dialect = Dialect()

            def connect(self):
                return contextlib.nullcontext(object())

        engine = Engine()
        inspector = Inspector()
        with mock.patch("sqlalchemy.inspect", return_value=inspector):
            result, version = run_dialect(engine)
        self.assertEqual(version, 90624)
        self.assertEqual(calls, [])
        steps = result["steps"]
        self.assertTrue(
            steps["dialect_connect"]["ok"]
            and steps["domains"]["ok"]
            and steps["enums"]["ok"]
        )
        self.assertFalse(
            steps["view_names"]["required"]
            or steps["jobs_board_health.indexes"]["required"]
        )
        # NotImplementedError on a constraint passes, as in the connector; an SQL error fails.
        inspector.unique_error = NotImplementedError
        with mock.patch("sqlalchemy.inspect", return_value=inspector):
            result, _ = run_dialect(engine)
        self.assertTrue(result["steps"]["jobs_board_health.unique"]["ok"])
        inspector.unique_error = RuntimeError
        engine.dialect.enums_fail = True
        with mock.patch("sqlalchemy.inspect", return_value=inspector):
            result, _ = run_dialect(engine)
        self.assertFalse(result["steps"]["jobs_board_health.unique"]["ok"])
        self.assertTrue(result["steps"]["jobs_board_health.unique"]["required"])
        self.assertFalse(result["steps"]["enums"]["ok"])
        with mock.patch(
            "sqlalchemy.inspect",
            side_effect=AssertionError("Could not determine version"),
        ):
            failed, version = run_dialect(engine)
        self.assertEqual(
            (failed["steps"]["dialect_connect"]["error"], version),
            ("AssertionError", None),
        )

    def test_partition_and_foreign_key_errors_are_required_failures(self):
        from data_lake.collate_probe import probe_duckdb

        queries = dict(
            _queries(),
            POSTGRES_PARTITION_DETAILS="select * from pg_partitioned_table where :table_name = :schema_name",
        )
        with TEMP() as root, TEMP() as out:
            result = probe_duckdb(
                self._warehouse(root, out), queries=queries, server_version=90600
            )
        self.assertFalse(result["viable"])
        self.assertIn("catalog.jobs_board_health.partitions", result["required_failed"])

    def test_test_connection_queries_do_not_decide(self):
        from data_lake.collate_probe import probe_duckdb

        queries = dict(
            _queries(),
            TEST_COLUMN_METADATA="select no_such_function()",
            POSTGRES_TABLE_COMMENTS="select no_such_function()",
        )
        with TEMP() as root, TEMP() as out:
            result = probe_duckdb(
                self._warehouse(root, out), queries=queries, server_version=90600
            )
        self.assertTrue(result["viable"], result["required_failed"])
        self.assertIn("catalog.test_column_metadata", result["optional_failed"])

    def test_cli_prints_a_verdict_or_a_type_only_failure(self):
        from data_lake import collate_probe as probe

        with mock.patch.dict(
            os.environ,
            {"MOTHERDUCK_READER_TOKEN": "tok-secret-123", "MOTHERDUCK_PG_HOST": ""},
        ):
            with mock.patch("sys.stdout", io.StringIO()) as buf:
                code = probe.main([])
        self.assertEqual(code, 2)
        self.assertEqual(
            json.loads(buf.getvalue()), {"status": "failed", "error": "ProbeError"}
        )
        with TEMP() as root, TEMP() as out:
            target = self._warehouse(root, out)
            with mock.patch.object(probe, "load_queries", return_value=_queries()):
                with mock.patch("sys.stdout", io.StringIO()) as buf:
                    code = probe.main(
                        ["--duckdb", target, "--assume-server-version", "90600"]
                    )
            summary = json.loads(buf.getvalue())
            self.assertEqual(
                (code, summary["status"], summary["viable"]), (0, "verdict", True)
            )

    def test_verdict_needs_required_steps_and_the_catalog_columns(self):
        from data_lake.collate_probe import verdict
        from data_lake.motherduck_load import EXPORT_APPROVED

        good = {
            "catalog": {
                "steps": {
                    "a": {"required": True, "ok": True},
                    "b": {"required": False, "ok": False, "error": "X"},
                },
                "found": {
                    "schema_listed": True,
                    "tables_listed": sorted(EXPORT_APPROVED),
                    "columns_match": {m: True for m in EXPORT_APPROVED},
                },
            },
            "dialect": {
                "steps": {"dialect_connect": {"required": True, "ok": True}},
                "found": {"tables_listed": sorted(EXPORT_APPROVED)},
            },
        }
        self.assertTrue(verdict(good)["viable"])
        self.assertEqual(verdict(good)["optional_failed"], ["catalog.b"])
        bad = json.loads(json.dumps(good))
        bad["dialect"]["steps"]["dialect_connect"] = {
            "required": True,
            "ok": False,
            "error": "X",
        }
        self.assertEqual(verdict(bad)["required_failed"], ["dialect.dialect_connect"])
        wrong = json.loads(json.dumps(good))
        wrong["catalog"]["found"]["columns_match"]["jobs_board_health"] = False
        self.assertFalse(verdict(wrong)["viable"])

    @unittest.skipUnless(
        os.environ.get("COLLATE_PROBE_NETWORK") == "1",
        "downloads the pinned OpenMetadata SQL",
    )
    def test_real_sql_dry_run(self):
        from data_lake.collate_probe import probe_duckdb

        with TEMP() as root, TEMP() as out:
            result = probe_duckdb(self._warehouse(root, out))
            self.assertIn(
                "catalog.jobs_board_health.columns", result["required_failed"]
            )


if __name__ == "__main__":
    unittest.main()
