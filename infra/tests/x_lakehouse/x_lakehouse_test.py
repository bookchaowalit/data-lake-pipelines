#!/usr/bin/env python3
"""Offline tests for infra/scripts/x_lakehouse (no Sheet, R2 or Supabase).

The Postgres tests run only when X_LAKE_TEST_DB_URL points at a throwaway database, e.g.
    docker run -d --rm -e POSTGRES_PASSWORD=t -p 127.0.0.1:55439:5432 postgres:17-alpine
    X_LAKE_TEST_DB_URL=postgresql://postgres:t@127.0.0.1:55439/postgres python3 -m unittest ...
"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

import pyarrow.parquet as pq  # noqa: E402

from x_lakehouse import backfill, validate  # noqa: E402
from x_lakehouse import curated as cur  # noqa: E402
from x_lakehouse.append import append_rows  # noqa: E402
from x_lakehouse.coerce import CoerceError, coerce, type_row  # noqa: E402
from x_lakehouse.lake import LakeError, LocalStore, read_dataset, write_raw  # noqa: E402
from x_lakehouse.schemas import (  # noqa: E402
    DATASETS, SERVE_TABS, X_FEED, X_POST_CONTEXT, X_SIGNALS, postgres_ddl,
)
from x_lakehouse.pipeline import DatasetCheck  # noqa: E402
from x_lakehouse.sheet import SheetError, SheetWriter, assert_writable  # noqa: E402

HEADERS = {
    **json.loads((ROOT / "infra/tests/fixtures/x_lakehouse_sheet_headers_2026-11-05.json").read_text())["headers"],
    # X Analyses / X Triage Audit joined the lakehouse on 2026-11-08 (headers read that day).
    **json.loads((ROOT / "infra/tests/fixtures/x_lakehouse_sheet_headers_2026-11-08.json").read_text())["headers"],
}
NOW = datetime(2026, 11, 5, 4, 0, tzinfo=timezone.utc)


def feed_row(post_id: str, collected: str = "2026-11-05 08:16", **kw: object) -> dict[str, object]:
    row: dict[str, object] = {
        "collected_at": collected, "requesting_chief": "Chief Data", "watch_type": "keyword",
        "watch_value": "duckdb", "post_id": post_id, "post_url": f"https://x.com/a/status/{post_id}",
        "author_handle": "duckdb", "posted_at": "2026-11-05 07:00", "text": "hello", "likes": 3,
        "reposts": 1, "replies": 0, "views": 120, "tag": "data", "query_id": "Q1", "score": 1.5, "top": "TRUE",
    }
    row.update(kw)
    return row


class ContractTests(unittest.TestCase):
    def test_contract_matches_the_sheet_headers_read_2026_11_05(self):
        for ds in DATASETS.values():
            self.assertEqual(HEADERS[ds.tab], ds.names, ds.tab)

    def test_ids_are_strings_in_every_schema(self):
        for ds in DATASETS.values():
            for name, kind in ds.columns:
                if name.endswith("post_id") or name in ("user_id", "last_since_id"):
                    self.assertEqual(kind, "str", f"{ds.name}.{name}")

    def test_ddl_has_primary_keys_and_indexes(self):
        ddl = postgres_ddl()
        self.assertIn("create table if not exists x_intel.x_signals", ddl)
        self.assertNotIn("create schema if not exists", ddl)
        self.assertIn("primary key (post_id)", ddl)
        self.assertIn("x_signals_domain_collected_at_idx", ddl)
        self.assertIn("x_signals_cred_tier_idx", ddl)
        self.assertIn("x_signals_created_at_idx", ddl)
        self.assertNotIn("insert into", ddl)


class CoerceTests(unittest.TestCase):
    def test_ict_wall_time_becomes_utc(self):
        self.assertEqual(coerce("2026-11-05 08:16", "ts"), datetime(2026, 11, 5, 1, 16, tzinfo=timezone.utc))

    def test_big_number_in_id_column_is_an_error_not_a_silent_loss(self):
        with self.assertRaises(CoerceError):
            coerce(2.1144072395787103e18, "str")
        self.assertEqual(coerce("2114407239578710111", "str"), "2114407239578710111")

    def test_bools_and_ints(self):
        self.assertIs(coerce("TRUE", "bool"), True)
        self.assertIs(coerce(False, "bool"), False)
        self.assertEqual(coerce(12.0, "int"), 12)
        with self.assertRaises(CoerceError):
            coerce("0; 0", "int")

    def test_bad_cell_is_logged_and_row_kept(self):
        row = type_row(X_FEED, feed_row("1", likes="lots", surprise="new column"))
        self.assertIsNone(row.values["likes"])
        self.assertEqual(row.errors["likes"]["raw"], "lots")
        self.assertEqual(row.extra, {"surprise": "new column"})

    def test_impossible_dates_and_int64_overflow_are_coercion_errors(self):
        row = type_row(X_FEED, feed_row("1", collected="2026-13-05 08:16", views="9223372036854775808"))
        self.assertIn("collected_at", row.errors)
        self.assertEqual(row.errors["views"]["reason"], "outside int64")
        with self.assertRaises(CoerceError):
            coerce(2**63, "int")

    def test_long_bad_value_is_kept_whole_in_coerce_errors(self):
        long = "x" * 5000
        self.assertEqual(type_row(X_FEED, feed_row("1", likes=long)).errors["likes"]["raw"], long)

    def test_post_context_needs_only_post_id(self):
        row = type_row(X_POST_CONTEXT, {"post_id": "9", "media_count": 1})
        self.assertEqual(row.errors, {})
        self.assertIn("post_id", type_row(X_POST_CONTEXT, {"ref_type": "quoted"}).errors)


class GuardTests(unittest.TestCase):
    def test_only_serving_tabs_are_writable(self):
        for tab in ("AI Summaries", "AI Summaries Latest", "Chief Finance", "Chief of Staff", "X Feed",
                    "X Signals", "Claims", "README"):
            with self.assertRaises(SheetError, msg=tab):
                assert_writable(tab)
        for tab in SERVE_TABS.values():
            assert_writable(tab)


class BackfillGuardTests(unittest.TestCase):
    def test_execute_refuses_before_any_target_is_opened_when_a_column_is_missing(self):
        chk = DatasetCheck("x_feed", "X Feed", missing_columns=["likes"])
        with patch.object(backfill, "load", return_value=({"x_feed": []}, {"x_feed": chk}, [])), \
                patch.object(backfill, "open_store") as open_store, patch.object(backfill.cur, "connect") as connect:
            self.assertEqual(backfill.main(["--execute", "--datasets", "x_feed"]), 2)
        open_store.assert_not_called()
        connect.assert_not_called()


    def test_scoped_backfill_reads_x_feed_for_context_dates(self):
        feed = [type_row(X_FEED, feed_row("66", collected="2026-11-01 10:00"))]
        ctx = [type_row(X_POST_CONTEXT, {"post_id": "66", "media_count": 1}, sheet_row=2)]

        def fake_load(cfg, names):
            if names == ["x_feed"]:
                return {"x_feed": feed}, {"x_feed": DatasetCheck("x_feed", "X Feed")}, []
            return {"x_post_context": ctx}, {"x_post_context": DatasetCheck("x_post_context", "X Post Context")}, []
        with tempfile.TemporaryDirectory() as tmp, patch.object(backfill, "load", side_effect=fake_load):
            self.assertEqual(backfill.main(["--datasets", "x_post_context", "--local-out", tmp]), 0)
            self.assertTrue((Path(tmp) / "raw/x_post_context/date=2026-11-01/part-0000.parquet").exists())
            self.assertFalse((Path(tmp) / "raw/x_feed").exists())  # lookup only


class ServeWriterTests(unittest.TestCase):
    def test_grid_grows_and_new_rows_land_before_old_rows_are_trimmed(self):
        w = SheetWriter.__new__(SheetWriter)
        w.cfg = type("C", (), {"sheet_id": "S"})()
        calls = []

        def fake(method, url, **kw):
            calls.append((method, url.split("/S")[-1], kw.get("json")))
            if method == "GET":
                return {"sheets": [{"properties": {"sheetId": 7, "title": SERVE_TABS["x_signals"],
                                                   "gridProperties": {"rowCount": 1000, "columnCount": 3}}}]}
            return {}
        w._call = fake
        w.replace_body(SERVE_TABS["x_signals"], [["a", "b", "c"]] * 2930)
        kinds = [(m, u.split("?")[0][-12:]) for m, u, _ in calls]
        self.assertEqual(kinds[1][0], "POST")  # resize
        self.assertEqual(calls[1][2]["requests"][0]["updateSheetProperties"]["properties"]["gridProperties"]["rowCount"], 2931)
        self.assertEqual(calls[2][0], "PUT")  # write rows 2..
        self.assertEqual(len(calls), 3)  # grid was smaller than the data: nothing stale to clear
        calls.clear()
        w.replace_body(SERVE_TABS["x_signals"], [["a", "b", "c"]] * 10)
        self.assertEqual([c[0] for c in calls], ["GET", "PUT", "POST"])
        self.assertIn("A12%3AZZ", calls[2][1])  # only the old rows below the new body are cleared


class RawLayerTests(unittest.TestCase):
    def test_local_put_is_atomic_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as d:
            store = LocalStore(Path(d))
            with patch("os.link", side_effect=OSError("crash")):
                with self.assertRaises(OSError):
                    store.put_new("raw/a/part-0000.parquet", b"x")
            self.assertEqual(store.list("raw/"), [])  # nothing under the final name, no temp left
            store.put_new("raw/a/part-0000.parquet", b"x")
            with self.assertRaises(LakeError):
                store.put_new("raw/a/part-0000.parquet", b"y")
            self.assertEqual(store.read("raw/a/part-0000.parquet"), b"x")

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(Path(self.tmp.name))

    def tearDown(self):
        self.tmp.cleanup()

    def _write(self, rows, execute=True, batch="b1"):
        typed = [type_row(X_FEED, r) for r in rows]
        return write_raw(X_FEED, typed, [t.values["collected_at"] for t in typed], store=self.store, prefix="p",
                         source="test", execute=execute, run_date=date(2026, 11, 5), batch_id=batch, now=NOW)

    def test_layout_schema_and_zstd(self):
        rep = self._write([feed_row("1"), feed_row("2", collected="2026-11-04 23:59")])
        self.assertEqual([p.key for p in rep.partitions],
                         ["p/raw/x_feed/date=2026-11-04/part-0000.parquet",
                          "p/raw/x_feed/date=2026-11-05/part-0000.parquet"])
        f = pq.ParquetFile(io.BytesIO(self.store.read(rep.partitions[0].key)))
        self.assertEqual(f.schema_arrow.field("post_id").type, "string")
        self.assertEqual(f.metadata.row_group(0).column(0).compression, "ZSTD")
        self.assertTrue(self.store.list("p/_manifests/x_feed/"))

    def test_rerun_is_a_noop_and_resume_writes_only_missing_rows(self):
        self._write([feed_row("1")])
        again = self._write([feed_row("1"), feed_row("2")], batch="b2")
        self.assertEqual((again.written, again.skipped), (1, 1))
        self.assertEqual(again.partitions[0].key, "p/raw/x_feed/date=2026-11-05/part-0001.parquet")
        self.assertEqual(read_dataset(self.store, "p", X_FEED).num_rows, 2)
        third = self._write([feed_row("1"), feed_row("2")], batch="b3")
        self.assertEqual(third.written, 0)

    def test_undated_context_row_is_not_rewritten_on_a_later_day(self):
        rows = [type_row(X_POST_CONTEXT, {"post_id": "9", "media_count": 1})]
        for day in (date(2026, 11, 5), date(2026, 11, 6)):
            rep = write_raw(X_POST_CONTEXT, rows, [None], store=self.store, prefix="", source="t", execute=True,
                            run_date=day, now=NOW)
        self.assertEqual((rep.written, rep.skipped), (0, 1))
        self.assertEqual(read_dataset(self.store, "", X_POST_CONTEXT).num_rows, 1)

    def test_context_row_is_not_rewritten_when_its_feed_date_appears_later(self):
        rows = [type_row(X_POST_CONTEXT, {"post_id": "9", "media_count": 1})]
        write_raw(X_POST_CONTEXT, rows, [None], store=self.store, prefix="", source="t", execute=True,
                  run_date=date(2026, 11, 6), now=NOW)
        older = datetime(2026, 11, 1, 3, 0, tzinfo=timezone.utc)
        rep = write_raw(X_POST_CONTEXT, rows, [older], store=self.store, prefix="", source="t", execute=True,
                        run_date=date(2026, 11, 7), now=NOW)
        self.assertEqual((rep.written, rep.skipped), (0, 1))

    def _reconcile(self, sheet_rows, ds, since):
        cfg = type("C", (), {"db_url": ""})()
        chk = DatasetCheck(ds.name, ds.tab)
        with patch.object(validate, "load", return_value=({ds.name: sheet_rows}, {ds.name: chk}, [])), \
                patch.object(validate, "open_store", return_value=(self.store, "")):
            return validate.reconcile(cfg, [ds.name], since, None)["datasets"][ds.name]

    def test_validate_scopes_context_rows_by_their_stored_partition(self):
        rows = [type_row(X_POST_CONTEXT, {"post_id": "9", "media_count": 1}, sheet_row=2)]
        write_raw(X_POST_CONTEXT, rows, [None], store=self.store, prefix="", source="t", execute=True,
                  run_date=date(2026, 11, 5), now=NOW)
        later = self._reconcile(rows, X_POST_CONTEXT, date(2026, 11, 6))
        self.assertEqual((later["sheet_rows"], later["ok"]), (0, True))
        same = self._reconcile(rows, X_POST_CONTEXT, date(2026, 11, 5))
        self.assertEqual((same["sheet_rows"], same["sheet_rows_missing_from_raw"], same["ok"]), (1, 0, True))
        unstored = [type_row(X_POST_CONTEXT, {"post_id": "10"}, sheet_row=3)]
        self.assertEqual(self._reconcile(rows + unstored, X_POST_CONTEXT, date(2026, 11, 6))
                         ["sheet_rows_missing_from_raw"], 1)

    def test_validate_fails_on_coercion_errors_that_exist_only_in_raw(self):
        bad = type_row(X_FEED, feed_row("5", likes="lots"))
        write_raw(X_FEED, [bad], [bad.values["collected_at"]], store=self.store, prefix="", source="append",
                  execute=True, run_date=date(2026, 11, 5), now=NOW)
        r = self._reconcile([], X_FEED, None)
        self.assertFalse(r["ok"])
        self.assertTrue(any("raw rows with coercion errors" in x for x in r["reasons"]))

    def test_every_part_is_listed_by_a_manifest_written_before_it(self):
        order = []
        real_put = self.store.put_new
        self.store.put_new = lambda k, d: (order.append(k), real_put(k, d))[1]
        self._write([feed_row("1"), feed_row("2", collected="2026-11-04 10:00")], batch="bx")
        parts = [k for k in order if k.endswith(".parquet")]
        self.assertEqual(len(parts), 2)
        for part in parts:
            entries = [k for k in order[:order.index(part)] if "/_manifests/x_feed/bx/" in k]
            self.assertTrue(any(json.loads(self.store.read(e))["key"] == part for e in entries), part)

    def test_validate_refuses_without_db_url_unless_raw_only(self):
        with patch.dict(os.environ, {"X_LAKE_DB_URL": ""}), self.assertRaises(SystemExit):
            validate.main(["--datasets", "x_signals"])

    def test_dry_run_writes_nothing(self):
        self._write([feed_row("1")], execute=False)
        self.assertEqual(self.store.list(""), [])

    def test_part_number_of_a_crashed_batch_is_not_reused(self):
        real_put = self.store.put_new

        def crash_on_part(key, data):
            if key.endswith(".parquet"):
                raise OSError("crash after manifest")
            real_put(key, data)

        rows = [type_row(X_FEED, feed_row("1"))]
        ts = [datetime(2026, 11, 5, 3, tzinfo=timezone.utc)]
        with patch.object(self.store, "put_new", crash_on_part), self.assertRaises(OSError):
            write_raw(X_FEED, rows, ts, store=self.store, prefix="", source="t", execute=True,
                      run_date=date(2026, 11, 5), batch_id="b1")
        rep = write_raw(X_FEED, rows, ts, store=self.store, prefix="", source="t", execute=True,
                        run_date=date(2026, 11, 5), batch_id="b2")
        self.assertTrue(rep.partitions[0].key.endswith("part-0001.parquet"))  # 0000 stays b1's
        b1 = [k for k in self.store.list("_manifests/") if "/b1/" in k]
        self.assertTrue(b1 and b1[0].endswith("part-0000.json"))

    def test_parts_are_never_overwritten(self):
        self.store.put_new("k", b"1")
        with self.assertRaises(LakeError):
            self.store.put_new("k", b"2")
        self.assertEqual(self.store.read("k"), b"1")
        with self.assertRaises(LakeError):
            write_raw(X_FEED, [type_row(X_FEED, feed_row("1"))], [None], store=None, prefix="", source="t",
                      execute=True, run_date=date(2026, 11, 5))


class ScheduledTests(unittest.TestCase):
    """The cron wrapper logs counts only and fails (with an ok:false heartbeat) on any problem."""

    def _fake_backfill(self, argv):
        print(json.dumps({"checks": {"x_feed": {"missing_columns": []}},
                          "raw": {"x_feed": {"partitions": [{"to_write": 3, "already_present": 5}]}},
                          "curated": {}, "missing_tabs": []}))
        print("SECRET ROW TEXT", file=sys.stderr)
        return 0

    def _run(self, ok: bool):
        from x_lakehouse import scheduled
        report = {"ok": ok, "datasets": {"x_feed": {"ok": ok, "reasons": [] if ok else ["1 Sheet rows missing from raw"],
                                                     "sheet_rows": 8}}}
        beats = []
        out = io.StringIO()
        with patch.object(scheduled.backfill, "main", self._fake_backfill), \
                patch.object(scheduled, "reconcile", return_value=report), \
                patch.object(scheduled, "_serve_counts", return_value={}), \
                patch.object(scheduled, "_heartbeat", beats.append), \
                patch("sys.stdout", out), patch("sys.stderr", io.StringIO()):
            code = scheduled.main([])
        return code, beats, out.getvalue()

    def test_ok_run(self):
        code, beats, out = self._run(True)
        self.assertEqual((code, beats), (0, [True]))
        self.assertIn('"new": 3', out)

    def test_validate_failure_fails_the_run(self):
        code, beats, _ = self._run(False)
        self.assertEqual((code, beats), (1, [False]))

    def test_crash_sends_failed_heartbeat(self):
        from x_lakehouse import scheduled
        beats = []
        with patch.object(scheduled, "_backfill", side_effect=RuntimeError("boom")), \
                patch.object(scheduled, "_heartbeat", beats.append):
            with self.assertRaises(RuntimeError):
                scheduled.main([])
        self.assertEqual(beats, [False])


@unittest.skipUnless(os.environ.get("X_LAKE_TEST_DB_URL"), "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class PostgresTests(unittest.TestCase):
    def setUp(self):
        self.conn = cur.connect(os.environ["X_LAKE_TEST_DB_URL"])
        with self.conn.cursor() as c:
            c.execute("drop schema if exists x_intel cascade")
        self.conn.commit()
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(Path(self.tmp.name))

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def _append(self, rows, now=NOW):
        return append_rows(rows, store=self.store, prefix="", conn=self.conn, now=now)

    def test_append_upserts_and_joins_feed(self):
        sig = {h: "" for h in X_SIGNALS.names} | {"post_id": "77", "collected_at": "2026-11-05 08:20",
                                                  "domain": "D5 Technology", "cred_tier": "T2", "alert_tier": "none",
                                                  "opportunity_score": 3}
        first = self._append({"x_feed": [feed_row("77")], "x_signals": [sig]})
        self.assertEqual(first["curated"]["x_signals"], {"rows": 1, "inserted": 1, "updated": 0})
        self.assertEqual(first["raw"]["x_signals"]["written"], 1)
        same = self._append({"x_signals": [sig]})  # no feed row this time: joined columns must survive
        self.assertEqual(same["curated"]["x_signals"], {"rows": 1, "inserted": 0, "updated": 0})
        changed = self._append({"x_signals": [sig | {"cred_tier": "T1"}]})
        self.assertEqual(changed["curated"]["x_signals"]["updated"], 1)
        with self.conn.cursor() as c:
            c.execute("select cred_tier, author_handle, post_text from x_intel.x_signals where post_id = '77'")
            self.assertEqual(c.fetchone(), ("T1", "duckdb", "hello"))

    def test_signal_appended_after_its_post_gets_the_stored_feed_facts(self):
        self._append({"x_feed": [feed_row("88")]})
        sig = {h: "" for h in X_SIGNALS.names} | {"post_id": "88", "collected_at": "2026-11-05 09:00"}
        self._append({"x_signals": [sig]})
        with self.conn.cursor() as c:
            c.execute("select author_handle, post_text, views from x_intel.x_signals where post_id = '88'")
            self.assertEqual(c.fetchone(), ("duckdb", "hello", 120))

    def test_replaying_an_older_claim_does_not_roll_back_the_newer_one(self):
        base = {h: "" for h in DATASETS["claims"].names} | {"claim_id": "CL-2", "claim": "x",
                                                           "last_checked": "2026-11-05 08:01", "queue_state": "queued"}
        newer = base | {"last_checked": "2026-11-05 20:01", "queue_state": "checking"}
        self._append({"claims": [base]})
        self._append({"claims": [newer]})
        retry = self._append({"claims": [base]})
        self.assertEqual(retry["curated"]["claims"]["updated"], 0)
        self._append({"claims": [newer, base]})  # same batch, older last: newest still wins
        with self.conn.cursor() as c:
            c.execute("select queue_state from x_intel.claims where claim_id = 'CL-2'")
            self.assertEqual(c.fetchone(), ("checking",))

    def test_validate_fails_on_raw_rows_missing_from_postgres_and_on_coercion_errors(self):
        cur.ensure_schema(self.conn)
        sig = type_row(X_SIGNALS, {"post_id": "99", "collected_at": "2026-11-05 09:00", "opportunity_score": "x"})
        write_raw(X_SIGNALS, [sig], [sig.values["collected_at"]], store=self.store, prefix="", source="append",
                  execute=True, run_date=date(2026, 11, 5), now=NOW)
        cfg = type("C", (), {"db_url": os.environ["X_LAKE_TEST_DB_URL"]})()
        chk = DatasetCheck("x_signals", "X Signals")
        with patch.object(validate, "load", return_value=({"x_signals": [sig]}, {"x_signals": chk}, [])), \
                patch.object(validate, "open_store", return_value=(self.store, "")):
            rep = validate.reconcile(cfg, ["x_signals"], None, None)
        r = rep["datasets"]["x_signals"]
        self.assertFalse(rep["ok"])
        self.assertEqual(r["postgres_missing_keys"], 1)
        self.assertTrue(any("coercion" in x for x in r["reasons"]))

    def test_context_appended_alone_lands_in_its_posts_feed_partition(self):
        self._append({"x_feed": [feed_row("66", collected="2026-11-04 10:00")]})
        rep = self._append({"x_post_context": [{"post_id": "66", "media_count": 1}]})
        self.assertEqual(rep["raw"]["x_post_context"]["parts"],
                         ["raw/x_post_context/date=2026-11-04/part-0000.parquet"])

    def test_validate_flags_a_newer_raw_version_that_postgres_missed(self):
        base = {h: "" for h in DATASETS["claims"].names} | {"claim_id": "CL-3", "claim": "x",
                                                           "last_checked": "2026-11-05 08:01"}
        self._append({"claims": [base]})
        newer = type_row(DATASETS["claims"], base | {"last_checked": "2026-11-05 20:01"})
        write_raw(DATASETS["claims"], [newer], [newer.values["last_checked"]], store=self.store, prefix="",
                  source="append", execute=True, run_date=date(2026, 11, 5), now=NOW)  # upsert "failed"
        cfg = type("C", (), {"db_url": os.environ["X_LAKE_TEST_DB_URL"]})()
        chk = DatasetCheck("claims", "Claims")
        with patch.object(validate, "load", return_value=({"claims": []}, {"claims": chk}, [])), \
                patch.object(validate, "open_store", return_value=(self.store, "")):
            r = validate.reconcile(cfg, ["claims"], None, None)["datasets"]["claims"]
        self.assertEqual((r["postgres_missing_keys"], r["postgres_stale_versions"], r["ok"]), (0, 1, False))

    def test_claims_keep_latest_version_in_postgres_and_every_version_in_raw(self):
        base = {h: "" for h in DATASETS["claims"].names} | {"claim_id": "CL-1", "claim": "x", "first_post_id": "5",
                                                           "last_checked": "2026-11-05 08:01", "queue_state": "queued"}
        self._append({"claims": [base]})
        self._append({"claims": [base | {"last_checked": "2026-11-05 20:01", "queue_state": "checking"}]})
        with self.conn.cursor() as c:
            c.execute("select queue_state from x_intel.claims")
            self.assertEqual(c.fetchall(), [("checking",)])
        self.assertEqual(read_dataset(self.store, "", DATASETS["claims"]).num_rows, 2)


if __name__ == "__main__":
    unittest.main()
