#!/usr/bin/env python3
"""Tests for the bot inbox (x_lakehouse.inbox, bot_sql, scheduled X_LAKE_SOURCE modes).

T1..T11 are the acceptance tests of the 2026-11-08 cutover plan. The Postgres tests run only when
X_LAKE_TEST_DB_URL points at a throwaway database (they drop schema x_intel), e.g.
    docker run -d --rm -e POSTGRES_PASSWORD=t -p 127.0.0.1:55439:5432 postgres:17-alpine
    X_LAKE_TEST_DB_URL=postgresql://postgres:t@127.0.0.1:55439/postgres python3 -m unittest ...
"""

from __future__ import annotations

import io
import json
import os
import random
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import bot_sql, inbox, scheduled, validate  # noqa: E402
from x_lakehouse import curated as cur  # noqa: E402
from x_lakehouse.coerce import row_key, type_row  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402
from x_lakehouse.lake import LocalStore, read_dataset  # noqa: E402
from x_lakehouse.schemas import DATASETS, SERVE_TABS, X_FEED, X_SIGNALS  # noqa: E402
from x_lakehouse.sheet import SheetError, assert_writable  # noqa: E402

DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
NOW = datetime(2026, 11, 13, 9, 30, tzinfo=timezone.utc)  # 16:30 ICT


def feed_row(post_id: str, collected: str = "2026-11-13 08:16", **kw: object) -> dict[str, object]:
    row: dict[str, object] = {
        "collected_at": collected, "requesting_chief": "Chief Data", "watch_type": "keyword",
        "watch_value": "duckdb", "post_id": post_id, "post_url": f"https://x.com/a/status/{post_id}",
        "author_handle": "duckdb", "posted_at": "2026-11-13 07:00", "text": "hello", "likes": 3,
        "reposts": 1, "replies": 0, "views": 120, "tag": "data", "query_id": "Q1", "score": 1.5, "top": "TRUE",
    }
    row.update(kw)
    return row


def signal_row(post_id: str, collected: str = "2026-11-13 08:20", **kw: object) -> dict[str, object]:
    return {"post_id": post_id, "collected_at": collected, "domain": "D5 Technology", "cred_tier": "T2",
            "opportunity_score": 3, **kw}


def claim_row(claim_id: str, last_checked: str, **kw: object) -> dict[str, object]:
    return {"claim_id": claim_id, "claim": "x", "last_checked": last_checked, "queue_state": "queued", **kw}


# Values that stress the SQL twin of coerce.raw_text: Python strip() whitespace, Thai, ids beyond 2^53
# (as text and as exact JSON integers), whole floats, booleans, blanks.
TRICKY = ["2026-11-13 08:16", " 2026-11-13 08:16　", "\xa0Q7\t", "สวัสดี ครับ", "emoji 😀", "a$json$b",
          "2114407239578710300", 2114407239578710300, 9007199254740993, 2**63 - 1, -0.0, 3.0, 7, 1.5, 0.0001,
          True, False, "TRUE", "", None, "  ", " x ", "\x1cx\x1f", "0012", "1,234"]


def fixture_rows(ds_name: str, n: int = 50, seed: int = 7) -> list[dict[str, object]]:
    rnd = random.Random(f"{seed}-{ds_name}")
    ds = DATASETS[ds_name]
    out = []
    for i in range(n):
        row: dict[str, object] = {}
        for c in ds.names:
            row[c] = rnd.choice(TRICKY) if rnd.random() < 0.7 else f"{c}-{i}"
        if ds_name == "x_post_context" and i % 3 == 0:  # media-only rows: blank ref keys
            row["ref_type"] = row["ref_post_id"] = ""
        out.append(row)
    return out


class BotSqlTests(unittest.TestCase):
    """T7 (offline half) and T10: the bot helper's built-in header equals the contract."""

    def test_t10_headers_match_the_contract(self):
        for n, ds in DATASETS.items():
            self.assertEqual(list(bot_sql.HEADERS[n]), ds.names, n)
        self.assertEqual(set(bot_sql.HEADERS), set(DATASETS))

    def test_t10_new_tabs_match_the_live_headers_read_2026_11_08(self):
        hdr = json.loads((ROOT / "infra/tests/fixtures/x_lakehouse_sheet_headers_2026-11-08.json").read_text())
        for tab, names in hdr["headers"].items():
            ds = next(d for d in DATASETS.values() if d.tab == tab)
            self.assertEqual(ds.names, names)

    def test_t7_chunks_respect_row_and_byte_limits_and_quote_safely(self):
        rows = [feed_row(str(10**18 + i), text=f"ข่าว {i} 🚀 $json$ $$ ' \\ " + "ก" * (i % 400))
                for i in range(2300)]
        stmts = bot_sql.chunks("x_feed", bot_sql.as_dicts("x_feed", rows), "run1113m")
        self.assertGreaterEqual(len(stmts), 5)
        total = 0
        for s in stmts:
            self.assertLessEqual(len(s.encode()), bot_sql.MAX_BYTES)
            tag = s.split("', ", 1)[1].split("[", 1)[0]
            payload = s[s.index(tag) + len(tag):s.rindex(tag)]
            self.assertNotIn(tag, payload)
            self.assertNotEqual(tag, "$json$")  # the payload holds $json$, so another tag was chosen
            got = json.loads(payload)
            self.assertLessEqual(len(got), bot_sql.MAX_ROWS)
            total += len(got)
        self.assertEqual(total, 2300)

    def test_list_rows_use_the_sheet_order_and_bad_input_is_refused(self):
        rec = bot_sql.as_dicts("x_triage_audit", [["2026-W45", "2026-11-03 00:17", 30]])[0]
        self.assertEqual(rec, {"audit_week": "2026-W45", "logged_at": "2026-11-03 00:17", "n": 30})
        with self.assertRaises(bot_sql.BotSqlError):
            bot_sql.as_dicts("x_triage_audit", [list(range(11))])
        with self.assertRaises(bot_sql.BotSqlError):
            bot_sql.statement("x_feed", [], "run'; drop table x")
        with self.assertRaises(bot_sql.BotSqlError):
            bot_sql.as_dicts("nope", [])
        self.assertEqual(bot_sql.as_dicts("x_feed", [{"text": "a\x00b"}]), [{"text": "ab"}])

    def test_write_files_names_by_dataset_and_run(self):
        with tempfile.TemporaryDirectory() as d:
            paths = bot_sql.write_files("claims", [claim_row("CL-1", "2026-11-13 08:00")], "run1113m", d)
            self.assertEqual([p.name for p in paths], ["claims-run1113m-000.sql"])
            self.assertTrue(paths[0].read_text().startswith("select x_intel.ingest('claims', $json$"))


class GuardTests(unittest.TestCase):
    """T8: the serving tabs are the only writable tabs; source and protected tabs stay refused."""

    def test_t8_only_serving_tabs_are_writable(self):
        for tab in SERVE_TABS.values():
            assert_writable(tab)
        for tab in ("X Query Plan", "X Feed", "AI Summaries", "AI Summaries Latest", "Chief Data", "README"):
            with self.assertRaises(SheetError):
                assert_writable(tab)

    def test_inbox_ddl_is_self_contained_and_grants_nothing_to_public_roles(self):
        ddl = inbox.inbox_ddl()
        self.assertIn("security definer", ddl)
        self.assertIn("revoke all on function x_intel.ingest(text, jsonb, text) from public", ddl)
        self.assertIn("grant usage on schema x_intel to postgres", ddl)
        self.assertNotIn("to anon", ddl)
        self.assertNotIn("to authenticated", ddl)
        for t in ("x_signals", "claims", "x_query_plan", "x_watchlist", "taxonomy_map"):
            self.assertIn(f"_ingest_upsert_{t}", ddl)

    def test_unknown_source_is_refused(self):
        with self.assertRaises(ValueError):
            scheduled._source(Config(env={"X_LAKE_SOURCE": "lake"}))


@unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class InboxPostgresTests(unittest.TestCase):
    def setUp(self):
        self.conn = cur.connect(DB)
        with self.conn.cursor() as c:
            c.execute("drop schema if exists x_intel cascade")
        self.conn.commit()
        inbox.ensure_inbox(self.conn)
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(Path(self.tmp.name))

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()
        self.tmp.cleanup()

    def ingest(self, dataset, rows, run_id="run1113m"):
        with self.conn.cursor() as c:
            c.execute("select x_intel.ingest(%s, %s::jsonb, %s)", (dataset, json.dumps(rows), run_id))
            out = c.fetchone()[0]
        self.conn.commit()
        return out

    def q(self, sql, *args):
        with self.conn.cursor() as c:
            c.execute(sql, args)
            rows = c.fetchall()
        self.conn.commit()
        return rows

    def env(self, **extra):
        return {"X_LAKE_DB_URL": DB, "X_LAKE_URI": f"file://{self.tmp.name}", "X_LAKE_SOURCE": "inbox", **extra}

    def run_scheduled(self, **extra):
        out, beats = io.StringIO(), []
        with patch.dict(os.environ, self.env(**extra)), patch.object(scheduled, "_heartbeat", beats.append), \
                patch.object(scheduled, "_serve_counts", return_value={}), patch("sys.stdout", out):
            code = scheduled.main([])
        events = [json.loads(line) for line in out.getvalue().splitlines() if line.startswith("{")]
        return code, beats, {e["event"]: e for e in events}

    # -- T1 / T9 ---------------------------------------------------------------------------------
    def test_t1_ingest_is_idempotent(self):
        rows = [feed_row(str(10**18 + i)) for i in range(600)]
        first = self.ingest("x_feed", rows)
        second = self.ingest("x_feed", rows)
        self.assertEqual((first["inserted"], first["skipped"], first["errors"]), (600, 0, []))
        self.assertEqual((second["inserted"], second["skipped"]), (0, 600))
        self.assertEqual(self.q("select count(*) from x_intel.ingest_inbox")[0][0], 600)

    def test_a_post_collected_again_keeps_one_feed_row_and_validates(self):
        # 2026-11-14: the bot sent posts it had collected the evening before again, with a new collected_at
        # (a new row key), and validate failed on duplicate post_id in raw.
        first = self.ingest("x_feed", [feed_row("7"), feed_row("8"), feed_row("8", query_id="Q2")], "run1113e")
        self.assertEqual((first["inserted"], first["duplicate_posts"], first["errors"]), (2, 1, []))
        code, _, ev = self.run_scheduled()
        self.assertEqual(code, 0, ev)
        again = self.ingest("x_feed", [feed_row("7", collected="2026-11-14 08:00", likes=9), feed_row("9")],
                            "run1114m")
        self.assertEqual((again["inserted"], again["skipped"], again["duplicate_posts"]), (1, 1, 1))
        self.assertEqual(self.ingest("x_feed", [feed_row("9")], "run1114m")["duplicate_posts"], 0)  # a retry
        code, _, ev = self.run_scheduled()
        self.assertEqual(code, 0, ev)
        self.assertTrue(ev["validate"]["ok"], ev["validate"])
        ids = sorted(read_dataset(self.store, "", X_FEED).column("post_id").to_pylist())
        self.assertEqual(ids, ["7", "8", "9"])

    def test_t9_bad_dataset_or_column_is_reported_and_inserts_nothing_for_that_row(self):
        out = self.ingest("x_nope", [feed_row("1")])
        self.assertEqual(out["errors"][0]["error"], "unknown dataset")
        out = self.ingest("x_feed", [feed_row("1", bogus="x"), feed_row("2"), "not a row",
                                     feed_row("3", text={"nested": 1}), feed_row("", query_id="")])
        self.assertEqual(out["inserted"], 1)
        self.assertEqual([(e["row"], e["error"]) for e in out["errors"]],
                         [(1, "unknown columns"), (3, "row is not an object"), (4, "nested values"),
                          (5, "key column is blank")])
        self.assertEqual(out["errors"][0]["columns"], ["bogus"])
        self.assertEqual(self.q("select row ->> 'post_id' from x_intel.ingest_inbox"), [("2",)])
        out = self.ingest("x_feed", [feed_row("4")], run_id="bad run id")
        self.assertEqual(out["inserted"], 0)
        self.assertEqual(self.ingest("x_feed", {"not": "an array"})["errors"][0]["row"], None)

    # -- T2 --------------------------------------------------------------------------------------
    def test_t2_sql_row_key_equals_python_row_key_for_every_dataset(self):
        for n, ds in DATASETS.items():
            for row in fixture_rows(n):
                with self.conn.cursor() as c:
                    c.execute("select x_intel.row_key(%s, %s, %s::jsonb), %s::jsonb::text",
                              (n, list(ds.key), json.dumps(row), json.dumps(row)))
                    sql_key, pg_text = c.fetchone()
                # The drain types what psycopg reads back from jsonb, so that is the key to match.
                self.assertEqual(sql_key, row_key(ds, json.loads(pg_text)), f"{n}: {row}")
                if not any(isinstance(v, float) for v in row.values()):
                    self.assertEqual(sql_key, row_key(ds, row), f"{n}: {row}")
        self.conn.commit()

    def test_ingested_row_keys_are_the_drains_keys(self):
        rows = fixture_rows("x_post_context") + [{"post_id": "9007199254740993", "media_count": 1}]
        rows = [r for r in rows if str(r.get("post_id") or "").strip(inbox._WS_CHARS)]
        self.ingest("x_post_context", rows)
        stored = self.q("select row_key, row from x_intel.ingest_inbox")
        self.assertTrue(stored)
        for rk, row in stored:
            self.assertEqual(rk, type_row(DATASETS["x_post_context"], row).row_key)

    # -- T3 --------------------------------------------------------------------------------------
    def test_t3_versions_and_book_checked_claims_are_protected(self):
        self.ingest("x_feed", [feed_row("77")])
        self.ingest("x_signals", [signal_row("77", "2026-11-13 09:00", cred_tier="T1")])
        out = self.ingest("x_signals", [signal_row("77", "2026-11-13 08:00", cred_tier="T3")])
        self.assertEqual(out["upserted"]["updated"], 0)
        self.assertEqual(self.q("select cred_tier, author_handle, post_text, views from x_intel.x_signals"),
                         [("T1", "duckdb", "hello", 120)])

        self.ingest("claims", [claim_row("CL-1", "2026-11-13 08:00", checked_by="book",
                                         verification_status="confirmed")])
        bot = claim_row("CL-1", "2026-11-13 20:00", checked_by="rule", verification_status="false")
        self.assertEqual(self.ingest("claims", [bot])["upserted"]["updated"], 0)
        # The drain (Python upsert of the same inbox rows) must keep Book's claim too.
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.assertEqual(self.q("select verification_status, checked_by from x_intel.claims"),
                         [("confirmed", "book")])
        book = claim_row("CL-1", "2026-11-13 21:00", checked_by="Book", verification_status="refuted")
        self.assertEqual(self.ingest("claims", [book])["upserted"]["updated"], 1)
        self.assertEqual(read_dataset(self.store, "", DATASETS["claims"]).num_rows, 2)  # raw keeps all versions

    def test_review7_a_config_date_python_cannot_read_is_refused(self):
        wl = {k: "x" for k in DATASETS["x_watchlist"].key}
        for bad in ("today", "11/13/2026", "Nov 13 2026"):
            out = self.ingest("x_watchlist", [wl | {"added_date": bad}], run_id="d" + str(len(bad)))
            self.assertEqual(out["errors"][0]["columns"], ["added_date"], bad)
        out = self.ingest("x_watchlist", [wl | {"added_date": "2026-11-13"}], run_id="dok")
        self.assertEqual(out["errors"], [])

    def test_review6_a_config_timestamp_sql_cannot_read_is_refused_not_nulled(self):
        qp = {"query_id": "Q2", "query": "duckdb", "runs": 1}
        for bad in ("20261113T081600+0700", "2026-11-13", "13/11/2026 08:16"):
            out = self.ingest("x_query_plan", [qp | {"last_run_at": bad}])
            self.assertEqual((out["inserted"], out["errors"][0]["columns"]), (0, ["last_run_at"]), bad)
        out = self.ingest("x_query_plan", [qp | {"runs": "many"}])
        self.assertEqual(out["errors"][0]["columns"], ["runs"])
        for good in ("2026-11-13 08:16", "2026-11-13T08:16:00+07:00"):
            out = self.ingest("x_query_plan", [qp | {"last_run_at": good}], run_id=f"ok{good[-2:]}")
            self.assertEqual(out["errors"], [], good)
        self.assertEqual(self.ingest("x_query_plan", [qp | {"last_run_at": ""}], run_id="blank")["errors"], [])

    def test_config_rows_keep_the_newest_edit_for_the_drain(self):
        qp = {"query_id": "Q1", "query": "duckdb", "last_since_id": "100", "runs": 1}
        self.ingest("x_query_plan", [qp])
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        out = self.ingest("x_query_plan", [qp | {"last_since_id": "200", "runs": 2}])
        self.assertEqual((out["inserted"], out["upserted"]["updated"]), (1, 1))
        self.assertEqual(self.ingest("x_query_plan", [qp | {"last_since_id": "200", "runs": 2}])["skipped"], 1)
        self.assertEqual(self.q("select count(*) from x_intel.ingest_inbox where drained_at is null")[0][0], 1)
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.assertEqual(self.q("select last_since_id, runs from x_intel.x_query_plan"), [("200", 2)])
        view = self.q("select query_id, last_since_id, runs, last_run_at from x_intel.v_query_plan_sheet")
        self.assertEqual(view, [("Q1", "200", "2", "")])

    # -- review fixes (Codex, 2026-11-08) -------------------------------------------------------
    def test_book_update_in_a_batch_wins_over_a_later_bot_row_sql_and_python(self):
        self.ingest("claims", [claim_row("CL-5", "2026-11-13 08:00", checked_by="book", verification_status="a")])
        out = self.ingest("claims", [claim_row("CL-5", "2026-11-13 10:00", checked_by="book", verification_status="b"),
                                     claim_row("CL-5", "2026-11-13 12:00", checked_by="rule", verification_status="c")])
        self.assertEqual(out["upserted"]["updated"], 1)
        self.assertEqual(self.q("select verification_status from x_intel.claims"), [("b",)])
        # No stored row: a Book row in the batch still beats a later bot row (order-independent).
        self.ingest("claims", [claim_row("CL-6", "2026-11-13 12:00", checked_by="rule", verification_status="c"),
                               claim_row("CL-6", "2026-11-13 10:00", checked_by="book", verification_status="b")])
        self.assertEqual(self.q("select verification_status from x_intel.claims where claim_id = 'CL-6'"), [("b",)])
        # The Python path (drain / Sheet backfill) picks the same winner.
        self.q("update x_intel.claims set verification_status = 'a', last_checked = '2026-11-13 01:00+00' returning 1")
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.assertEqual(self.q("select claim_id, verification_status from x_intel.claims order by 1"),
                         [("CL-5", "b"), ("CL-6", "b")])

    def test_validate_flags_a_missed_book_update_behind_a_later_bot_version(self):
        from x_lakehouse.lake import write_raw
        self.ingest("claims", [claim_row("CL-7", "2026-11-13 08:00", checked_by="book")])
        rows = [type_row(DATASETS["claims"], claim_row("CL-7", t, checked_by=g))
                for t, g in (("2026-11-13 10:00", "book"), ("2026-11-13 12:00", "rule"))]
        write_raw(DATASETS["claims"], rows, [r.values["last_checked"] for r in rows], store=self.store, prefix="",
                  source="append", execute=True, run_date=NOW.date(), now=NOW)  # Book's upsert "failed"
        cfg = type("C", (), {"db_url": DB})()
        with patch.object(validate, "open_store", return_value=(self.store, "")):
            r = validate.reconcile(cfg, ["claims"], None, None, sheet=False)["datasets"]["claims"]
        self.assertEqual((r["postgres_stale_versions"], r["postgres_protected_held"], r["ok"]), (1, 1, False))

    def test_drain_never_rolls_back_a_config_edit_made_while_it_ran(self):
        qp = {"query_id": "Q1", "query": "duckdb", "last_since_id": "100"}
        self.ingest("x_query_plan", [qp])
        snapshot = inbox.undrained(self.conn)  # the drain read v1 ...
        self.ingest("x_query_plan", [qp | {"last_since_id": "200"}])  # ... then a bot wrote v2
        with patch.object(inbox, "undrained", return_value=snapshot):
            rep = inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.assertEqual((rep["marked"], rep["superseded"], rep["unmarked"]), (0, 1, 0))  # v2 stays undrained
        self.assertEqual(self.q("select last_since_id from x_intel.x_query_plan"), [("200",)])
        self.assertEqual(self.q("select count(*) from x_intel.ingest_inbox where drained_at is null")[0][0], 1)

    def test_config_coercion_error_fails_the_run_and_keeps_the_row(self):
        out = self.ingest("x_query_plan", [{"query_id": "Q1", "query": "duckdb", "runs": "oops"}])
        self.assertEqual(out["errors"][0]["columns"], ["runs"])  # refused at the door since review 6
        # A row stored before that gate still fails the drain (defence in depth).
        self.q("insert into x_intel.ingest_inbox (dataset, run_id, row, row_key) select 'x_query_plan', 'old', r, "
               "x_intel.row_key('x_query_plan', %s, r) from (select %s::jsonb r) s returning id",
               list(DATASETS["x_query_plan"].key), json.dumps({"query_id": "Q1", "query": "duckdb", "runs": "oops"}))
        code, beats, ev = self.run_scheduled()
        self.assertEqual((code, beats), (1, [False]))
        self.assertEqual(ev["drain"]["config_coerce_errors"], {"x_query_plan": 1})
        self.assertNotIn("prune", ev)
        self.assertEqual(self.q("select count(*) from x_intel.ingest_inbox where drained_at is null")[0][0], 1)
        self.ingest("x_query_plan", [{"query_id": "Q1", "query": "duckdb", "runs": 3}])  # corrected row
        self.assertEqual(self.run_scheduled()[0], 0)

    def test_raw_coercion_error_on_an_old_row_fails_the_inbox_run(self):
        self.ingest("x_feed", [feed_row("1", "2026-11-01 08:00", likes="oops")])  # dated before the window
        code, beats, ev = self.run_scheduled()
        self.assertEqual((code, beats), (1, [False]))
        self.assertEqual(ev["drain"]["coerce_errors"], {"x_feed": {"likes": 1}})
        self.assertNotIn("prune", ev)

    def test_compare_checks_undated_analyses_by_their_landing_date(self):
        from x_lakehouse.append import append_rows
        old = {"report_week": "2026-W45", "analysis": "fad", "rank": 1, "item": "A"}
        new = {"report_week": "2026-W46", "analysis": "fad", "rank": 1, "item": "B"}
        append_rows({"x_analyses": [old]}, store=self.store, prefix="", conn=self.conn,
                    now=NOW - timedelta(days=5))  # landed before the dual-write window
        append_rows({"x_analyses": [new]}, store=self.store, prefix="", conn=self.conn, now=NOW)
        sheet = {"x_analyses": [type_row(DATASETS["x_analyses"], r) for r in (old, new)]}
        window = NOW - timedelta(days=1)  # missing_in_inbox for undated rows uses the landing date only
        stored = inbox.stored_dates(self.store, "")
        rep = inbox.compare(self.conn, sheet, window, NOW, stored_on=stored)["x_analyses"]
        self.assertEqual((rep["sheet_rows"], rep["missing_in_inbox"]), (1, 1))  # only the new week is due
        self.ingest("x_analyses", [new])
        rep = inbox.compare(self.conn, sheet, window, NOW, stored_on=stored)["x_analyses"]
        self.assertEqual(rep["missing_in_inbox"], 0)

    def test_compare_ignores_inbox_rows_received_after_the_sheet_was_read(self):
        snapshot = datetime.now(timezone.utc) - timedelta(minutes=5)
        self.ingest("x_feed", [feed_row("42")])  # bot wrote the Sheet after our read, then the inbox
        window = datetime.now(timezone.utc) - timedelta(days=1)
        rep = inbox.compare(self.conn, {"x_feed": []}, window, NOW, snapshot_at=snapshot)["x_feed"]
        self.assertEqual(rep["missing_in_sheet"], 0)
        rep = inbox.compare(self.conn, {"x_feed": []}, window, NOW,
                            snapshot_at=datetime.now(timezone.utc) + timedelta(minutes=5))["x_feed"]
        self.assertEqual(rep["missing_in_sheet"], 1)

    def test_both_mode_catch_up_does_not_roll_back_a_newer_bot_config_edit(self):
        from x_lakehouse.append import append_rows
        qp = {"query_id": "Q1", "query": "duckdb", "last_since_id": "100"}
        self.ingest("x_query_plan", [qp])
        typed = {n: [] for n in DATASETS}
        checks = {n: validate.DatasetCheck(n, DATASETS[n].tab) for n in DATASETS}

        def stale_catch_up():  # Sheet read before the bot's edit, upserted after it
            self.ingest("x_query_plan", [qp | {"last_since_id": "200"}])
            append_rows({"x_query_plan": [qp]}, store=self.store, prefix="", conn=self.conn, now=NOW)
            return 0, {}

        with patch.object(scheduled, "_backfill", stale_catch_up), \
                patch.object(scheduled.backfill, "load", return_value=(typed, checks, [])):
            _, _, ev = self.run_scheduled(X_LAKE_SOURCE="both")
        self.assertEqual(ev["reapply_config"]["upserted"]["x_query_plan"]["updated"], 1)
        self.assertEqual(self.q("select last_since_id from x_intel.x_query_plan"), [("200",)])

    def test_config_ingest_waits_for_the_replay_lock_but_feed_ingest_does_not(self):
        other = cur.connect(DB)
        try:
            with other.cursor() as c:
                c.execute("select pg_advisory_xact_lock(%s)", (inbox.CONFIG_LOCK,))  # replay in progress
            with self.conn.cursor() as c:
                c.execute("set lock_timeout = '300ms'")
            self.conn.commit()
            self.assertEqual(self.ingest("x_feed", [feed_row("1")])["inserted"], 1)
            with self.assertRaises(Exception) as ctx:
                self.ingest("x_query_plan", [{"query_id": "Q1", "query": "q"}])
            self.assertIn("lock timeout", str(ctx.exception))
            self.conn.rollback()
            other.commit()  # replay done: the config write goes through
            self.assertEqual(self.ingest("x_query_plan", [{"query_id": "Q1", "query": "q"}])["inserted"], 1)
        finally:
            other.close()

    def test_a_config_edit_during_the_drain_does_not_fail_the_run(self):
        qp = {"query_id": "Q1", "query": "duckdb", "last_since_id": "100"}
        self.ingest("x_query_plan", [qp])
        real = inbox.undrained

        def read_then_bot_edits(conn):
            rows = real(conn)
            self.ingest("x_query_plan", [qp | {"last_since_id": "200"}])
            return rows

        with patch.object(inbox, "undrained", read_then_bot_edits):
            code, beats, ev = self.run_scheduled()
        self.assertEqual((code, beats, ev["drain"]["superseded"]), (0, [True], 1), ev)

    def test_the_lock_is_held_through_the_serving_refresh(self):
        seen = []

        def finish(ok, started, cfg):
            other = cur.connect(DB)
            try:
                seen.append(inbox.try_lock(other))
            finally:
                other.close()
            return 0

        with patch.object(scheduled, "_finish", finish):
            self.run_scheduled()
        self.assertEqual(seen, [False])

    def test_a_retried_config_file_does_not_roll_back_a_later_run(self):
        qp = {"query_id": "Q1", "query": "duckdb", "last_since_id": "100"}
        self.ingest("x_query_plan", [qp], run_id="runA")
        self.ingest("x_query_plan", [qp | {"last_since_id": "200"}], run_id="runB")
        retry = self.ingest("x_query_plan", [qp], run_id="runA")
        self.assertEqual((retry["inserted"], retry["skipped"]), (0, 1))
        self.assertEqual(self.q("select last_since_id from x_intel.x_query_plan"), [("200",)])
        self.assertEqual(self.q("select row ->> 'last_since_id' from x_intel.ingest_inbox"), [("200",)])
        # A changed payload within one run still applies (run-state written twice in a run).
        self.ingest("x_query_plan", [qp | {"last_since_id": "300"}], run_id="runB")
        self.assertEqual(self.q("select last_since_id from x_intel.x_query_plan"), [("300",)])

    def test_a_retried_config_file_with_the_number_written_differently_is_still_a_retry(self):
        # jsonb treats 1 and 1.0 as equal, so the delivery hash must too, or the retry of runA
        # passes the dedupe and rolls the cursor back to 100.
        qp = {"query_id": "Q1", "query": "duckdb", "priority": 1, "last_since_id": "100"}
        self.ingest("x_query_plan", [qp], run_id="runA")
        self.ingest("x_query_plan", [qp | {"last_since_id": "200"}], run_id="runB")
        retry = self.ingest("x_query_plan", [qp | {"priority": 1.0}], run_id="runA")
        self.assertEqual((retry["inserted"], retry["skipped"]), (0, 1))
        self.assertEqual(self.q("select last_since_id from x_intel.x_query_plan"), [("200",)])

    def test_a_retry_still_matches_a_delivery_recorded_with_the_legacy_hash(self):
        qp = {"query_id": "Q1", "query": "duckdb", "priority": 1.0, "last_since_id": "100"}
        self.ingest("x_query_plan", [qp], run_id="runA")
        # Before payload_md5 the delivery hash was md5(row::text): rewrite runA's row that way.
        self.q("update x_intel.ingest_deliveries set payload_md5 = md5(%s::jsonb::text) where run_id = 'runA' "
               "returning run_id", json.dumps(qp))
        self.ingest("x_query_plan", [qp | {"last_since_id": "200"}], run_id="runB")
        retry = self.ingest("x_query_plan", [qp], run_id="runA")
        self.assertEqual((retry["inserted"], retry["skipped"]), (0, 1))
        self.assertEqual(self.q("select last_since_id from x_intel.x_query_plan"), [("200",)])

    def test_a_raw_key_resent_with_other_values_is_refused_not_half_applied(self):
        self.ingest("x_signals", [signal_row("5", category="news")])
        out = self.ingest("x_signals", [signal_row("5", category="opinion")])
        self.assertEqual(out["errors"][0]["error"], "key already stored with different values; send a new version")
        self.assertEqual(self.ingest("x_signals", [signal_row("5", category="news")])["skipped"], 1)  # retry
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.assertEqual(self.q("select category from x_intel.x_signals"), [("news",)])
        later = self.ingest("x_signals", [signal_row("5", "2026-11-13 09:00", category="opinion")])
        self.assertEqual(later["upserted"]["updated"], 1)  # a new version is how a correction arrives

    # -- T4 --------------------------------------------------------------------------------------
    def test_t4_inbox_drain_writes_raw_and_curated_once_and_validates(self):
        self.ingest("x_feed", [feed_row("1"), feed_row("2")])
        self.ingest("x_signals", [signal_row("1")])
        self.ingest("x_triage_audit", [{"audit_week": "2026-W46", "logged_at": "2026-11-13 00:10", "n": 30,
                                        "label_agree": "0.8"}])
        code, beats, ev = self.run_scheduled()
        self.assertEqual((code, beats), (0, [True]), ev)
        self.assertEqual(ev["drain"]["rows"], 4)
        self.assertEqual(ev["drain"]["raw"], {"x_feed": 2, "x_signals": 1, "x_triage_audit": 1})
        self.assertTrue(ev["validate"]["ok"])
        self.assertEqual(ev["validate"]["drained_missing_from_raw"], {})
        self.assertEqual(read_dataset(self.store, "", X_FEED).num_rows, 2)
        self.assertEqual(self.q("select count(*) from x_intel.ingest_inbox where drained_at is null")[0][0], 0)
        parts = sorted(self.store.list("raw/"))
        # Second drain: nothing new. Re-mark rows undrained to prove the writes are idempotent too.
        self.q("update x_intel.ingest_inbox set drained_at = null returning id")
        rep = inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.assertEqual({n: r["written"] for n, r in rep["raw"].items()}, {"x_feed": 0, "x_signals": 0,
                                                                            "x_triage_audit": 0})
        self.assertTrue(all(c.get("updated", 0) == 0 and c.get("inserted", 0) == 0 for c in rep["curated"].values()))
        self.assertEqual(sorted(self.store.list("raw/")), parts)
        self.assertEqual(inbox.drain(self.conn, store=self.store, prefix="", now=NOW)["rows"], 0)

    def test_prune_deletes_only_rows_drained_more_than_three_days_ago(self):
        self.ingest("x_feed", [feed_row("1"), feed_row("2")])
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.q("update x_intel.ingest_inbox set drained_at = %s where row ->> 'post_id' = '1' returning id",
               NOW - timedelta(days=4))
        self.assertEqual(inbox.prune(self.conn, NOW), 1)
        self.assertEqual(self.q("select row ->> 'post_id' from x_intel.ingest_inbox"), [("2",)])

    def test_review3_a_changed_curated_row_is_refused_after_its_inbox_row_was_pruned(self):
        self.ingest("x_signals", [signal_row("5", category="news")])
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.q("update x_intel.ingest_inbox set drained_at = %s returning id", NOW - timedelta(days=4))
        self.assertEqual(inbox.prune(self.conn, NOW), 1)
        out = self.ingest("x_signals", [signal_row("5", category="opinion")])
        self.assertEqual(out["errors"][0]["error"], "key already stored with different values; send a new version")
        self.assertEqual(self.ingest("x_signals", [signal_row("5", category="news")])["skipped"], 1)  # retry
        self.assertEqual(self.q("select category from x_intel.x_signals"), [("news",)])

    def test_review4_the_same_number_written_differently_is_a_retry_after_prune(self):
        row = signal_row("6", category="news")  # opportunity_score 3
        self.ingest("x_signals", [row])
        self.assertEqual(self.ingest("x_signals", [row | {"opportunity_score": 3.0}])["skipped"], 1)  # jsonb: 3 == 3.0
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.q("update x_intel.ingest_inbox set drained_at = %s returning id", NOW - timedelta(days=4))
        inbox.prune(self.conn, NOW)
        after = self.ingest("x_signals", [row | {"opportunity_score": 3.0}])
        self.assertEqual((after["errors"], after["skipped"]), ([], 1))
        self.assertEqual(self.ingest("x_signals", [row | {"opportunity_score": 4}])["errors"][0]["error"],
                         "key already stored with different values; send a new version")

    def test_review5_a_changed_resend_during_prune_is_still_refused(self):
        import threading

        self.ingest("x_signals", [signal_row("7", category="news")])
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        self.q("update x_intel.ingest_inbox set drained_at = %s returning id", NOW - timedelta(days=4))
        pruning, go = threading.Event(), threading.Event()

        class HeldCommit:  # prune's statements run, its commit waits for the resend to start
            def __init__(self, conn):
                self.conn = conn

            def cursor(self):
                return self.conn.cursor()

            def commit(self):
                pruning.set()
                go.wait(10)
                self.conn.commit()

        prune_conn, bot = cur.connect(DB), cur.connect(DB)
        out = {}

        def resend():
            with bot.cursor() as c:
                c.execute("select x_intel.ingest('x_signals', %s::jsonb, 'run1113r')",
                          (json.dumps([signal_row("7", category="opinion")]),))
                out["r"] = c.fetchone()[0]
            bot.commit()

        try:
            t1 = threading.Thread(target=inbox.prune, args=(HeldCommit(prune_conn), NOW))
            t1.start()
            self.assertTrue(pruning.wait(10))
            t2 = threading.Thread(target=resend)
            t2.start()
            t2.join(0.5)
            go.set()
            t1.join(10)
            t2.join(10)
        finally:
            prune_conn.close()
            bot.close()
        self.assertEqual(out["r"]["errors"][0]["error"], "key already stored with different values; send a new version")
        self.assertEqual(self.q("select category from x_intel.x_signals where post_id = '7'"), [("news",)])

    def test_inbox_validate_fails_when_a_drained_row_is_missing_from_raw(self):
        self.ingest("x_feed", [feed_row("1")])
        inbox.drain(self.conn, store=self.store, prefix="", now=NOW)
        for k in self.store.list("raw/"):
            (Path(self.tmp.name) / k).unlink()
        self.assertEqual(inbox.landed(self.conn, self.store, "", NOW - timedelta(days=1)), {"x_feed": 1})

    # -- T5 --------------------------------------------------------------------------------------
    def test_t5_compare_finds_sheet_rows_missing_from_the_inbox_and_passes_when_equal(self):
        sheet_rows = [feed_row("1", "2026-11-13 08:00"), feed_row("2", "2026-11-13 08:05")]
        self.ingest("x_feed", sheet_rows[:1])
        self.ingest("claims", [claim_row("CL-1", "2026-11-13 08:00"), claim_row("CL-1", "2026-11-13 12:00")])
        sheet = {"x_feed": [type_row(X_FEED, r) for r in sheet_rows],
                 "claims": [type_row(DATASETS["claims"], claim_row("CL-1", "2026-11-13 12:00"))]}
        window = datetime.now(timezone.utc) - timedelta(days=1)  # received_at is the database clock
        rep = inbox.compare(self.conn, sheet, window, NOW)
        self.assertEqual((rep["x_feed"]["missing_in_inbox"], rep["x_feed"]["missing_in_sheet"]), (1, 0))
        self.assertEqual((rep["claims"]["missing_in_sheet"], rep["claims"]["missing_in_inbox"]), (0, 0))
        self.ingest("x_feed", sheet_rows[1:])
        rep = inbox.compare(self.conn, sheet, window, NOW)
        self.assertEqual(rep["x_feed"]["missing_in_inbox"], 0)
        # A row the bot is still writing (inside the grace period) is not a miss yet.
        late = feed_row("3", "2026-11-13 16:20")
        sheet["x_feed"].append(type_row(X_FEED, late))
        self.assertEqual(inbox.compare(self.conn, sheet, window, NOW)["x_feed"]["missing_in_inbox"], 0)
        self.ingest("x_feed", [feed_row("9")])  # in the inbox, never in the Sheet
        self.assertEqual(inbox.compare(self.conn, sheet, window, NOW)["x_feed"]["missing_in_sheet"], 1)

    def test_compare_flags_a_newer_signal_version_missing_from_the_sheet(self):
        old, new = signal_row("8", "2026-11-13 08:00"), signal_row("8", "2026-11-13 09:00")
        self.ingest("x_signals", [old, new])
        sheet = {"x_signals": [type_row(X_SIGNALS, old)]}
        window = datetime.now(timezone.utc) - timedelta(days=1)
        rep = inbox.compare(self.conn, sheet, window, NOW)["x_signals"]
        self.assertEqual(rep["missing_in_sheet"], 1)

    def test_t5_both_mode_run_fails_on_a_mismatch_and_passes_on_a_match(self):
        rows = [feed_row("1", "2026-11-13 08:00"), feed_row("2", "2026-11-13 08:05")]
        self.ingest("x_feed", rows[:1])
        typed = {n: [] for n in DATASETS}
        typed["x_feed"] = [type_row(X_FEED, r, sheet_row=i + 2) for i, r in enumerate(rows)]
        checks = {n: validate.DatasetCheck(n, DATASETS[n].tab) for n in DATASETS}

        def fake_backfill():  # the Sheet catch-up: lands the Sheet rows, like backfill --execute
            from x_lakehouse.append import append_rows
            append_rows({"x_feed": rows}, store=self.store, prefix="", conn=self.conn, now=NOW)
            return 0, {}

        with patch.object(scheduled, "_backfill", fake_backfill), \
                patch.object(scheduled.backfill, "load", return_value=(typed, checks, [])), \
                patch.object(scheduled, "datetime", wraps=datetime) as dt:
            dt.now.return_value = NOW
            code, beats, ev = self.run_scheduled(X_LAKE_SOURCE="both")
            self.assertEqual((code, beats), (1, [False]))
            self.assertEqual(ev["compare"]["mismatch"]["x_feed"]["missing_in_inbox"], 1)
            self.ingest("x_feed", rows[1:])
            code, beats, ev = self.run_scheduled(X_LAKE_SOURCE="both")
        self.assertEqual((code, beats), (0, [True]), ev)
        self.assertTrue(ev["compare"]["ok"])

    # -- T6 --------------------------------------------------------------------------------------
    def test_t6_a_second_drainer_exits_without_writing(self):
        self.ingest("x_feed", [feed_row("1")])
        other = cur.connect(DB)
        try:
            self.assertTrue(inbox.try_lock(other))
            code, beats, ev = self.run_scheduled()
            self.assertEqual((code, beats), (0, []))
            self.assertEqual(ev["drain"]["status"], "locked")
            self.assertEqual(self.q("select count(*) from x_intel.ingest_inbox where drained_at is null")[0][0], 1)
            self.assertEqual(self.store.list("raw/"), [])
        finally:
            other.close()
        self.assertEqual(self.run_scheduled()[0], 0)  # lock released with the connection

    # -- T7 --------------------------------------------------------------------------------------
    def test_t7_bot_sql_files_round_trip_through_ingest(self):
        rows = [feed_row(str(10**18 + i), text=f"ข่าว {i} 😀 $json$ ' \\ 　") for i in range(2300)]
        with tempfile.TemporaryDirectory() as d:
            paths = bot_sql.write_files("x_feed", rows, "run1113m", d)
            for p in paths:
                with self.conn.cursor() as c:
                    c.execute(p.read_text(encoding="utf-8"))
                    self.assertEqual(c.fetchone()[0]["errors"], [])
                self.conn.commit()
        stored = {r["post_id"]: r for (r,) in self.q("select row from x_intel.ingest_inbox")}
        self.assertEqual(len(stored), 2300)
        for r in rows:
            self.assertEqual(stored[r["post_id"]], r)

    # -- T11 -------------------------------------------------------------------------------------
    def test_t11_anon_and_authenticated_cannot_ingest_or_read_the_inbox(self):
        with self.conn.cursor() as c:
            for role in ("anon", "authenticated"):
                c.execute(f"do $$ begin if not exists (select 1 from pg_roles where rolname = '{role}') "
                          f"then create role {role} nologin; end if; end $$")
                c.execute(f"grant usage on schema x_intel to {role}")  # worst case: schema exposed
        self.conn.commit()
        inbox.ensure_inbox(self.conn)  # revokes run once the roles exist
        for role in ("anon", "authenticated"):
            for sql in ("select x_intel.ingest('x_feed', '[]'::jsonb, 'r1')",
                        "select count(*) from x_intel.ingest_inbox",
                        "select count(*) from x_intel.ingest_deliveries",
                        "select count(*) from x_intel.v_query_plan_sheet",
                        "select x_intel._ingest_upsert_claims('[]'::jsonb, 'b')"):
                with self.conn.cursor() as c:
                    c.execute(f"set role {role}")
                    with self.assertRaises(Exception, msg=f"{role}: {sql}") as ctx:
                        c.execute(sql)
                    self.assertIn("permission denied", str(ctx.exception))
                self.conn.rollback()
        with self.conn.cursor() as c:
            c.execute("revoke usage on schema x_intel from anon, authenticated")
        self.conn.commit()


@unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class SheetModeUnchangedTests(unittest.TestCase):
    def test_sheet_mode_never_touches_the_inbox(self):
        from x_lakehouse import scheduled as s
        with patch.dict(os.environ, {"X_LAKE_SOURCE": "sheet"}), \
                patch.object(s, "_backfill", return_value=(0, {})), \
                patch.object(s, "reconcile", return_value={"ok": True, "datasets": {}}), \
                patch.object(s, "_serve_counts", return_value={}), \
                patch.object(s, "_heartbeat"), patch.object(s.inbox, "drain") as drain, \
                patch("sys.stdout", io.StringIO()):
            self.assertEqual(s.main([]), 0)
        drain.assert_not_called()


if __name__ == "__main__":
    unittest.main()
