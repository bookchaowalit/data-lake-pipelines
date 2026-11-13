#!/usr/bin/env python3
"""Tests for the seven bot-only datasets (raw only), the private inbox, the section-8 datasets staying
raw-only and x_intel.drain_log (spec docs/specs/x-lake-private-datasets.md; acceptance P1-P9, I1-I7, R5,
S1-S5, S8).

Synthetic fixtures only: every text value says "synthetic"; handles are made up or the three public
account handles; no real post text, bookmark, mention or timeline row. Reports print counts only.
The Postgres tests run only with X_LAKE_TEST_DB_URL (a throwaway database; they drop x_intel and
x_intel_private), e.g.
    docker run -d --rm -e POSTGRES_PASSWORD=t -p 127.0.0.1:55439:5432 postgres:17-alpine
    X_LAKE_TEST_DB_URL=postgresql://postgres:t@127.0.0.1:55439/postgres python3 <this file>
"""

from __future__ import annotations

import io
import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / "infra" / "scripts"
sys.path.insert(0, str(SCRIPTS))

from x_lakehouse import bot_sql, inbox, own_account_backfill, private, scheduled, serve_sheet  # noqa: E402
from x_lakehouse import curated as cur  # noqa: E402
from x_lakehouse.coerce import TZ, mask_phones, type_row  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402
from x_lakehouse.lake import LocalStore, read_dataset  # noqa: E402
from x_lakehouse.schemas import (CURATED, DATASETS, NEW_NAMES, NEW_PUBLIC_NAMES, PRIVATE_NAMES,  # noqa: E402
                                 SERVE_DENY, SERVE_TABS, postgres_ddl)
from x_lakehouse.sheet import WRITABLE_TABS, SheetError, assert_writable  # noqa: E402

DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
FIXTURE_CSV = ROOT / "infra/tests/fixtures/x_own_accounts_history_synthetic.csv"
MARK = "SYNTH-MARKER-ข้อความ"  # must never appear in a log line or drain_log
# Section 8 of the Chief Data prompt proposed curated tables for these; the owner chose raw only (2026-11-12).
SECTION8 = ("x_trends_th", "x_trends_intl", "x_topic_counts", "x_account_snapshots", "alpha_feed")
# The only curated tables in Postgres (unchanged by this work).
CURATED_TABLES = ["claims", "taxonomy_map", "x_analyses", "x_query_plan", "x_signals", "x_triage_audit", "x_watchlist"]
PHONE_TEXT = f"synthetic {MARK} โทร 081-234-5678 or +66 2 123 4567, id 2114407239578710300 $json$ $$"


def ict(dt: datetime) -> str:
    return dt.astimezone(TZ).isoformat(timespec="seconds")  # ISO-8601 with +07:00


def ago(**kw: float) -> str:
    return ict(datetime.now(timezone.utc) - timedelta(**kw))


def today_ict(days: int = 0) -> str:
    return (datetime.now(timezone.utc).astimezone(TZ).date() - timedelta(days=days)).isoformat()


def row(ds: str, i: int = 0, **kw: object) -> dict[str, object]:
    """One synthetic row per dataset (all keys and the timestamps the bots send)."""
    base: dict[str, dict[str, object]] = {
        "x_own_account_daily": {"snapshot_date": today_ict(i), "snapshot_at": ago(days=i, minutes=5),
                                "handle": "bookchaowalit", "user_id": "1000000000000000001", "followers": 100 + i,
                                "following": 50, "posts": 10, "listed": 1, "followers_delta": 1, "posts_24h": 2,
                                "views_24h": 300, "likes_24h": 4, "replies_24h": 1, "reposts_24h": 0,
                                "mentions_24h": 1, "source": "run"},
        "x_own_mentions": {"post_id": str(1_900_000_000_000_000_000 + i), "own_handle": "ghp273", "kind": "mention",
                           "collected_at": ago(hours=1), "posted_at": ago(hours=2), "author_handle": "synthetic_a",
                           "author_id": "42", "text": f"synthetic mention {i}", "likes": 1, "views": 9, "url":
                           f"https://x.com/synthetic_a/status/{1_900_000_000_000_000_000 + i}", "filter_flags": ""},
        "x_bookmarks": {"post_id": str(1_800_000_000_000_000_000 + i), "folder_id": "", "folder_name": "",
                        "first_seen_at": ago(days=2), "last_seen_at": ago(hours=1), "author_handle": "synthetic_b",
                        "text": f"synthetic bookmark {i}", "lang": "th", "domain": "D5 Technology", "topic": "AI",
                        "tags": "synthetic", "calendar_candidate": False, "likes": 3, "url": "https://x.com/s/1"},
        "x_home_timeline": {"post_id": str(1_700_000_000_000_000_000 + i), "collected_at": ago(minutes=30),
                            "posted_at": ago(hours=3), "author_handle": "synthetic_c", "author_followers": 1000,
                            "text": f"synthetic timeline {i}", "lang": "en", "likes": 10, "reposts": 2, "replies": 1,
                            "quotes": 0, "views": 500, "age_min": 150.0, "velocity": 6.0, "eng_rate": 0.026,
                            "rank": i + 1, "domain": "D5 Technology", "topic": "AI"},
        "x_spaces": {"space_id": f"1SYNTH{i}", "collected_at": ago(hours=1), "state": "scheduled",
                     "title": f"synthetic space {i}", "scheduled_start": ago(hours=-20), "lang": "th",
                     "creator_handle": "synthetic_d", "participant_count": 0, "query": "AI",
                     "calendar_candidate": True, "url": f"https://x.com/i/spaces/1SYNTH{i}"},
        "x_communities": {"community_id": str(1_600_000_000_000_000_000 + i), "snapshot_date": today_ict(),
                          "name": f"synthetic community {i}", "description": "synthetic", "member_count": 120,
                          "created_at": "2024-01-02T03:04:05+07:00", "access": "Public", "query": "AI",
                          "member_delta": 3, "filter_flags": "", "url": "https://x.com/i/communities/1"},
        "x_list_sources": {"list_id": str(1_500_000_000_000_000_000 + i), "name": f"synthetic list {i}",
                           "owner_handle": "synthetic_e", "member_count": 40, "follower_count": 5, "mapped_qid": "A93",
                           "members_hash": "0" * 16, "verified_at": ago(hours=2), "status": "ok"},
        # Section-8 datasets (Sheet header names).
        "x_trends_th": {"snapshot_date": today_ict(), "snapshot_at": ago(hours=1), "rank": i + 1,
                        "trend_name": f"#synthetic{i}", "trend_type": "hashtag", "post_count": 1000},
        "x_trends_intl": {"snapshot_date": today_ict(), "snapshot_at": ago(hours=1), "woeid": 1, "country": "World",
                          "rank": i + 1, "trend_name": f"#synthetic{i}"},
        "x_topic_counts": {"snapshot_date": today_ict(), "snapshot_at": ago(hours=1), "query_id": f"Q{i}",
                           "topic": "synthetic", "total_24h": 10, "spike": i % 2 == 0},
        "x_account_snapshots": {"snapshot_date": today_ict(), "snapshot_at": ago(hours=1), "list": "watch",
                                "handle": f"synthetic_{i}", "user_id": str(10 + i), "followers_count": 5},
        "alpha_feed": {"pulled_at_ict": ago(hours=1), "source": "synthetic", "sub_source": "s", "date": today_ict(),
                       "title": f"synthetic alpha {i}", "url": f"https://example.com/{i}", "time_bound_date": today_ict(-3)},
    }
    out = dict(base[ds])
    out.update(kw)
    return out


class OfflineTests(unittest.TestCase):
    def test_load_of_bot_only_datasets_never_touches_the_sheet(self):
        # validate --datasets x_bookmarks --raw-only must work without a Google credential.
        from x_lakehouse import backfill

        def no_sheet(*_a, **_k):
            raise AssertionError("SheetReader built for bot-only datasets")

        with patch.object(backfill, "SheetReader", no_sheet):
            typed, checks, missing = backfill.load(Config({}), ["x_bookmarks", "x_spaces"])
        self.assertEqual((typed, missing, sorted(checks)), ({}, [], ["x_bookmarks", "x_spaces"]))

    # -- P1 / P2 / S6 -----------------------------------------------------------------------------
    def test_p1_private_names_are_in_no_serving_or_summary_config(self):
        from sync import sheet_summaries as ss  # infra/scripts/sync
        lists = {"SERVE_TABS keys": set(SERVE_TABS), "WRITABLE_TABS": set(WRITABLE_TABS), "DATA_TABS": set(ss.DATA_TABS),
                 "FINDING_TABS": set(ss.FINDING_TABS)}
        for name in PRIVATE_NAMES:
            for label, items in lists.items():
                self.assertFalse(any(name in str(i).lower().replace(" ", "_") for i in items), (name, label))
        self.assertTrue(set(NEW_NAMES) <= SERVE_DENY)
        for name in NEW_PUBLIC_NAMES:
            self.assertNotIn(name, SERVE_TABS)

    def test_p2_serving_a_private_or_new_dataset_is_refused_before_any_read(self):
        for name in NEW_NAMES:
            with self.assertRaises(serve_sheet.ServeError, msg=name):
                serve_sheet.source_for(name)

        class Spy:  # records every statement fetch() would run
            def __init__(self):
                self.sql = []

            def cursor(self):
                return self

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, *a):
                self.sql.append(sql)

            def fetchall(self):
                return []

        spy = Spy()
        views = serve_sheet.fetch(spy)
        joined = " ".join(spy.sql)
        self.assertNotIn("x_intel_private", joined)
        for name in NEW_NAMES:
            self.assertNotIn(name, joined)
        self.assertEqual(set(views), set(SERVE_TABS.values()))

    def test_s6_only_the_three_serve_tabs_are_writable(self):
        self.assertEqual(set(WRITABLE_TABS), set(SERVE_TABS.values()))
        for tab in WRITABLE_TABS:
            assert_writable(tab)
        for tab in ("X Feed", "Alpha Feed", "X Trends TH", "AI Summaries", "Chief Data", "Serve · X Spaces",
                    "Serve · Alpha Feed 7d", "Serve · X Trends TH 3d"):
            with self.assertRaises(SheetError):
                assert_writable(tab)
        for name in SECTION8:  # no serving tab over a raw-only dataset
            with self.assertRaises(serve_sheet.ServeError):
                serve_sheet.source_for(name)

    # -- I4 / I6 / P9 -----------------------------------------------------------------------------
    def test_i4_phone_numbers_are_masked_and_ids_dates_kept(self):
        self.assertEqual(mask_phones("โทร 081-234-5678 / 02 123 4567 / +1 555 123 4567"),
                         "โทร [phone] / [phone] / [phone]")
        for phone in ("053-123-456", "053 123 456", "(053) 123-456", "(02) 123 4567", "+1 (555) 123-4567",
                      "+66 (2) 123 4567", "+44 20 7946 0958", "+44 (20) (7946) 0958"):
            self.assertEqual(mask_phones(f"call {phone} now"), "call [phone] now", phone)
        for keep in ("2114407239578710300", "2026-11-10 08:00", "1,234", "0812345678901", "+5 likes",
                     "0531234567", "๐๘๑๒๓๔๕๖๗๘", "+1 2 3", "+12 34 56", "score +10 20 30", "+500 likes 1,234"):
            self.assertEqual(mask_phones(keep), keep)
        typed = type_row(DATASETS["x_bookmarks"], row("x_bookmarks", text=PHONE_TEXT))
        self.assertNotIn("081-234-5678", typed.values["text"])
        self.assertIn("[phone]", typed.values["text"])
        self.assertIn("2114407239578710300", typed.values["text"])

    def test_i6_bot_sql_headers_and_private_door(self):
        for n in NEW_NAMES:
            self.assertEqual(list(bot_sql.HEADERS[n]), DATASETS[n].names, n)
        self.assertEqual(bot_sql.PRIVATE, set(PRIVATE_NAMES))
        for n in PRIVATE_NAMES:
            self.assertTrue(bot_sql.statement(n, [row(n)], "r1").startswith(f"select x_intel_private.ingest('{n}', "))
        for n in NEW_PUBLIC_NAMES:
            self.assertTrue(bot_sql.statement(n, [row(n)], "r1").startswith(f"select x_intel.ingest('{n}', "))

    def test_p9_fixture_holds_synthetic_rows_only(self):
        text = FIXTURE_CSV.read_text(encoding="utf-8")
        lines = text.strip().splitlines()
        self.assertEqual(lines[0], ",".join(own_account_backfill.CSV_COLUMNS))
        self.assertEqual(len(lines), 19)
        for line in lines[1:]:
            self.assertTrue(line.split(",")[2].startswith("9"), "user ids in the fixture are made up (9...)")

    # -- R5 (offline half) -----------------------------------------------------------------------
    def test_r5_backfill_dry_run_maps_the_csv_and_keeps_the_gap(self):
        out = io.StringIO()
        with patch("sys.stdout", out):
            self.assertEqual(own_account_backfill.main([str(FIXTURE_CSV)]), 0)
        rep = json.loads(out.getvalue())
        self.assertEqual((rep["mode"], rep["rows"], rep["handles"], rep["dates"]), ("dry-run", 18, 3, 6))
        self.assertEqual(rep["missing_dates"], [f"2026-11-0{d}" for d in range(2, 10)])
        self.assertEqual(set(rep["null_columns"]), {"snapshot_at", "listed", "followers_delta", "posts_24h",
                                                    "reposts_24h", "mentions_24h"})
        rows = own_account_backfill.read_rows(FIXTURE_CSV)
        self.assertTrue(all(r["source"] == "backfill_csv" for r in rows))
        with tempfile.TemporaryDirectory() as d:
            bad = Path(d) / "bad.csv"
            bad.write_text("date,handle,user_id,themes\n2026-11-01,a,9,x\n", encoding="utf-8")
            with self.assertRaises(own_account_backfill.BackfillError):
                own_account_backfill.read_rows(bad)

    # -- S1 (DDL) / S3 / S8 -----------------------------------------------------------------------
    def test_s1_s3_no_curated_table_for_the_bot_only_or_section8_datasets(self):
        self.assertEqual(sorted(CURATED), CURATED_TABLES)
        ddl, pddl = postgres_ddl(), private.private_ddl()
        self.assertIn("create table if not exists x_intel.drain_log", ddl)
        for name in NEW_NAMES + SECTION8 + ("x_feed", "x_post_context"):
            self.assertNotIn(f"x_intel.{name} ", ddl, name)
            self.assertNotIn(f"x_intel.{name} (", ddl, name)
            self.assertNotIn(f"x_intel_private.{name}", pddl, name)
        self.assertNotIn("x_intel_private", ddl)  # the private inbox has its own DDL
        tables = re.findall(r"create table if not exists (\S+)", pddl)
        self.assertEqual(tables, ["x_intel_private.ingest_inbox", "x_intel_private.ingest_seen"])
        self.assertNotIn("retention_watermark", pddl)

    def test_s8_docs_explain_the_choice(self):
        doc = (ROOT / "docs/specs/x-lake-private-datasets.md").read_text(encoding="utf-8")
        for phrase in ("raw only", "2026-11-12", "x_feed", "x_post_context", "postgres", "drain_log",
                       "ai_status", "x_intel_private.ingest", "lifecycle"):
            self.assertIn(phrase, doc)

    # -- rollback switches ------------------------------------------------------------------------
    def test_rollback_switch_removes_the_datasets(self):
        code = ("import sys; sys.path.insert(0, %r); from x_lakehouse import schemas as s, inbox, private; "
                "import json; print(json.dumps([sorted(s.DATASETS), sorted(s.CURATED), "
                "'x_bookmarks' in inbox.inbox_ddl(), 'x_spaces' in inbox.inbox_ddl(), "
                "\"when 'x_bookmarks'\" in private.private_ddl()]))") % str(SCRIPTS)
        env = {**os.environ, "X_LAKE_NEW_DATASETS": "off"}
        out = json.loads(subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True,
                                        check=True).stdout)
        names, tables, priv_in_public, spaces_in_public, priv_door = out
        self.assertEqual(len(names), 14)
        self.assertFalse(set(NEW_NAMES) & set(names))
        self.assertEqual(tables, CURATED_TABLES)
        self.assertTrue(priv_in_public)  # only in the explicit "private dataset" refusal
        self.assertFalse(spaces_in_public)
        self.assertFalse(priv_door)


@unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class PrivatePostgresTests(unittest.TestCase):
    def setUp(self):
        self.conn = cur.connect(DB)
        with self.conn.cursor() as c:
            c.execute("drop schema if exists x_intel_private cascade")
            c.execute("drop schema if exists x_intel cascade")
        self.conn.commit()
        inbox.ensure_inbox(self.conn)
        self.assertTrue(private.ensure_private(self.conn))
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(Path(self.tmp.name))

    def tearDown(self):
        self.conn.rollback()
        with self.conn.cursor() as c:  # leave no private rows behind for the other suites on this database
            c.execute("drop schema if exists x_intel_private cascade")
        self.conn.commit()
        self.conn.close()
        self.tmp.cleanup()

    def test_ingest_waits_out_a_private_prune(self):
        # prune() holds the exclusive lock while it moves rows to ingest_seen; ingest() must wait for it,
        # or a changed value for a key being pruned would be accepted (seen in neither table).
        other = cur.connect(DB)
        try:
            with other.cursor() as c:
                c.execute("select pg_advisory_xact_lock(%s)", (private.PRIVATE_PRUNE_LOCK,))
            with self.conn.cursor() as c:
                c.execute("set lock_timeout = '300ms'")
                with self.assertRaises(Exception) as ctx:
                    c.execute("select x_intel_private.ingest('x_bookmarks', '[]'::jsonb, 'r1')")
            self.assertIn("lock", str(ctx.exception).lower())
            self.conn.rollback()
        finally:
            other.rollback()
            other.close()
        with self.conn.cursor() as c:
            c.execute("set lock_timeout = 0")
        self.conn.commit()
        self.assertEqual(self.ingest("x_bookmarks", [], fn="x_intel_private.ingest")["errors"], [])

    # helpers
    def ingest(self, dataset, rows, run_id="run1110t", fn=None):
        fn = fn or bot_sql.ingest_function(dataset)
        with self.conn.cursor() as c:
            c.execute(f"select {fn}(%s, %s::jsonb, %s)", (dataset, json.dumps(rows, ensure_ascii=False), run_id))
            out = c.fetchone()[0]
        self.conn.commit()
        return out

    def q(self, sql, *args):
        with self.conn.cursor() as c:
            c.execute(sql, args)
            rows = c.fetchall()
        self.conn.commit()
        return rows

    def n(self, table):
        return self.q(f"select count(*) from {table}")[0][0]

    def raw(self, name):
        tbl = read_dataset(self.store, "", DATASETS[name])
        return [] if tbl is None else tbl.to_pylist()

    def relations(self, names):
        return self.q("select nspname || '.' || relname from pg_class c join pg_namespace n on n.oid = c.relnamespace "
                      "where nspname in ('x_intel', 'x_intel_private', 'public') and relname = any(%s)", list(names))

    def run_scheduled(self, **extra):
        out, beats = io.StringIO(), []
        env = {"X_LAKE_DB_URL": DB, "X_LAKE_URI": f"file://{self.tmp.name}", "X_LAKE_SOURCE": "inbox", **extra}
        with patch.dict(os.environ, env), patch.object(scheduled, "_heartbeat", beats.append), \
                patch.object(scheduled, "_serve_counts", return_value={}), patch("sys.stdout", out):
            code = scheduled.main([])
        text = out.getvalue()
        events = {e["event"]: e for e in (json.loads(x) for x in text.splitlines() if x.startswith("{"))}
        return code, beats, events, text

    # -- I1 / I2 / S2 -----------------------------------------------------------------------------
    def test_i1_s2_every_new_and_section8_dataset_is_retry_safe_and_raw_only(self):
        names = list(NEW_NAMES) + list(SECTION8)
        batches = {n: [row(n, i) for i in range(3)] for n in names}
        for n, rows in batches.items():
            first, second = self.ingest(n, rows), self.ingest(n, rows)
            self.assertEqual((first["inserted"], first["errors"]), (3, []), n)
            self.assertEqual((second["inserted"], second["skipped"]), (0, 3), n)
        code, beats, ev, _ = self.run_scheduled()
        self.assertEqual((code, beats), (0, [True]), ev.get("validate"))
        counts = {n: len(self.raw(n)) for n in names}
        self.assertEqual(counts, {n: 3 for n in names})
        self.assertEqual(self.relations(names), [])  # raw only: no table in Postgres
        for n, rows in batches.items():  # again after the drain: still no new row anywhere
            self.assertEqual(self.ingest(n, rows)["inserted"], 0, n)
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual({n: len(self.raw(n)) for n in names}, counts)

    def test_i2_composite_keys(self):
        self.ingest("x_bookmarks", [row("x_bookmarks", 1), row("x_bookmarks", 1, folder_id="f1", folder_name="F")])
        t0 = datetime.now(timezone.utc)
        self.ingest("x_home_timeline", [row("x_home_timeline", 1, collected_at=ict(t0 - timedelta(minutes=m)))
                                        for m in (10, 40)])
        self.ingest("x_spaces", [row("x_spaces", 1, collected_at=ict(t0 - timedelta(minutes=m))) for m in (10, 40)])
        self.ingest("x_communities", [row("x_communities", 1, snapshot_date=today_ict(d)) for d in (0, 1)])
        self.ingest("x_own_account_daily", [row("x_own_account_daily", 0, handle=h) for h in ("ghp273", "eatbeforeboard")])
        self.ingest("x_own_mentions", [row("x_own_mentions", 1, own_handle=h) for h in ("ghp273", "bookchaowalit")])
        self.assertEqual(self.run_scheduled()[0], 0)
        folders = sorted(r["folder_id"] for r in self.raw("x_bookmarks"))
        self.assertEqual(folders, ["", "f1"])  # '' is a value, never NULL
        for name in ("x_home_timeline", "x_spaces", "x_communities", "x_own_account_daily", "x_own_mentions"):
            self.assertEqual(len(self.raw(name)), 2, name)

    # -- I3 ----------------------------------------------------------------------------------------
    def test_i3_unknown_column_is_reported_and_other_rows_keep_every_column(self):
        good = row("x_bookmarks", 2)
        out = self.ingest("x_bookmarks", [row("x_bookmarks", 1, themes="AI"), good])
        self.assertEqual(out["inserted"], 1)
        self.assertEqual(out["errors"], [{"row": 1, "error": "unknown columns", "columns": ["themes"]}])
        [(stored,)] = self.q("select row from x_intel_private.ingest_inbox")
        self.assertEqual(stored, good)
        reply = self.ingest("x_home_timeline", [row("x_home_timeline", 1, reply_text="synthetic reply")])
        self.assertEqual(reply["errors"][0]["columns"], ["reply_text"])  # no generated reply is stored

    # -- I4 / I6 -----------------------------------------------------------------------------------
    def test_i4_i6_bot_sql_round_trip_masks_phones_in_inbox_and_raw(self):
        sent = {n: [row(n, 1, **{c: PHONE_TEXT for c in DATASETS[n].mask_phone})] for n in NEW_NAMES}
        with tempfile.TemporaryDirectory() as d:
            for n, rows in sent.items():
                for p in bot_sql.write_files(n, rows, "run1110t", d):
                    with self.conn.cursor() as c:
                        c.execute(p.read_text(encoding="utf-8"))
                        self.assertEqual(c.fetchone()[0]["errors"], [], n)
                    self.conn.commit()
        self.assertEqual(self.run_scheduled()[0], 0)
        for n, rows in sent.items():
            ds = DATASETS[n]
            inbox_table = private.P_INBOX if ds.private else inbox.INBOX
            [(stored,)] = self.q(f"select row from {inbox_table} where dataset = %s", n)
            [raw] = self.raw(n)
            self.assertEqual(set(stored), set(rows[0]), n)  # every sent column round-trips
            for col in ds.mask_phone:
                for where, value in (("inbox", stored[col]), ("raw", raw[col])):
                    self.assertNotIn("081-234-5678", value, (n, where))
                    self.assertIn("[phone]", value, (n, where))
                    self.assertIn("$json$", value, (n, where))
                    self.assertIn("ข้อความ", value, (n, where))

    def test_i4_sql_and_python_mask_the_same_way(self):
        samples = [PHONE_TEXT, "081 234 5678", "0812345678", "02-123-4567", "+66812345678", "+1 (555) 123 4567",
                   "x0812345678", "08123456789", "tel:+44 20 7946 0958", "๐๘๑๒๓๔๕๖๗๘", "2026-11-10T08:00:00+07:00",
                   "1900000000000000001", "", "no digits", "053-123-456", "(02) 123 4567", "+1 (555) 123-4567",
                   "0531234567", "+1 2 3", "+12 34 56", "score +10 20 30"]
        for t in samples:
            [(sql,)] = self.q("select x_intel._mask_phones(%s)", t)
            self.assertEqual(sql, mask_phones(t), t)

    # -- I7 ----------------------------------------------------------------------------------------
    def test_i7_list_sources_keep_every_verification_in_raw(self):
        v1 = row("x_list_sources", 1, verified_at=ago(hours=5), status="ok")
        v2 = row("x_list_sources", 1, verified_at=ago(hours=1), status="stale")
        self.ingest("x_list_sources", [v2])
        self.ingest("x_list_sources", [v1])  # an older verification arriving late
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(sorted(r["status"] for r in self.raw("x_list_sources")), ["ok", "stale"])

    # -- P3 / P4 / P5 / P6 ------------------------------------------------------------------------
    def test_p4_public_door_refuses_private_datasets_and_stores_nothing(self):
        before = self.n("x_intel.ingest_inbox")
        for n in PRIVATE_NAMES:
            out = self.ingest(n, [row(n)], fn="x_intel.ingest")
            self.assertEqual(out["inserted"], 0)
            self.assertIn("x_intel_private.ingest", out["errors"][0]["error"])
        self.assertEqual(self.n("x_intel.ingest_inbox"), before)
        for n in NEW_PUBLIC_NAMES:  # and the private door takes only private datasets
            self.assertEqual(self.ingest(n, [row(n)], fn="x_intel_private.ingest")["errors"][0]["error"],
                             "unknown private dataset")
        self.assertEqual(self.n(private.P_INBOX), 0)

    def test_p5_private_raw_lives_only_under_private_and_public_listings_see_none(self):
        for n in PRIVATE_NAMES:
            self.ingest(n, [row(n, 1, **{c: PHONE_TEXT for c in DATASETS[n].mask_phone})])
        self.ingest("x_spaces", [row("x_spaces", 1)])
        self.assertEqual(self.run_scheduled()[0], 0)
        public = self.store.list("raw/") + self.store.list("_manifests/")
        self.assertTrue(any("/x_spaces/" in k for k in public))
        for n in PRIVATE_NAMES:
            self.assertFalse(any(f"/{n}/" in k for k in public), n)
            self.assertTrue(any(k.endswith(".parquet") for k in self.store.list(f"private/raw/{n}/")), n)
        for k in public:  # no public file carries a private row's text
            self.assertNotIn(MARK.encode(), self.store.read(k))
        self.assertEqual(self.q("select count(*) from x_intel.ingest_inbox where dataset = any(%s)",
                                list(PRIVATE_NAMES)), [(0,)])

    def test_p3_no_view_in_x_intel_or_public_reads_private_data(self):
        from x_lakehouse.marts import ensure_marts
        ensure_marts(self.conn)
        rows = self.q("select view_schema, view_name from information_schema.view_table_usage "
                      "where table_schema = 'x_intel_private' or table_name = any(%s)", list(PRIVATE_NAMES))
        self.assertEqual(rows, [])
        defs = self.q("select schemaname, viewname, definition from pg_views where schemaname in ('x_intel', 'public')")
        self.assertTrue(defs)
        for schema, view, definition in defs:
            self.assertNotIn("x_intel_private", definition, view)
            for n in PRIVATE_NAMES:
                self.assertNotIn(n, definition, view)

    def test_p6_anon_and_authenticated_cannot_touch_the_private_schema(self):
        with self.conn.cursor() as c:
            for role in ("anon", "authenticated"):
                c.execute(f"do $$ begin if not exists (select 1 from pg_roles where rolname = '{role}') "
                          f"then create role {role} nologin; end if; end $$")
                # Worst case: someone exposed the schema and its tables.
                c.execute(f"grant usage on schema x_intel_private to {role}")
                c.execute(f"grant select, insert on all tables in schema x_intel_private to {role}")
                c.execute(f"grant execute on function x_intel_private.ingest(text, jsonb, text) to {role}")
        self.conn.commit()
        self.assertTrue(private.ensure_private(self.conn))  # the revokes run on every cron run
        statements = ["select x_intel_private.ingest('x_bookmarks', '[]'::jsonb, 'r1')",
                      "insert into x_intel_private.ingest_inbox (dataset, run_id, row, row_key) values ('a','b','{}','c')"]
        statements += [f"select count(*) from {private.P_INBOX}", f"select count(*) from {private.P_SEEN}"]
        for role in ("anon", "authenticated"):
            for sql in statements:
                with self.conn.cursor() as c:
                    c.execute(f"set role {role}")
                    with self.assertRaises(Exception, msg=f"{role}: {sql}") as ctx:
                        c.execute(sql)
                    self.assertIn("permission denied", str(ctx.exception))
                self.conn.rollback()
            privs = self.q("select has_schema_privilege(%s, 'x_intel_private', 'USAGE'), "
                           "has_function_privilege(%s, 'x_intel_private.ingest(text, jsonb, text)', 'EXECUTE')",
                           role, role)
            self.assertEqual(privs, [(False, False)], role)
        rls = self.q("select relname, relrowsecurity from pg_class c join pg_namespace n on n.oid = c.relnamespace "
                     "where nspname = 'x_intel_private' and relkind = 'r' order by 1")
        self.assertEqual([r for r, _ in rls], ["ingest_inbox", "ingest_seen"])  # the inbox only, no curated table
        self.assertTrue(all(on for _, on in rls))
        with self.conn.cursor() as c:
            c.execute("drop owned by anon, authenticated")
        self.conn.commit()

    # -- P7 / P8 -----------------------------------------------------------------------------------
    def test_p7_both_mode_compare_skips_the_bot_only_datasets(self):
        for n in NEW_PUBLIC_NAMES:
            self.ingest(n, [row(n, 1)])
        now = datetime.now(timezone.utc)
        cmp = inbox.compare(self.conn, {}, now - timedelta(days=1), now + timedelta(minutes=5))
        self.assertFalse(set(NEW_NAMES) & set(cmp))
        self.assertFalse(any(r["missing_in_sheet"] or r.get("missing_in_inbox") for r in cmp.values()))

    def test_p8_logs_and_drain_log_carry_counts_only(self):
        for n in NEW_NAMES:
            self.ingest(n, [row(n, 1, **{c: PHONE_TEXT for c in DATASETS[n].mask_phone})])
        code, _, ev, text = self.run_scheduled()
        self.assertEqual(code, 0)
        self.assertEqual(ev["drain_private"]["datasets"], {n: 1 for n in PRIVATE_NAMES})
        log = json.dumps(self.q("select to_jsonb(d) from x_intel.drain_log d"), ensure_ascii=False, default=str)
        for blob in (text, log):
            for needle in (MARK, "synthetic", "081-234", "[phone]", "x.com/", DB, "postgresql://"):
                self.assertNotIn(needle, blob)

    # -- R5 ----------------------------------------------------------------------------------------
    def test_r5_backfill_execute_is_idempotent_and_keeps_the_gap(self):
        out = io.StringIO()
        with patch.dict(os.environ, {"X_LAKE_DB_URL": DB}), patch("sys.stdout", out):
            self.assertEqual(own_account_backfill.main([str(FIXTURE_CSV), "--execute"]), 0)
            self.assertEqual(own_account_backfill.main([str(FIXTURE_CSV), "--execute"]), 0)
        first, second = (json.loads(x) for x in out.getvalue().splitlines())
        self.assertEqual((first["inserted"], second["inserted"], second["skipped"]), (18, 0, 18))
        self.assertEqual(self.run_scheduled()[0], 0)
        rows = self.raw("x_own_account_daily")
        self.assertEqual(len(rows), 18)
        days = {r["snapshot_date"].isoformat() for r in rows}
        self.assertFalse(days & {f"2026-11-0{d}" for d in range(2, 10)})
        nulls = ("snapshot_at", "listed", "followers_delta", "posts_24h", "reposts_24h", "mentions_24h")
        self.assertTrue(all(r["source"] == "backfill_csv" and all(r[k] is None for k in nulls) for r in rows))

    # -- S3 / S5 -----------------------------------------------------------------------------------
    def test_s3_feed_and_context_stay_raw_only(self):
        self.ingest("x_feed", [{"collected_at": ago(hours=1), "requesting_chief": "Chief Data", "watch_type": "k",
                                "watch_value": "v", "post_id": "1", "query_id": "Q1", "text": "synthetic"}])
        self.ingest("x_post_context", [{"post_id": "1", "media_count": 0}])
        self.assertEqual(self.run_scheduled()[0], 0)
        rels = self.q("select relname from pg_class c join pg_namespace n on n.oid = c.relnamespace "
                      "where nspname in ('x_intel', 'public') and relname in ('x_feed', 'x_post_context')")
        self.assertEqual(rels, [])
        views = self.q("select view_name from information_schema.view_table_usage where table_name in ('x_feed', 'x_post_context')")
        self.assertEqual(views, [])
        self.assertTrue(self.store.list("raw/x_feed/"))
        self.assertTrue(self.store.list("raw/x_post_context/"))

    def test_s5_one_drain_log_row_per_run_and_missing_raw_is_visible(self):
        self.ingest("x_trends_th", [row("x_trends_th", 1)])
        self.ingest("x_bookmarks", [row("x_bookmarks", 1)])
        self.assertEqual(self.run_scheduled()[0], 0)
        [(entry,)] = self.q("select to_jsonb(d) from x_intel.drain_log d")
        self.assertEqual(set(entry), {"batch_id", "started_at", "source", "rows_by_dataset", "raw_written", "curated",
                                      "coerce_errors", "missing_from_raw", "ok"})
        self.assertEqual(entry["rows_by_dataset"], {"x_trends_th": 1, "x_bookmarks": 1})
        self.assertEqual(entry["raw_written"], {"x_trends_th": 1, "x_bookmarks": 1})
        self.assertEqual(entry["missing_from_raw"], {"x_trends_th": 0, "x_bookmarks": 0})
        self.assertEqual((entry["source"], entry["ok"]), ("inbox", True))
        for key in self.store.list("raw/x_trends_th/"):  # simulate a part that never reached R2
            (Path(self.tmp.name) / key).unlink()
        code, beats, ev, _ = self.run_scheduled()
        self.assertEqual((code, beats), (1, [False]))
        last = self.q("select missing_from_raw, ok from x_intel.drain_log order by started_at desc limit 1")[0]
        self.assertEqual(last, ({"x_trends_th": 1, "x_bookmarks": 0}, False))
        self.assertEqual(self.n("x_intel.drain_log"), 2)
        out = self.ingest("drain_log", [{"batch_id": "x"}], fn="x_intel.ingest")
        self.assertEqual(out["errors"][0]["error"], "unknown dataset")

    def test_rollback_switch_closes_both_doors_and_keeps_stored_rows(self):
        self.ingest("x_bookmarks", [row("x_bookmarks", 1)])
        self.ingest("x_spaces", [row("x_spaces", 1)])
        self.assertEqual(self.run_scheduled()[0], 0)
        code = ("import sys; sys.path.insert(0, %r); from x_lakehouse import curated as cur, inbox, private; "
                "conn = cur.connect(%r); inbox.ensure_inbox(conn); print(private.ensure_private(conn)); conn.close()"
                ) % (str(SCRIPTS), DB)
        env = {**os.environ, "X_LAKE_NEW_DATASETS": "off"}
        out = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True)
        self.assertEqual(out.stdout.strip(), "False")
        try:
            for n, fn in (("x_bookmarks", "x_intel_private.ingest"), ("x_spaces", "x_intel.ingest")):
                res = self.ingest(n, [row(n, 2)], fn=fn)
                self.assertEqual(res["inserted"], 0, n)
                self.assertIn("unknown", res["errors"][0]["error"], n)
            self.assertEqual((len(self.raw("x_bookmarks")), len(self.raw("x_spaces"))), (1, 1))  # stored rows stay
        finally:
            inbox.ensure_inbox(self.conn)  # switch back on: both doors accept again
            private.ensure_private(self.conn)
        self.assertEqual(self.ingest("x_bookmarks", [row("x_bookmarks", 2)])["inserted"], 1)

    def test_standalone_validate_checks_private_raw_without_the_schema(self):
        from x_lakehouse import validate
        with self.conn.cursor() as c:
            c.execute("drop schema x_intel_private cascade")
        self.conn.commit()
        out = io.StringIO()
        env = {"X_LAKE_DB_URL": DB, "X_LAKE_URI": f"file://{self.tmp.name}"}
        with patch.dict(os.environ, env), patch.object(validate, "load", return_value=({}, {
                n: validate.DatasetCheck(n, d.tab) for n, d in DATASETS.items()}, [])), patch("sys.stdout", out):
            code = validate.main([])
        self.assertEqual(code, 0, out.getvalue())
        rep = validate.reconcile(Config(env=env), list(PRIVATE_NAMES), None, None, sheet=False)
        self.assertTrue(rep["ok"])
        self.assertFalse(any("postgres_rows" in r for r in rep["datasets"].values()))  # raw only, no Postgres check

    # -- schema missing ----------------------------------------------------------------------------
    def test_private_steps_are_skipped_while_the_owner_has_not_created_the_schema(self):
        with self.conn.cursor() as c:
            c.execute("drop schema x_intel_private cascade")
            c.execute("do $$ begin if not exists (select 1 from pg_roles where rolname = 'xl_nocreate') then "
                      "create role xl_nocreate login password 'nocreate'; end if; end $$")
            c.execute("grant all on schema x_intel to xl_nocreate")
        self.conn.commit()
        parts = urlsplit(DB)
        url = urlunsplit(parts._replace(netloc=f"xl_nocreate:nocreate@{parts.hostname}:{parts.port}"))
        other = cur.connect(url)
        try:
            self.assertFalse(private.ensure_private(other))
        finally:
            other.close()
        with self.conn.cursor() as c:
            c.execute("revoke all on schema x_intel from xl_nocreate")
        self.conn.commit()
        self.assertEqual(self.q("select count(*) from pg_namespace where nspname = 'x_intel_private'"), [(0,)])


if __name__ == "__main__":
    unittest.main()
