#!/usr/bin/env python3
"""Tests for the seven bot-only datasets, the private schema, the section-8 small curated tables and
x_intel.drain_log (spec docs/specs/x-lake-private-datasets.md; acceptance P1-P9, I1-I7, R1-R5, S1-S8).

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
from x_lakehouse.schemas import (CURATED, DATASETS, NEW_NAMES, NEW_PUBLIC_NAMES, OPTIONAL_SERVE_TABS,  # noqa: E402
                                 PRIVATE_CURATED, PRIVATE_NAMES, SERVE_DENY, SERVE_TABS, SMALL_CURATED,
                                 postgres_ddl)
from x_lakehouse.sheet import WRITABLE_TABS, SheetError, assert_writable  # noqa: E402

DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
FIXTURE_CSV = ROOT / "infra/tests/fixtures/x_own_accounts_history_synthetic.csv"
MARK = "SYNTH-MARKER-ข้อความ"  # must never appear in a log line or drain_log
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
    # -- P1 / P2 / S6 -----------------------------------------------------------------------------
    def test_p1_private_names_are_in_no_serving_or_summary_config(self):
        from sync import sheet_summaries as ss  # infra/scripts/sync
        lists = {"SERVE_TABS keys": set(SERVE_TABS), "OPTIONAL_SERVE_TABS keys": set(OPTIONAL_SERVE_TABS),
                 "WRITABLE_TABS": set(WRITABLE_TABS), "DATA_TABS": set(ss.DATA_TABS),
                 "FINDING_TABS": set(ss.FINDING_TABS)}
        for name in PRIVATE_NAMES:
            for label, items in lists.items():
                self.assertFalse(any(name in str(i).lower().replace(" ", "_") for i in items), (name, label))
        self.assertTrue(set(NEW_NAMES) <= SERVE_DENY)
        for name in NEW_PUBLIC_NAMES:
            self.assertNotIn(name, SERVE_TABS)
            self.assertNotIn(name, OPTIONAL_SERVE_TABS)

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
        views = serve_sheet.fetch(spy, small_tabs=True)
        joined = " ".join(spy.sql)
        self.assertNotIn("x_intel_private", joined)
        for name in NEW_NAMES:
            self.assertNotIn(name, joined)
        self.assertEqual(set(views), set(SERVE_TABS.values()) | set(OPTIONAL_SERVE_TABS.values()))

    def test_s6_optional_tabs_are_off_by_default_and_only_the_two_names(self):
        self.assertEqual(set(OPTIONAL_SERVE_TABS.values()), {"Serve · Alpha Feed 7d", "Serve · X Trends TH 3d"})
        self.assertFalse(serve_sheet.small_tabs_on(Config(env={})))
        self.assertTrue(serve_sheet.small_tabs_on(Config(env={"X_LAKE_SERVE_SMALL_TABS": "on"})))
        self.assertEqual(serve_sheet.source_for("alpha_feed"), ("Serve · Alpha Feed 7d", "x_intel.alpha_feed"))
        self.assertEqual(serve_sheet.source_for("x_trends_th"), ("Serve · X Trends TH 3d", "x_intel.x_trends_th"))
        for tab in WRITABLE_TABS:
            assert_writable(tab)
        for tab in ("X Feed", "Alpha Feed", "X Trends TH", "AI Summaries", "Chief Data", "Serve · X Spaces"):
            with self.assertRaises(SheetError):
                assert_writable(tab)
        self.assertEqual(len(WRITABLE_TABS), 5)

    # -- I4 / I6 / P9 -----------------------------------------------------------------------------
    def test_i4_phone_numbers_are_masked_and_ids_dates_kept(self):
        self.assertEqual(mask_phones("โทร 081-234-5678 / 02 123 4567 / +1 555 123 4567"),
                         "โทร [phone] / [phone] / [phone]")
        for phone in ("053-123-456", "053 123 456", "(053) 123-456", "(02) 123 4567", "+1 (555) 123-4567",
                      "+66 (2) 123 4567", "+44 20 7946 0958"):
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
    def test_s1_s3_ddl_has_the_five_tables_and_no_feed_or_context_table(self):
        ddl = postgres_ddl()
        for t, pk in (("x_trends_th", "snapshot_at, rank, trend_name"),
                      ("x_trends_intl", "snapshot_at, woeid, rank, trend_name"),
                      ("x_topic_counts", "snapshot_at, query_id"), ("x_account_snapshots", "snapshot_at, list, handle"),
                      ("alpha_feed", "pulled_at_ict, source, sub_source, url, title")):
            self.assertIn(t, SMALL_CURATED)
            self.assertIn(t, CURATED)
            block = ddl.split(f"create table if not exists x_intel.{t} (")[1].split(");")[0]
            self.assertIn(f"primary key ({pk})", block)
            for lineage in ("created_at timestamptz", "updated_at timestamptz", "_batch_id text"):
                self.assertIn(lineage, block)
        self.assertIn("on x_intel.x_topic_counts (spike) where spike", ddl)
        self.assertIn("x_trends_intl_snapshot_date_woeid_idx", ddl)
        self.assertIn("x_account_snapshots_handle_snapshot_at_idx", ddl)
        self.assertIn("alpha_feed_time_bound_date_idx", ddl)
        self.assertIn("create table if not exists x_intel.drain_log", ddl)
        self.assertNotIn("x_intel.x_feed ", ddl)
        self.assertNotIn("x_intel.x_post_context", ddl)
        self.assertNotIn("x_intel_private", ddl)  # the private schema has its own DDL
        for name in PRIVATE_NAMES:
            self.assertNotIn(f"x_intel.{name}", ddl)

    def test_s8_docs_explain_the_choice(self):
        doc = (ROOT / "docs/specs/x-lake-private-datasets.md").read_text(encoding="utf-8")
        for phrase in ("500 MB", "x_feed", "x_post_context", "postgres", "X_LAKE_SERVE_SMALL_TABS", "drain_log",
                       "ai_status", "x_intel_private.ingest"):
            self.assertIn(phrase, doc)

    # -- rollback switches ------------------------------------------------------------------------
    def test_rollback_switches_remove_the_datasets_and_tables(self):
        code = ("import sys; sys.path.insert(0, %r); from x_lakehouse import schemas as s, inbox, private; "
                "import json; print(json.dumps([sorted(s.DATASETS), sorted(s.CURATED), sorted(s.PRIVATE_CURATED), "
                "'x_bookmarks' in inbox.inbox_ddl(), 'x_spaces' in inbox.inbox_ddl(), "
                "\"when 'x_bookmarks'\" in private.private_ddl()]))") % str(SCRIPTS)
        env = {**os.environ, "X_LAKE_NEW_DATASETS": "off", "X_LAKE_SMALL_CURATED": "off"}
        out = json.loads(subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True,
                                        check=True).stdout)
        names, tables, ptables, priv_in_public, spaces_in_public, priv_door = out
        self.assertEqual(len(names), 14)
        self.assertFalse(set(NEW_NAMES) & set(names))
        self.assertFalse(set(SMALL_CURATED) & set(tables))
        self.assertEqual(ptables, [])
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

    def table(self, name):
        c = CURATED.get(name) or PRIVATE_CURATED[name]
        return c.qualified

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
    def test_i1_s2_every_new_and_small_dataset_is_retry_safe_end_to_end(self):
        names = list(NEW_NAMES) + list(SMALL_CURATED)
        batches = {n: [row(n, i) for i in range(3)] for n in names}
        for n, rows in batches.items():
            first, second = self.ingest(n, rows), self.ingest(n, rows)
            self.assertEqual((first["inserted"], first["errors"]), (3, []), n)
            self.assertEqual((second["inserted"], second["skipped"]), (0, 3), n)
        code, beats, ev, _ = self.run_scheduled()
        self.assertEqual((code, beats), (0, [True]), ev.get("validate"))
        counts = {n: self.n(self.table(n)) for n in names}
        self.assertEqual(counts, {n: 3 for n in names})
        for n, rows in batches.items():  # again after the drain: still no new row anywhere
            self.assertEqual(self.ingest(n, rows)["inserted"], 0, n)
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual({n: self.n(self.table(n)) for n in names}, counts)

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
        folders = self.q("select folder_id from x_intel_private.x_bookmarks order by folder_id")
        self.assertEqual(folders, [("",), ("f1",)])  # '' is a value, never NULL
        for name in ("x_home_timeline", "x_spaces", "x_communities", "x_own_account_daily", "x_own_mentions"):
            self.assertEqual(self.n(self.table(name)), 2, name)

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
    def test_i4_i6_bot_sql_round_trip_masks_phones_in_inbox_raw_and_curated(self):
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
            cols = [c for c, _ in (CURATED.get(n) or PRIVATE_CURATED[n]).data_columns]
            [cur_row] = self.q(f"select {', '.join(cols)} from {self.table(n)}")
            raw = read_dataset(self.store, "", ds).to_pylist()[0]
            self.assertEqual(set(stored), set(rows[0]), n)  # every sent column round-trips
            for col in ds.mask_phone:
                for where, value in (("inbox", stored[col]), ("raw", raw[col]), ("curated", cur_row[cols.index(col)])):
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

    def test_bookmark_removal_outranks_a_late_active_row_with_the_same_last_seen(self):
        seen = ago(hours=3)
        removed = row("x_bookmarks", 1, last_seen_at=seen, removed_at=ago(hours=1))
        active = row("x_bookmarks", 1, last_seen_at=seen)
        self.ingest("x_bookmarks", [removed])
        self.assertEqual(self.run_scheduled()[0], 0)
        self.ingest("x_bookmarks", [active])  # a delayed observation from before the removal
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.q("select removed_at is not null from x_intel_private.x_bookmarks"), [(True,)])
        self.ingest("x_bookmarks", [active, removed], run_id="run1110u")  # same pair in one batch, any order
        again = row("x_bookmarks", 1, last_seen_at=ago(minutes=5))  # bookmarked again later
        self.ingest("x_bookmarks", [again])
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.q("select removed_at is null from x_intel_private.x_bookmarks"), [(True,)])

    def test_an_expired_removal_still_ends_a_stored_bookmark_and_nothing_older_revives_it(self):
        now = datetime.now(timezone.utc)
        seen = ict(now - timedelta(days=200))
        self.ingest("x_bookmarks", [row("x_bookmarks", 1, last_seen_at=seen)])
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.n("x_intel_private.x_bookmarks"), 1)  # still bookmarked: kept
        self.ingest("x_bookmarks", [row("x_bookmarks", 1, last_seen_at=seen, removed_at=ict(now - timedelta(days=190)))])
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.n("x_intel_private.x_bookmarks"), 0)  # removal applied, then pruned
        self.assertEqual(self.q("select tbl, pk from x_intel_private.retention_watermark"),
                         [("x_bookmarks", [row("x_bookmarks", 1)["post_id"], ""])])
        self.ingest("x_bookmarks", [row("x_bookmarks", 1, last_seen_at=ict(now - timedelta(days=210)))])  # late, older
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.n("x_intel_private.x_bookmarks"), 0)
        self.ingest("x_bookmarks", [row("x_bookmarks", 1, last_seen_at=ago(minutes=5))])  # bookmarked again
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.q("select removed_at from x_intel_private.x_bookmarks"), [(None,)])

    def test_validate_does_not_expect_a_pruned_bookmark_from_its_older_active_version(self):
        from x_lakehouse import validate
        now = datetime.now(timezone.utc)
        self.ingest("x_bookmarks", [row("x_bookmarks", 1, last_seen_at=ict(now - timedelta(days=200))),
                                    row("x_bookmarks", 1, last_seen_at=ict(now - timedelta(days=200)),
                                        removed_at=ict(now - timedelta(days=190)))])
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.n("x_intel_private.x_bookmarks"), 0)  # newest version is past retention
        self.assertEqual(read_dataset(self.store, "", DATASETS["x_bookmarks"]).num_rows, 2)
        cfg = Config(env={"X_LAKE_DB_URL": DB, "X_LAKE_URI": f"file://{self.tmp.name}"})
        rep = validate.reconcile(cfg, ["x_bookmarks"], None, None, sheet=False)["datasets"]["x_bookmarks"]
        self.assertEqual((rep["postgres_missing_keys"], rep["postgres_stale_versions"], rep["ok"]), (0, 0, True), rep)

    # -- I7 ----------------------------------------------------------------------------------------
    def test_i7_list_sources_upsert_by_list_id_newest_verification_wins(self):
        v1 = row("x_list_sources", 1, verified_at=ago(hours=5), status="ok")
        v2 = row("x_list_sources", 1, verified_at=ago(hours=1), status="stale")
        self.ingest("x_list_sources", [v2])
        self.ingest("x_list_sources", [v1])  # an older verification arriving late
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.q("select status from x_intel.x_list_sources"), [("stale",)])

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
        statements += [f"select count(*) from {c.qualified}" for c in PRIVATE_CURATED.values()]
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
        self.assertEqual(len(rls), 3 + len(PRIVATE_CURATED))  # inbox, seen, retention_watermark + curated
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

    # -- R1-R4 / S4 --------------------------------------------------------------------------------
    def test_r1_r4_s4_retention_is_per_table_idempotent_and_never_touches_raw(self):
        now = datetime.now(timezone.utc)
        old_new = {  # name -> (rows past retention, rows kept)
            "x_home_timeline": ([row("x_home_timeline", 1, collected_at=ict(now - timedelta(days=31)))],
                                [row("x_home_timeline", 2, collected_at=ict(now - timedelta(days=29)))]),
            "x_own_mentions": ([row("x_own_mentions", 1, collected_at=ict(now - timedelta(days=366)))],
                               [row("x_own_mentions", 2, collected_at=ict(now - timedelta(days=360)))]),
            "x_communities": ([row("x_communities", 1, snapshot_date=today_ict(366))],
                              [row("x_communities", 2, snapshot_date=today_ict(364))]),
            "x_spaces": ([row("x_spaces", 1, collected_at=ict(now - timedelta(days=181)))],
                         [row("x_spaces", 2, collected_at=ict(now - timedelta(days=179)))]),
            "x_bookmarks": ([row("x_bookmarks", 1, last_seen_at=ict(now - timedelta(days=200)),
                                 removed_at=ict(now - timedelta(days=181)))],
                            [row("x_bookmarks", 2, last_seen_at=ict(now - timedelta(days=900))),  # still bookmarked
                             row("x_bookmarks", 3, removed_at=ict(now - timedelta(days=10)))]),
            "x_own_account_daily": ([], [row("x_own_account_daily", 0, snapshot_date=today_ict(800),
                                             snapshot_at=ict(now - timedelta(days=800)))]),
            "x_list_sources": ([], [row("x_list_sources", 1, verified_at=ict(now - timedelta(days=800)))]),
        }
        for name in SMALL_CURATED:
            col = SMALL_CURATED[name].retain_col
            old_new[name] = ([row(name, 1, **{col: ict(now - timedelta(days=91))})],
                             [row(name, 2, **{col: ict(now - timedelta(days=89))})])
        # Rows already past retention are written to raw but never upserted (a replay cannot revive them).
        for name, (old, new) in old_new.items():
            self.assertEqual(self.ingest(name, old + new)["errors"], [], name)
        self.assertEqual(self.run_scheduled()[0], 0)
        expect = {n: len(new) for n, (_, new) in old_new.items()}
        self.assertEqual({n: self.n(self.table(n)) for n in old_new}, expect)
        # Rows that aged in place (inserted directly, as if stored months ago) are pruned by the cron.
        for name, (old, _) in old_new.items():
            if not old:
                continue
            c = CURATED.get(name) or PRIVATE_CURATED[name]
            typed = type_row(c.source, old[0]).values
            cols = [k for k, _ in c.data_columns]
            with self.conn.cursor() as q:
                q.execute(f"insert into {c.qualified} ({', '.join(cols)}) values ({', '.join(['%s'] * len(cols))}) "
                          "on conflict do nothing", [typed.get(k) for k in cols])
            self.conn.commit()
        raw_before = sorted(self.store.list("raw/") + self.store.list("private/raw/"))
        for _ in range(2):  # idempotent
            code, _, ev, _ = self.run_scheduled()
            self.assertEqual(code, 0)
            self.assertEqual({n: self.n(self.table(n)) for n in old_new}, expect)
        self.assertEqual(sorted(self.store.list("raw/") + self.store.list("private/raw/")), raw_before)
        for name, (old, new) in old_new.items():  # raw keeps every row ever drained
            self.assertEqual(read_dataset(self.store, "", DATASETS[name]).num_rows, len(old) + len(new), name)

    # -- R5 ----------------------------------------------------------------------------------------
    def test_r5_backfill_execute_is_idempotent_and_keeps_the_gap(self):
        out = io.StringIO()
        with patch.dict(os.environ, {"X_LAKE_DB_URL": DB}), patch("sys.stdout", out):
            self.assertEqual(own_account_backfill.main([str(FIXTURE_CSV), "--execute"]), 0)
            self.assertEqual(own_account_backfill.main([str(FIXTURE_CSV), "--execute"]), 0)
        first, second = (json.loads(x) for x in out.getvalue().splitlines())
        self.assertEqual((first["inserted"], second["inserted"], second["skipped"]), (18, 0, 18))
        self.assertEqual(self.run_scheduled()[0], 0)
        rows = self.q("select snapshot_date, source, snapshot_at, listed, followers_delta, posts_24h, reposts_24h, "
                      "mentions_24h from x_intel_private.x_own_account_daily")
        self.assertEqual(len(rows), 18)
        days = {r[0].isoformat() for r in rows}
        self.assertFalse(days & {f"2026-11-0{d}" for d in range(2, 10)})
        self.assertTrue(all(r[1] == "backfill_csv" and all(v is None for v in r[2:]) for r in rows))

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

    # -- S6 ----------------------------------------------------------------------------------------
    def test_s6_optional_tabs_read_only_the_new_tables(self):
        self.ingest("alpha_feed", [row("alpha_feed", i) for i in range(2)])
        self.ingest("x_trends_th", [row("x_trends_th", i) for i in range(3)])
        self.ingest("x_trends_th", [row("x_trends_th", 9, snapshot_at=ago(days=4))])  # outside the 3 days
        self.assertEqual(self.run_scheduled()[0], 0)
        default = serve_sheet.fetch(self.conn)
        self.assertEqual(set(default), set(SERVE_TABS.values()))
        views = serve_sheet.fetch(self.conn, small_tabs=True)
        self.assertEqual(len(views["Serve · Alpha Feed 7d"][1]), 2)
        self.assertEqual(len(views["Serve · X Trends TH 3d"][1]), 3)
        self.assertEqual(views["Serve · X Trends TH 3d"][0], DATASETS["x_trends_th"].names)

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
            self.assertEqual((self.n("x_intel_private.x_bookmarks"), self.n("x_intel.x_spaces")), (1, 1))
        finally:
            inbox.ensure_inbox(self.conn)  # switch back on: both doors accept again
            private.ensure_private(self.conn)
        self.assertEqual(self.ingest("x_bookmarks", [row("x_bookmarks", 2)])["inserted"], 1)

    def test_standalone_validate_reports_private_tables_as_not_provisioned(self):
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
        self.assertEqual({r["postgres_skipped"] for r in rep["datasets"].values()}, {"schema x_intel_private not created yet"})

    def test_small_tables_are_seeded_from_raw_history_they_never_saw(self):
        self.ingest("x_trends_th", [row("x_trends_th", i) for i in range(3)])
        self.ingest("x_trends_th", [row("x_trends_th", 7, snapshot_at=ago(days=100))])  # raw only: past 90 days
        self.assertEqual(self.run_scheduled()[0], 0)
        with self.conn.cursor() as c:  # as if the table was created (or switched on) after this history landed
            c.execute("delete from x_intel.x_trends_th")
        self.conn.commit()
        code, _, ev, _ = self.run_scheduled()
        self.assertEqual(code, 0, ev.get("validate"))
        self.assertEqual(ev["seed_small_curated"]["inserted"]["x_trends_th"], 3)
        self.assertEqual(self.n("x_intel.x_trends_th"), 3)
        self.assertEqual(self.run_scheduled()[2]["seed_small_curated"]["inserted"]["x_trends_th"], 0)  # idempotent

    def test_scoped_validate_sees_a_removal_outside_the_window(self):
        from x_lakehouse import validate
        now = datetime.now(timezone.utc)
        removal = row("x_bookmarks", 1, last_seen_at=ict(now - timedelta(days=220)),
                      removed_at=ict(now - timedelta(days=200)))
        active = row("x_bookmarks", 1, last_seen_at=ict(now - timedelta(days=210)))
        self.ingest("x_bookmarks", [removal, active])
        self.assertEqual(self.run_scheduled()[0], 0)
        self.assertEqual(self.n("x_intel_private.x_bookmarks"), 0)
        cfg = Config(env={"X_LAKE_DB_URL": DB, "X_LAKE_URI": f"file://{self.tmp.name}"})
        since = (now - timedelta(days=215)).astimezone(TZ).date()
        rep = validate.reconcile(cfg, ["x_bookmarks"], since, None, sheet=False)["datasets"]["x_bookmarks"]
        self.assertEqual((rep["postgres_missing_keys"], rep["ok"]), (0, True), rep)

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
