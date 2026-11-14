#!/usr/bin/env python3
"""Gold on the lake (x_lakehouse/gold.py): queries/ over a small file:// raw lake, written once per run."""

from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import gold, scheduled  # noqa: E402
from x_lakehouse.config import Config  # noqa: E402
from x_lakehouse.lake import LakeError, LocalStore  # noqa: E402

NOW = datetime(2026, 11, 10, 15, 20, tzinfo=timezone.utc)  # 22:20 ICT


def build_lake(root: Path) -> None:
    con = duckdb.connect()
    day = (NOW - timedelta(hours=2)).strftime("%Y-%m-%d")
    ts = f"TIMESTAMPTZ '{(NOW - timedelta(hours=2)).isoformat()}'"
    tables = {
        "x_signals": f"select * from (values ('1', 'D5 Technology', 'urgent_candidate', 'T1', 'alert_digest', {ts}),"
                     f" ('2', 'D2 Economy', 'none', 'T3', 'none', {ts})) t(post_id, domain, alert_tier, cred_tier,"
                     " route, collected_at)",
        # Post 1 seen by two watches (counts once, newest counts); post 3 by test_a too.
        "x_feed": f"select * from (values ('1', 'test_a', 'keyword', 'ai', 3, 0, 0, 100, {ts} - interval 1 hour),"
                  f" ('1', 'test_a', 'account', 'test_a', 5, 1, 0, 150, {ts}),"
                  f" ('3', 'test_a', 'keyword', 'ai', 2, 0, 0, 20, {ts}),"
                  f" ('2', 'test_b', 'keyword', 'ai', 1, 0, 0, 10, {ts}))"
                  " t(post_id, author_handle, watch_type, watch_value, likes, reposts, replies, views, collected_at)",
        "x_topic_counts": f"select * from (values ('q1', 'ai', {ts}, 50, 9, 4.0, true, {ts}))"
                          " t(query_id, topic, snapshot_at, total_24h, max_hour_24h, ratio, spike, _ingested_at)",
        "x_account_snapshots": f"select * from (values ('test_a', 'u1', {ts} - interval 2 day, 100, 'ok'),"
                               f" ('test_a', 'u1', {ts}, 130, 'ok')) t(handle, user_id, snapshot_at, followers_count,"
                               " account_status)",
    }
    for name, sql in tables.items():
        path = root / "raw" / name / f"date={day}"
        path.mkdir(parents=True)
        con.execute(f"copy (select *, null::varchar as _coerce_errors from ({sql})) "
                    f"to '{path / 'part-0000.parquet'}' (format parquet)")
    con.close()


class GoldTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        build_lake(self.root)
        self.cfg = Config(env={"X_LAKE_URI": f"file://{self.root}"})

    def run_gold(self, write: bool, now: datetime = NOW):
        store = LocalStore(self.root) if write else None
        return gold.run(self.cfg, write=write, now=now, store=store, prefix="")

    def test_dry_run_counts_and_writes_nothing(self):
        report = self.run_gold(write=False)
        self.assertTrue(report["ok"], report)
        self.assertEqual(set(report["queries"]), {f.stem for f in gold.QUERIES.glob("*.sql")})
        self.assertEqual(report["queries"]["02_top_accounts_by_posts"]["rows"], 2)
        self.assertFalse((self.root / "gold").exists())

    def test_write_lands_one_part_per_query_and_a_manifest(self):
        report = self.run_gold(write=True)
        self.assertTrue(report["ok"], report)
        part = self.root / "gold" / "05_account_follower_growth" / "as_of=2026-11-10" / "run=222000.parquet"
        rows = duckdb.connect().execute(f"select handle, gained from read_parquet('{part}')").fetchall()
        self.assertEqual(rows, [("test_a", 30)])
        [manifest] = (self.root / "_manifests" / "gold").glob("*.json")
        body = json.loads(manifest.read_text())
        self.assertEqual(len(body["parts"]), 5)
        self.assertEqual(body["as_of"], "2026-11-10")

    def test_windows_end_at_the_run_clock_not_the_wall_clock(self):
        """Queries take {now} from the run: fixtures dated NOW stay inside the windows whatever today is, and a run
        60 days later sees them aged out."""
        at_run = self.run_gold(write=False)["queries"]
        self.assertEqual(at_run["05_account_follower_growth"]["rows"], 1)
        self.assertEqual(at_run["02_top_accounts_by_posts"]["rows"], 2)
        later = self.run_gold(write=False, now=NOW + timedelta(days=60))["queries"]
        self.assertEqual(later["05_account_follower_growth"]["rows"], 0)
        self.assertEqual(later["02_top_accounts_by_posts"]["rows"], 0)

    def test_top_accounts_count_each_post_once_with_its_newest_counts(self):
        self.run_gold(write=True)
        [part] = (self.root / "gold" / "02_top_accounts_by_posts").rglob("*.parquet")
        rows = duckdb.connect().execute(f"select author_handle, posts, total_likes, total_views, watches, "
                                        f"main_watch from read_parquet('{part}')").fetchall()
        self.assertEqual(rows, [("test_a", 2, 7, 170, 2, "keyword:ai"), ("test_b", 1, 1, 10, 1, "keyword:ai")])
        # A run dated between post 1's two observations (NOW-3h, NOW-2h) sees only the first one.
        early = gold.run(self.cfg, write=False, now=NOW - timedelta(hours=2, minutes=30))["queries"]
        self.assertEqual(early["02_top_accounts_by_posts"]["rows"], 1)  # only post 1's first observation

    def test_daily_volume_counts_a_post_once_per_watch_and_day(self):
        self.run_gold(write=True)
        [part] = (self.root / "gold" / "01_daily_volume_per_watch").rglob("*.parquet")
        rows = duckdb.connect().execute(f"select watch, posts, engagement from read_parquet('{part}') "
                                        "order by watch").fetchall()
        # keyword:ai saw posts 1, 2, 3 (post 1: likes 3); account:test_a saw post 1 (likes 5 + 2 x 1 repost).
        self.assertEqual(rows, [("account:test_a", 1, 7), ("keyword:ai", 3, 6)])

    def test_no_query_reads_duckdb_now(self):
        for f in gold.QUERIES.glob("*.sql"):
            self.assertNotIn("now()", f.read_text().lower(), f.name)

    def test_parts_are_write_once(self):
        self.run_gold(write=True)
        with self.assertRaises(LakeError):
            self.run_gold(write=True)  # same stamp: the manifest is refused first
        self.assertEqual(len(list((self.root / "_manifests" / "gold").glob("*.json"))), 1)
        # Same minute, other second: its own names, so it lands beside the first run instead of failing
        second = self.run_gold(write=True, now=NOW + timedelta(seconds=30))
        self.assertTrue(second["ok"])
        manifests = sorted((self.root / "_manifests" / "gold").glob("*.json"))
        self.assertEqual(len(manifests), 2)
        keys = [{p["key"] for p in json.loads(m.read_text())["parts"]} for m in manifests]
        self.assertFalse(keys[0] & keys[1])  # no manifest claims another run's part
        later = self.run_gold(write=True, now=NOW + timedelta(hours=8))
        self.assertTrue(later["ok"])
        self.assertTrue((self.root / "gold" / "01_daily_volume_per_watch" / "as_of=2026-11-11").exists())

    def test_manifest_is_written_before_any_part(self):
        order = []
        store = LocalStore(self.root)
        real = store.put_new

        def put(key, data):
            order.append("manifest" if key.startswith("_manifests/") else "part")
            real(key, data)

        store.put_new = put  # type: ignore[method-assign]
        gold.run(self.cfg, write=True, now=NOW, store=store, prefix="")
        self.assertEqual(order[0], "manifest")
        self.assertEqual(order.count("part"), 5)

    def test_a_crash_mid_write_leaves_only_listed_parts(self):
        store = LocalStore(self.root)
        real, calls = store.put_new, []

        def put(key, data):
            calls.append(key)
            if len(calls) == 3:  # manifest + 1 part landed, then the process dies
                raise OSError("simulated crash")
            real(key, data)

        store.put_new = put  # type: ignore[method-assign]
        with self.assertRaises(OSError):
            gold.run(self.cfg, write=True, now=NOW, store=store, prefix="")
        [manifest] = (self.root / "_manifests" / "gold").glob("*.json")
        listed = {p["key"] for p in json.loads(manifest.read_text())["parts"]}
        landed = {str(p.relative_to(self.root)) for p in (self.root / "gold").rglob("*.parquet")}
        self.assertEqual(len(landed), 1)
        self.assertTrue(landed <= listed)

    def test_one_bad_query_does_not_stop_the_others(self):
        bad = gold.QUERIES / "99_broken_test_only.sql"
        bad.write_text("select * from read_parquet('{lake}/raw/nope/*.parquet')")
        self.addCleanup(bad.unlink)
        report = self.run_gold(write=True)
        self.assertFalse(report["ok"])
        self.assertEqual(report["queries"]["99_broken_test_only"]["status"], "error")
        self.assertEqual(report["queries"]["01_daily_volume_per_watch"]["status"], "ok")

    def test_scheduled_hook_is_off_by_default_and_never_raises(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            scheduled._gold(Config(env={}))
            scheduled._gold(Config(env={"X_LAKE_GOLD": "write"}))  # no X_LAKE_URI: logged, not raised
        logs = [json.loads(line) for line in buf.getvalue().splitlines()]
        self.assertEqual(len(logs), 1)
        self.assertEqual((logs[0]["event"], logs[0]["status"]), ("gold", "error"))


if __name__ == "__main__":
    unittest.main()
