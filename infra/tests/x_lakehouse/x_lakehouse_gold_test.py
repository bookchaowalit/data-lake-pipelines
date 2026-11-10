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
        "x_feed": f"select * from (values ('1', 'test_a', 3, 100, {ts}), ('2', 'test_b', 1, 10, {ts}))"
                  " t(post_id, author_handle, likes, views, collected_at)",
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
        self.assertEqual(report["queries"]["02_top_accounts_by_signals"]["rows"], 2)
        self.assertFalse((self.root / "gold").exists())

    def test_write_lands_one_part_per_query_and_a_manifest(self):
        report = self.run_gold(write=True)
        self.assertTrue(report["ok"], report)
        part = self.root / "gold" / "05_account_follower_growth" / "as_of=2026-11-10" / "run=2220.parquet"
        rows = duckdb.connect().execute(f"select handle, gained from read_parquet('{part}')").fetchall()
        self.assertEqual(rows, [("test_a", 30)])
        [manifest] = (self.root / "_manifests" / "gold").glob("*.json")
        body = json.loads(manifest.read_text())
        self.assertEqual(len(body["parts"]), 5)
        self.assertEqual(body["as_of"], "2026-11-10")

    def test_parts_are_write_once(self):
        self.run_gold(write=True)
        with self.assertRaises(LakeError):
            self.run_gold(write=True)  # same as_of and run=HHMM: refuses to overwrite
        later = self.run_gold(write=True, now=NOW + timedelta(hours=8))
        self.assertTrue(later["ok"])
        self.assertTrue((self.root / "gold" / "01_daily_volume_per_domain" / "as_of=2026-11-11").exists())

    def test_one_bad_query_does_not_stop_the_others(self):
        bad = gold.QUERIES / "99_broken_test_only.sql"
        bad.write_text("select * from read_parquet('{lake}/raw/nope/*.parquet')")
        self.addCleanup(bad.unlink)
        report = self.run_gold(write=True)
        self.assertFalse(report["ok"])
        self.assertEqual(report["queries"]["99_broken_test_only"]["status"], "error")
        self.assertEqual(report["queries"]["01_daily_volume_per_domain"]["status"], "ok")

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
