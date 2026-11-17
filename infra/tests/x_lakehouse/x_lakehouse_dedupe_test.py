#!/usr/bin/env python3
"""Tests for the one-off x_home_timeline raw repair (x_lakehouse.dedupe_raw).

Synthetic rows only; every text value says "synthetic". LocalStore only, no network. The guard that
reads the private inbox runs only with X_LAKE_TEST_DB_URL (a throwaway database).
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import dedupe_raw  # noqa: E402
from x_lakehouse.coerce import TZ, type_row  # noqa: E402
from x_lakehouse.lake import LakeError, LocalStore, read_dataset, write_raw  # noqa: E402
from x_lakehouse.schemas import DATASETS  # noqa: E402

DS = DATASETS["x_home_timeline"]
DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
DAY1 = datetime(2026, 11, 14, 7, 5, tzinfo=TZ)


def row(
    post: str, at: datetime, likes: int = 3, views: int = 100, rank: int = 1
) -> dict[str, object]:
    return {
        "post_id": post,
        "collected_at": at.isoformat(),
        "posted_at": (DAY1 - timedelta(hours=2)).isoformat(),
        "author_handle": "synthetic_author",
        "author_id": "42",
        "author_followers": "10",
        "text": "synthetic post",
        "lang": "th",
        "likes": str(likes),
        "reposts": "0",
        "replies": "1",
        "quotes": "0",
        "views": str(views),
        "age_min": "120",
        "velocity": "0.1",
        "eng_rate": "0.04",
        "rank": str(rank),
        "domain": "",
        "topic": "",
        "filter_flags": "",
        "url": f"https://x.com/i/{post}",
    }


class DedupeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = LocalStore(Path(self.tmp.name))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def send(self, cells: list[dict[str, object]], at: datetime) -> None:
        typed = [type_row(DS, c) for c in cells]
        write_raw(
            DS,
            typed,
            [t.values["collected_at"] for t in typed],
            store=self.store,
            prefix="p",
            source="inbox",
            execute=True,
            run_date=at.date(),
            now=at.astimezone(timezone.utc),
        )

    def seed(self) -> None:
        """One fetch at 07:05 sent three times (08:00, 16:00, next day 00:10); one real re-pull."""
        fetch = [row("1", DAY1), row("2", DAY1, likes=0)]
        self.send(fetch, DAY1)
        self.send(
            [
                row("1", DAY1 + timedelta(hours=9), rank=4),
                row("2", DAY1 + timedelta(hours=9), likes=0),
            ],
            DAY1 + timedelta(hours=9),
        )
        # Resent after midnight: lands in the next day's partition.
        self.send([row("1", DAY1 + timedelta(hours=17))], DAY1 + timedelta(hours=17))
        # A real re-pull: counts moved, so it stays.
        self.send(
            [row("1", DAY1 + timedelta(hours=18), likes=9, views=400)],
            DAY1 + timedelta(hours=18),
        )

    def test_plan_keeps_the_earliest_copy_and_real_repulls(self) -> None:
        self.seed()
        p = dedupe_raw.plan(self.store, "p")
        self.assertEqual(p["rows"], 6)
        self.assertEqual(p["drop"], 3)
        self.assertEqual(
            {d: v["drop"] for d, v in p["days"].items()},
            {"2026-11-14": 2, "2026-11-15": 1},
        )
        self.assertNotIn("synthetic", json.dumps(dedupe_raw.summary(p)))  # counts only

    def test_apply_backs_up_rewrites_and_is_idempotent(self) -> None:
        self.seed()
        before = {k: self.store.read(k) for k in self.store.list("p/private/raw/")}
        out = dedupe_raw.apply(
            self.store,
            "p",
            dedupe_raw.plan(self.store, "p"),
            now=datetime(2026, 11, 18, tzinfo=timezone.utc),
        )
        self.assertEqual(out["dropped"], 3)
        t = read_dataset(self.store, "p", DS)
        assert t is not None
        got = sorted(
            zip(
                t.column("post_id").to_pylist(),
                t.column("likes").to_pylist(),
                [c.astimezone(TZ).hour for c in t.column("collected_at").to_pylist()],
            )
        )
        self.assertEqual(got, [("1", 3, 7), ("1", 9, 1), ("2", 0, 7)])
        # Every original part is in the backup, byte for byte, outside the raw listing.
        backup = {
            k: self.store.read(k)
            for k in self.store.list(out["backup"] + "/")
            if k.endswith(".parquet")
        }
        self.assertEqual(sorted(backup.values()), sorted(before.values()))
        self.assertFalse(
            any("/_backup/" in k for k in self.store.list("p/private/raw/"))
        )
        # One part per partition, and the writer still dedupes a resend of a kept row.
        self.assertEqual(len(dedupe_raw._parts(self.store, "p")["2026-11-14"]), 1)
        self.send([row("1", DAY1)], DAY1 + timedelta(days=1))
        self.assertEqual(read_dataset(self.store, "p", DS).num_rows, 3)  # type: ignore[union-attr]
        # Rerun: nothing left to drop.
        self.assertEqual(dedupe_raw.plan(self.store, "p")["drop"], 0)

    def test_new_part_number_is_reserved_for_the_next_drain(self) -> None:
        self.seed()
        dedupe_raw.apply(
            self.store,
            "p",
            dedupe_raw.plan(self.store, "p"),
            now=datetime(2026, 11, 18, tzinfo=timezone.utc),
        )
        self.send([row("3", DAY1 + timedelta(hours=10))], DAY1 + timedelta(hours=10))
        keys = dedupe_raw._parts(self.store, "p")["2026-11-14"]
        self.assertEqual(len(keys), 2)
        self.assertEqual(read_dataset(self.store, "p", DS).num_rows, 4)  # type: ignore[union-attr]

    def test_half_done_repair_is_refused(self) -> None:
        self.seed()
        p = dedupe_raw.plan(self.store, "p")
        real_delete = self.store.delete
        self.store.delete = lambda key: (_ for _ in ()).throw(OSError("crash"))  # type: ignore[method-assign]
        with self.assertRaises(OSError):
            dedupe_raw.apply(
                self.store, "p", p, now=datetime(2026, 11, 18, tzinfo=timezone.utc)
            )
        self.store.delete = real_delete  # type: ignore[method-assign]
        p2 = dedupe_raw.plan(self.store, "p")
        self.assertTrue(p2["half_done"])
        with self.assertRaises(LakeError):
            dedupe_raw.apply(
                self.store, "p", p2, now=datetime(2026, 11, 18, 1, tzinfo=timezone.utc)
            )

    def test_rows_with_a_coerce_error_in_a_fetch_column_are_kept(self) -> None:
        a = row("7", DAY1) | {"views": "100-ish"}
        b = row("7", DAY1 + timedelta(hours=9)) | {"views": "200-ish"}
        self.send([a], DAY1)
        self.send([b], DAY1 + timedelta(hours=9))
        self.assertEqual(dedupe_raw.plan(self.store, "p")["drop"], 0)

    def test_rows_with_an_uncoercible_collected_at_are_kept(self) -> None:
        self.send([row("8", DAY1)], DAY1)
        typed = [type_row(DS, row("8", DAY1) | {"collected_at": "yesterday-ish"})]
        write_raw(DS, typed, [DAY1], store=self.store, prefix="p", source="inbox", execute=True,
                  run_date=DAY1.date(), now=DAY1.astimezone(timezone.utc))
        p = dedupe_raw.plan(self.store, "p")
        self.assertEqual((p["rows"], p["drop"]), (2, 0))

    def test_part_numbers_reserved_by_a_manifest_are_skipped(self) -> None:
        self.seed()
        # A drain that crashed after its manifest entry and before its part reserved part-0005.
        self.store.put_new("p/private/_manifests/x_home_timeline/crashed/date=2026-11-14-part-0005.json", b"{}")
        out = dedupe_raw.apply(self.store, "p", dedupe_raw.plan(self.store, "p"),
                               now=datetime(2026, 11, 18, tzinfo=timezone.utc))
        self.assertTrue(dedupe_raw._parts(self.store, "p")["2026-11-14"][0].endswith("part-0006.parquet"))
        self.assertEqual(out["dropped"], 3)

    @unittest.skipUnless(DB, "X_LAKE_TEST_DB_URL not set")
    def test_recently_drained_rows_block_the_repair(self) -> None:
        from x_lakehouse import curated as cur
        from x_lakehouse import private

        conn = cur.connect(DB)
        try:
            with conn.cursor() as c:
                c.execute("drop schema if exists x_intel_private cascade")
                c.execute("create schema x_intel_private")
                c.execute(
                    f"create table {private.P_INBOX} (dataset text, row_key text, drained_at timestamptz)"
                )
                now = datetime.now(timezone.utc)
                c.execute(
                    f"insert into {private.P_INBOX} values ('x_home_timeline', 'old', %s), "
                    f"('x_home_timeline', 'new', %s)",
                    (now - timedelta(hours=30), now - timedelta(hours=2)),
                )
            conn.commit()
            self.assertEqual(dedupe_raw.recently_drained(conn, {"old"}, now), 0)
            self.assertEqual(dedupe_raw.recently_drained(conn, {"old", "new"}, now), 1)
            with conn.cursor() as c:
                c.execute("drop schema x_intel_private cascade")
            conn.commit()
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
