#!/usr/bin/env python3
"""Stable watchlist row numbers for the Sheet cutover (x_lakehouse/watchlist_rows.py).

Postgres tests run only with X_LAKE_TEST_DB_URL (throwaway database; they drop schema x_intel).
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from x_lakehouse import curated as cur  # noqa: E402
from x_lakehouse import watchlist_rows as wr  # noqa: E402
from x_lakehouse.inbox import ensure_inbox  # noqa: E402

DB = os.environ.get("X_LAKE_TEST_DB_URL", "")
A, B, C, D = ("chief-data", "account", "a"), ("chief-data", "account", "b"), ("chief-x", "keyword", "c"), \
    ("chief-x", "keyword", "d")


class PlanTests(unittest.TestCase):
    def test_new_keys_take_their_sheet_numbers(self):
        plan = wr.plan_seed([(2, A), (3, B)], {})
        self.assertEqual(plan["to_insert"], [(2, A), (3, B)])
        self.assertEqual((plan["moved"], plan["number_conflicts"]), ([], []))

    def test_never_renumbers(self):
        plan = wr.plan_seed([(2, B), (3, A)], {A: 2, B: 3})
        self.assertEqual(len(plan["moved"]), 2)
        self.assertEqual(plan["to_insert"], [])

    def test_a_taken_number_is_a_conflict(self):
        plan = wr.plan_seed([(2, C)], {A: 2})
        self.assertEqual(plan["number_conflicts"], [2])

    def test_duplicate_keys_in_the_tab_are_reported(self):
        self.assertEqual(wr.plan_seed([(2, A), (5, A)], {})["duplicate_keys_in_sheet"], [5])


@unittest.skipUnless(DB, "set X_LAKE_TEST_DB_URL to a throwaway Postgres")
class MapPostgresTests(unittest.TestCase):
    def setUp(self):
        self.conn = cur.connect(DB)
        with self.conn.cursor() as c:
            c.execute("drop schema if exists x_intel cascade")
        self.conn.commit()
        ensure_inbox(self.conn)

    def tearDown(self):
        self.conn.rollback()
        self.conn.close()

    def add(self, *keys):
        with self.conn.cursor() as c:
            for chief, typ, value in keys:
                c.execute("insert into x_intel.x_watchlist (requesting_chief, type, value) values (%s, %s, %s)",
                          (chief, typ, value))
        self.conn.commit()

    def view(self):
        with self.conn.cursor() as c:
            c.execute("select requesting_chief, type, value, sheet_row from x_intel.v_watchlist_sheet")
            rows = c.fetchall()
        self.conn.commit()
        return rows

    def test_trigger_waits_for_the_seed(self):
        self.add(A, B)
        self.assertEqual(wr.read_map(self.conn), {})  # nothing numbered before the tab is seeded
        self.assertEqual([r[3] for r in self.view()], [None, None])

    def test_seed_uses_tab_numbers_then_numbers_the_rest_and_new_rows(self):
        self.add(A, B, C)  # C is in curated but not in the tab (sent by a bot through the inbox)
        dry = wr.seed(self.conn, [(2, A), (7, B)], execute=False)
        self.assertEqual((dry["to_insert"], dry["written"], dry["curated_unmapped_before"]), (2, 0, 3))
        self.assertEqual(wr.read_map(self.conn), {})
        done = wr.seed(self.conn, [(2, A), (7, B)], execute=True)
        self.assertEqual((done["written"], done["curated_numbered_after_tab"]), (3, 1))
        self.assertEqual(wr.read_map(self.conn), {A: 2, B: 7, C: 8})
        self.add(D)  # a new entry after the seed: the trigger numbers it like an append to the tab
        self.assertEqual(wr.read_map(self.conn)[D], 9)
        self.assertEqual(self.view(), [(*A, 2), (*B, 7), (*C, 8), (*D, 9)])

    def test_seed_is_idempotent_and_blocks_on_a_moved_row(self):
        self.add(A, B)
        wr.seed(self.conn, [(2, A), (3, B)], execute=True)
        again = wr.seed(self.conn, [(2, A), (3, B)], execute=True)
        self.assertEqual((again["written"], again["blocked"]), (0, False))
        moved = wr.seed(self.conn, [(2, B), (3, A)], execute=True)
        self.assertTrue(moved["blocked"])
        self.assertEqual(moved["written"], 0)
        self.assertEqual(wr.read_map(self.conn), {A: 2, B: 3})

    def test_upsert_of_an_existing_entry_keeps_its_number(self):
        self.add(A)
        wr.seed(self.conn, [(5, A)], execute=True)
        with self.conn.cursor() as c:
            c.execute("insert into x_intel.x_watchlist (requesting_chief, type, value, purpose) values (%s, %s, %s, 'x')"
                      " on conflict (requesting_chief, type, value) do update set purpose = excluded.purpose", A)
        self.conn.commit()
        self.assertEqual(wr.read_map(self.conn), {A: 5})

    def test_replaces_the_old_view_in_place(self):
        """Production has the 7-column view ordered by key; the new one only adds sheet_row at the end."""
        with self.conn.cursor() as c:
            c.execute("drop view x_intel.v_watchlist_sheet")
            c.execute("create view x_intel.v_watchlist_sheet as select requesting_chief, type, value, purpose, "
                      "added_date::text as added_date, last_used::text as last_used, tag from x_intel.x_watchlist "
                      "order by requesting_chief, type, value")
        self.conn.commit()
        ensure_inbox(self.conn)
        self.assertEqual(self.view(), [])


if __name__ == "__main__":
    unittest.main()
