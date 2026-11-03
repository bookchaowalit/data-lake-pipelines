#!/usr/bin/env python3
"""Tests for the jobs BI static site generator (snapshot DuckDB → HTML)."""

from __future__ import annotations

import re
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

HOSTILE = '<script>alert(1)</script>"&\''
PAGES = ["index.html", "boards/index.html", "skills/index.html", "companies/index.html", "404.html"]


def _snapshot(path: Path, *, as_of: str = "2026-10-22", empty: bool = False) -> None:
    """A snapshot with the same tables and columns jobs_bi_snapshot.py writes."""
    import duckdb

    con = duckdb.connect(str(path))
    try:
        con.execute(
            "create table jobs_postings_daily (posted_date date, board varchar, postings bigint, "
            "companies bigint, with_salary bigint)"
        )
        con.execute(
            "create table jobs_board_health (board varchar, postings_total bigint, companies bigint, "
            "last_posted_date date, last_seen_at timestamp, postings_7d bigint, as_of_date date)"
        )
        con.execute(
            "create table jobs_skills_demand (posted_week date, tag varchar, postings bigint, demand_rank bigint)"
        )
        con.execute(
            "create table jobs_company_hiring (company_key varchar, company varchar, postings_total bigint, "
            "postings_30d bigint, boards varchar[], first_posted_date date, last_posted_date date)"
        )
        con.execute(
            "create table gold_meta as select 'gold-abc' as gold_run_id, 'silver-def' as silver_run_id, "
            f"date '{as_of}' as as_of_date, timestamp '{as_of} 06:00:00' as max_seen_at"
        )
        if empty:
            return
        con.execute(
            f"""
            insert into jobs_postings_daily values
                (date '{as_of}' - 100, 'Old', 9, 1, 0),
                (date '{as_of}' - 89, 'Himalayas', 2, 1, 1),
                (date '{as_of}', 'Himalayas', 3, 2, 0),
                (date '{as_of}', ?, 1, 1, 0),
                (date '{as_of}' + 30, 'Future', 7, 1, 0)
            """,
            [HOSTILE],
        )
        con.execute(
            f"""
            insert into jobs_board_health values
                ('Himalayas', 5, 2, date '{as_of}', timestamp '{as_of} 06:00:00', 3, date '{as_of}'),
                (?, 1, 1, date '{as_of}' - 30, timestamp '{as_of} 05:00:00', 0, date '{as_of}'),
                ('NoDates', 2, 1, null, timestamp '{as_of} 04:00:00', 0, date '{as_of}')
            """,
            [HOSTILE],
        )
        # 2026-10-22 is a Thursday: the last complete week starts Monday 2026-10-12.
        con.execute(
            """
            insert into jobs_skills_demand values
                (date '2026-10-19', 'in-progress-week', 50, 1),
                (date '2026-10-12', 'python', 10, 1),
                (date '2026-10-12', ?, 4, 2),
                (date '2026-10-05', 'python', 6, 1),
                (date '2026-09-14', 'too-old', 99, 1)
            """,
            [HOSTILE],
        )
        con.execute(
            """
            insert into jobs_company_hiring values
                ('acme', 'Acme', 5, 4, ['Himalayas', 'Jobicy'], date '2026-09-01', date '2026-10-22'),
                (?, ?, 1, 1, [?], null, date '2026-10-01')
            """,
            [HOSTILE.lower(), HOSTILE, HOSTILE],
        )
    finally:
        con.close()


class JobsBiSiteTests(unittest.TestCase):
    def setUp(self):
        try:
            import duckdb  # noqa: F401
        except ImportError:
            self.skipTest("duckdb not installed")

    def _build(self, **kwargs):
        from data_lake.jobs_bi_site import build_site

        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        database = Path(temp.name) / "jobs.duckdb"
        _snapshot(database, **kwargs)
        out = Path(temp.name) / "site"
        result = build_site(database, out)
        return out, result

    def _read(self, out: Path, page: str) -> str:
        return (out / page).read_text(encoding="utf-8")

    def test_every_page_and_asset_is_written(self):
        out, result = self._build()
        for page in PAGES:
            self.assertTrue((out / page).is_file(), page)
            self.assertIn('<html lang="en">', self._read(out, page))
            self.assertIn('href="#main"', self._read(out, page))
        self.assertTrue((out / "assets" / "site.css").is_file())
        self.assertTrue((out / "assets" / "filter.js").is_file())
        self.assertEqual(result["gold_run_id"], "gold-abc")

    def test_scraped_text_is_escaped_everywhere(self):
        out, _ = self._build()
        for page in PAGES:
            text = self._read(out, page)
            self.assertNotIn("<script>alert", text, page)
        for page in ("index.html", "boards/index.html", "skills/index.html", "companies/index.html"):
            self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", self._read(out, page), page)

    def test_pages_load_nothing_from_other_hosts_and_ship_a_strict_csp(self):
        out, _ = self._build()
        for page in PAGES:
            for ref in re.findall(r'(?:src|href)="([^"]*)"', self._read(out, page)):
                self.assertTrue(ref.startswith(("/", "#")), f"{page}: {ref}")
        headers = (out / "_headers").read_text(encoding="utf-8")
        self.assertIn("default-src 'none'", headers)
        self.assertIn("script-src 'self'", headers)
        self.assertNotIn("unsafe-inline", headers.split("script-src")[1].split(";")[0])

    def test_output_is_byte_for_byte_reproducible(self):
        from data_lake.jobs_bi_site import build_site

        out, _ = self._build()
        again = out.parent / "again"
        build_site(out.parent / "jobs.duckdb", again)
        first = {p.relative_to(out): p.read_bytes() for p in out.rglob("*") if p.is_file()}
        second = {p.relative_to(again): p.read_bytes() for p in again.rglob("*") if p.is_file()}
        self.assertEqual(first, second)

    def test_daily_chart_covers_the_90_dates_ending_on_as_of(self):
        out, _ = self._build()
        index = self._read(out, "index.html")
        self.assertIn("2026-07-25", index)  # as_of - 89: first date in the window
        self.assertNotIn("Old", index)  # as_of - 100
        self.assertNotIn("Future", index)
        self.assertIn('data-kpi="postings_90d">6<', index)  # 2 + 3 + 1 in the window

    def test_skills_use_the_last_complete_week(self):
        out, _ = self._build()
        skills = self._read(out, "skills/index.html")
        self.assertIn("2026-10-12", skills)
        self.assertNotIn("in-progress-week", skills)
        self.assertNotIn("too-old", skills)
        self.assertIn("python", skills)

    def test_boards_flag_quiet_and_undated_boards(self):
        out, _ = self._build()
        boards = self._read(out, "boards/index.html")
        self.assertIn("Quiet 14+ days", boards)
        self.assertIn("No posted date", boards)

    def test_empty_marts_still_build(self):
        out, result = self._build(empty=True)
        self.assertIn("No postings", self._read(out, "index.html"))
        self.assertEqual(result["pages"], len(PAGES))


if __name__ == "__main__":
    unittest.main()
