#!/usr/bin/env python3
"""Tests for the jobs Silver → dbt marts → Gold runner."""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "infra" / "scripts"))

from data_lake.ingest import ingest_payload  # noqa: E402
from data_lake.query import query_parquet  # noqa: E402
from data_lake.storage import ObjectStore  # noqa: E402

NOW = datetime(2026, 10, 22, 12, 0, tzinfo=timezone.utc)


def _job(index, **overrides):
    record = {
        "id": f"job-{index}",
        "title": f"Engineer {index}",
        "company": "Acme",
        "url": f"https://example.com/jobs/{index}",
        "event_time": "2026-10-22T06:00:00Z",
        "posted": "2026-10-20",
        "provider": "book-job-scraping",
        "source": "Himalayas",
        "tags": "",
        "salary": "",
        "keyword": "python",
    }
    record.update(overrides)
    return record


FIXTURE_JOBS = [
    _job(1, tags="Python, SQL", salary="$5k"),
    _job(2, tags="python"),
    _job(3, source="Jobicy", company="Beta", posted="2026-10-21", tags="Go", salary="$1"),
    _job(4, source="Jobicy", company=" beta ", posted="2026-09-01"),
    _job(5, url=""),  # quarantined
    _job(6, provider="offline_fixture"),  # excluded
]


def _ingest(root, records, raw=b"jobs-marts-fixture"):
    ingest_payload(
        raw,
        records,
        source="book-job-data",
        domain="jobs",
        dataset="job_postings",
        data_lake_uri=root,
        input_format="text",
        event_time_field="event_time",
        privacy_class="internal",
    )


def _dbt_available() -> bool:
    from data_lake.jobs_marts import resolve_dbt_bin

    return resolve_dbt_bin() is not None


class JobsMartsTests(unittest.TestCase):
    def setUp(self):
        try:
            import duckdb  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("duckdb/pyarrow not installed")
        if not _dbt_available():
            if os.environ.get("SOLO_EMPIRE_CI") == "1":
                self.fail("dbt not found; SOLO_EMPIRE_CI=1 requires the data-lake runtime")
            self.skipTest("dbt not installed")

    def _run(self, root, **kwargs):
        from data_lake.jobs_marts import run_jobs_marts

        kwargs.setdefault("now", NOW)
        # The fixture quarantines 1 of 5 rows on purpose; the real default
        # (0.02) is covered by test_quality_gate_failure_writes_no_gold.
        kwargs.setdefault("max_invalid_ratio", 0.25)
        return run_jobs_marts(data_lake_uri=root, **kwargs)

    def _rows(self, root, result, mart, sql):
        return query_parquet(str(Path(root) / result["marts"][mart]["key"]), sql)

    def test_marts_are_built_from_the_current_silver_snapshot(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            result = self._run(root)
            self.assertEqual(result["status"], "success")
            self.assertEqual(result["silver"]["record_count"], 4)
            self.assertEqual(result["silver"]["invalid_count"], 1)
            self.assertEqual(result["silver"]["excluded_count"], 1)

            daily = self._rows(
                root, result, "jobs_postings_daily",
                "SELECT CAST(posted_date AS VARCHAR) d, board, postings, companies, with_salary "
                "FROM lake_table ORDER BY d, board",
            )
            self.assertEqual(
                [tuple(row.values()) for row in daily],
                [
                    ("2026-09-01", "Jobicy", 1, 1, 0),
                    ("2026-10-20", "Himalayas", 2, 1, 1),
                    ("2026-10-21", "Jobicy", 1, 1, 1),
                ],
            )

            health = self._rows(
                root, result, "jobs_board_health",
                "SELECT board, postings_total, CAST(last_posted_date AS VARCHAR) last_posted, postings_7d "
                "FROM lake_table ORDER BY board",
            )
            self.assertEqual(
                [tuple(row.values()) for row in health],
                [("Himalayas", 2, "2026-10-20", 2), ("Jobicy", 2, "2026-10-21", 1)],
            )

            skills = self._rows(
                root, result, "jobs_skills_demand",
                "SELECT CAST(posted_week AS VARCHAR) w, tag, postings, demand_rank "
                "FROM lake_table ORDER BY w, demand_rank, tag",
            )
            self.assertEqual(
                [tuple(row.values()) for row in skills],
                [("2026-10-19", "python", 2, 1), ("2026-10-19", "go", 1, 2), ("2026-10-19", "sql", 1, 2)],
            )

            companies = self._rows(
                root, result, "jobs_company_hiring",
                "SELECT company_key, postings_total, postings_30d, boards FROM lake_table ORDER BY company_key",
            )
            self.assertEqual(
                [tuple(row.values()) for row in companies],
                [("acme", 2, 2, ["Himalayas"]), ("beta", 2, 1, ["Jobicy"])],
            )

    def test_rerun_is_idempotent_and_manifest_links_silver(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            first = self._run(root)
            second = self._run(root)
            self.assertEqual(first["gold_run_id"], second["gold_run_id"])
            self.assertTrue(second["manifest_existed"])
            self.assertTrue(all(mart["existed"] for mart in second["marts"].values()))
            self.assertTrue(
                all(
                    "/schema_version=1/" in mart["key"] and mart["key"].startswith("gold/domain=jobs/")
                    for mart in first["marts"].values()
                )
            )
            manifest = json.loads(ObjectStore(root).get_bytes(first["manifest_key"]))
            self.assertEqual(manifest["layer"], "gold")
            self.assertEqual(manifest["input"]["silver_run_id"], first["silver_run_id"])
            self.assertEqual(
                {mart["dataset"]: mart["key"] for mart in manifest["marts"]},
                {name: mart["key"] for name, mart in first["marts"].items()},
            )

    def test_new_bronze_batch_gets_new_gold_run(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            first = self._run(root)
            _ingest(root, [_job(7, source="Jobicy", company="Gamma")], raw=b"second-batch")
            second = self._run(root)
            self.assertNotEqual(first["gold_run_id"], second["gold_run_id"])
            health = self._rows(
                root, second, "jobs_board_health",
                "SELECT postings_total FROM lake_table WHERE board = 'Jobicy'",
            )
            self.assertEqual(health[0]["postings_total"], 3)

    def test_stale_silver_is_reported_but_gold_still_written(self):
        with tempfile.TemporaryDirectory() as root:
            _ingest(root, FIXTURE_JOBS)
            result = self._run(root, now=datetime(2026, 10, 25, 12, 0, tzinfo=timezone.utc), stale_hours=24)
            self.assertEqual(result["status"], "stale")
            self.assertTrue(result["freshness"]["stale"])
            self.assertGreater(result["freshness"]["age_hours"], 24)
            self.assertTrue(ObjectStore(root).list_keys("gold/domain=jobs/"))

    def test_quality_gate_failure_writes_no_gold(self):
        from data_lake.silver import SilverQualityError

        with tempfile.TemporaryDirectory() as root:
            _ingest(root, [_job(1), _job(2, url=""), _job(3, title="")])
            with self.assertRaises(SilverQualityError):
                self._run(root, max_invalid_ratio=0.02)
            self.assertEqual(ObjectStore(root).list_keys("gold/"), [])


if __name__ == "__main__":
    unittest.main()
